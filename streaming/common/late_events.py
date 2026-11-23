"""Event time: orders created per minute, a 10 minute watermark, and a table for late events.

Flow (see ADR-020):
  silver foreachBatch (orders)   -> process_order_events(): takes the `c` events of the batch,
                                    drops Debezium re-deliveries, splits them against the
                                    watermark, appends on-time ones to events/orders_created and
                                    late ones to late_events/orders
  start_orders_per_minute()      -> a real Spark stateful query: withWatermark(event_time, 10 min)
                                    + window(1 min) count, upserted into agg/orders_per_minute

Event time = `orders.created_at` (the business time, set by Postgres now()). Processing time =
bronze `ingest_ts`. A late event is one whose event time is older than
    watermark = (max event time of earlier batches) - WATERMARK_SECONDS.
Spark's own watermark silently DROPS such rows from the aggregate; it has no side output. So the
split is done here, before Spark sees the rows, with the same definition, and Spark's
withWatermark then bounds its state (and would drop the same rows) exactly as documented.
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType, TimestampType
from pyspark.sql.utils import AnalysisException

from .tables import BUCKET, CHECKPOINTS, idempotent

log = logging.getLogger("events")

EVENTS = f"{BUCKET}/events/orders_created"
LATE = f"{BUCKET}/late_events/orders"
AGG = f"{BUCKET}/agg/orders_per_minute"
WATERMARK_SECONDS = int(os.environ.get("WATERMARK_SECONDS", "600"))
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "10 seconds")

EVENTS_SCHEMA = StructType([
    StructField("order_id", LongType()),
    StructField("event_time", TimestampType()),   # orders.created_at
    StructField("ingest_ts", TimestampType()),    # when bronze wrote it
    StructField("batch_id", LongType()),          # silver batch that emitted it
])
LATE_SCHEMA = StructType(EVENTS_SCHEMA.fields + [
    StructField("watermark", TimestampType()),
    StructField("detected_at", TimestampType()),
])


def ensure_event_tables(spark) -> None:
    """Create empty tables so the aggregate query can start streaming before any order exists."""
    for path, schema in ((EVENTS, EVENTS_SCHEMA), (LATE, LATE_SCHEMA)):
        spark.createDataFrame([], schema).write.format("delta").mode("ignore").save(path)


def _ts(micros: int):
    return F.expr(f"timestamp_micros({int(micros)})")


def created_events(good: DataFrame) -> DataFrame:
    """One row per newly created order in the batch (`c` only: snapshot reads are not events)."""
    return (
        good.filter(F.col("_op") == "c")
        .select(
            F.col("order_id"),
            F.col("created_at").cast("timestamp").alias("event_time"),
            F.col("_ingest_ts").alias("ingest_ts"),
        )
        .dropDuplicates(["order_id"])
    )


class WatermarkTracker:
    """max(event_time) over the batches BEFORE the current one, in microseconds.

    Kept in memory; on a cold start (process restart) it is rebuilt from the events table with
    `batch_id < current`, so replaying batch N after a crash sees the same watermark as the
    first attempt did (batch N's own events don't move it).
    """

    def __init__(self, events_path: str = EVENTS, delay_seconds: int = WATERMARK_SECONDS):
        self.events_path = events_path
        self.delay_us = delay_seconds * 1_000_000
        self.max_us: int | None = None
        self.loaded = False

    def watermark_us(self, spark, batch_id: int) -> int | None:
        if not self.loaded:
            row = (spark.read.format("delta").load(self.events_path)
                   .filter(F.col("batch_id") < batch_id)
                   .agg(F.expr("max(unix_micros(event_time))")).first())
            self.max_us = row[0]
            self.loaded = True
        return None if self.max_us is None else self.max_us - self.delay_us

    def advance(self, max_us: int | None) -> None:
        if max_us is not None and (self.max_us is None or max_us > self.max_us):
            self.max_us = max_us


def split_late(events: DataFrame, wm_us: int | None) -> tuple[DataFrame, DataFrame | None]:
    """(on_time, late). Nothing is late until a watermark exists."""
    if wm_us is None:
        return events, None
    us = F.expr("unix_micros(event_time)")
    return events.filter(us >= wm_us), events.filter(us < wm_us)


def process_order_events(good: DataFrame, batch_id: int, tracker: WatermarkTracker,
                         events_path: str = EVENTS, late_path: str = LATE) -> tuple[int, int]:
    """Side output of the silver orders batch. Returns (on_time_written, late_written)."""
    spark = good.sparkSession
    ev = created_events(good).persist()
    try:
        n, max_us = ev.agg(F.count("*"), F.expr("max(unix_micros(event_time))")).first()
        if not n:
            return 0, 0
        wm_us = tracker.watermark_us(spark, batch_id)
        on_time, late = split_late(ev, wm_us)

        # Debezium is at-least-once: after a Connect crash the same `c` event can arrive again,
        # in a later batch. It has the identical event_time, so look only at event files that can
        # contain it (Delta data skipping on event_time) and drop what is already there. The same
        # goes for a re-delivered event that is itself late: it must not be reported twice.
        min_us = ev.agg(F.expr("min(unix_micros(event_time))")).first()[0]
        seen = (spark.read.format("delta").load(events_path)
                .filter(F.col("event_time") >= _ts(min_us)).select("order_id"))
        fresh = (on_time.join(seen, "order_id", "left_anti")
                 .withColumn("batch_id", F.lit(batch_id).cast("long"))
                 .select("order_id", "event_time", "ingest_ts", "batch_id").persist())
        written = fresh.count()
        if written:
            idempotent(fresh.write.format("delta").mode("append"), "orders-events", batch_id).save(events_path)
        fresh.unpersist()

        late_n = 0
        if late is not None:
            already_late = spark.read.format("delta").load(late_path).filter(F.col("batch_id") < batch_id).select("order_id")
            late = late.join(seen, "order_id", "left_anti").join(already_late, "order_id", "left_anti")
            late_rows = (late.withColumn("batch_id", F.lit(batch_id).cast("long"))
                         .withColumn("watermark", _ts(wm_us))
                         .withColumn("detected_at", F.current_timestamp())
                         .select(*[f.name for f in LATE_SCHEMA.fields]).persist())
            late_n = late_rows.count()
            if late_n:
                idempotent(late_rows.write.format("delta").mode("append"), "orders-late", batch_id).save(late_path)
                log.warning("batch %s: %s late order event(s), watermark %s us", batch_id, late_n, wm_us)
            late_rows.unpersist()

        if max_us is not None and (wm_us is None or max_us >= wm_us):
            tracker.advance(max_us)
        return written, late_n
    finally:
        ev.unpersist()


# ---------------------------------------------------------------- the stateful aggregate

def _upsert_counts(batch: DataFrame, batch_id: int, path: str = AGG) -> None:
    """update-mode output: the full count of every window that changed. Replacing by key makes a
    replayed batch harmless."""
    spark = batch.sparkSession
    rows = batch.select(F.col("window.start").alias("window_start"),
                        F.col("window.end").alias("window_end"), F.col("orders").cast("long"))
    try:
        spark.read.format("delta").load(path)
    except AnalysisException:
        rows.write.format("delta").mode("append").save(path)
        return
    rows.createOrReplaceTempView("_agg_updates")
    spark.sql(f"""
        MERGE INTO delta.`{path}` t USING _agg_updates s ON t.window_start = s.window_start
        WHEN MATCHED THEN UPDATE SET t.orders = s.orders
        WHEN NOT MATCHED THEN INSERT *
    """)


def start_orders_per_minute(spark):
    ev = (spark.readStream.format("delta").option("maxFilesPerTrigger", 20).load(EVENTS))
    agg = (
        ev.withWatermark("event_time", f"{WATERMARK_SECONDS} seconds")
        .groupBy(F.window("event_time", "1 minute"))
        .agg(F.count("*").alias("orders"))
    )
    return (
        agg.writeStream.outputMode("update")
        .foreachBatch(_upsert_counts)
        .option("checkpointLocation", f"{CHECKPOINTS}/orders_per_minute")
        .trigger(processingTime=TRIGGER)
        .queryName("orders_per_minute")
        .start()
    )
