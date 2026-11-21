"""Bronze -> silver transforms. Pure DataFrame functions so unit tests can run them on a
local SparkSession without Kafka or MinIO.

Flow per micro-batch and table:
  decode_table()      strip the Confluent header, from_avro per schema id, flatten the
                      Debezium envelope; undecodable / null-PK rows come back as quarantine
  to_silver_rows()    types, metadata columns, dedup inside the batch
  merge_into_silver() soft-delete MERGE guarded by the LSN
"""
from __future__ import annotations

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.utils import AnalysisException

from .schema_registry import SchemaNotFound
from .tables import TABLES

log = logging.getLogger("silver")
PARALLEL_TABLES = int(os.environ.get("SILVER_PARALLEL_TABLES", "5"))

RAW_COLS = ["topic", "partition", "offset", "key", "value"]
# Confluent wire format: 1 magic byte (0) + 4-byte big-endian schema id + Avro payload.
# substring() is 1-based, so the payload starts at byte 6.
PAYLOAD = "substring(value, 6, length(value) - 5)"


def schema_id_col():
    """Schema id from bytes 2-5 of the value; null when the value isn't in wire format."""
    wire_format = (F.length("value") >= 5) & F.expr("substring(value, 1, 1) = X'00'")
    return F.when(wire_format, F.conv(F.hex(F.expr("substring(value, 2, 4)")), 16, 10).cast("int"))


def _union(parts: list[DataFrame]) -> DataFrame | None:
    if not parts:
        return None
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)


def _quarantine_rows(df: DataFrame, reason) -> DataFrame:
    reason = F.lit(reason) if isinstance(reason, str) else reason
    return df.select(*RAW_COLS, reason.alias("reason"))


def decode_table(rows: DataFrame, table: str, schema_for, ids=None) -> tuple[DataFrame | None, DataFrame | None]:
    """Decode one table's bronze rows.

    Returns (good, bad). `good` has _op, _lsn, _source_ms, _offset plus the source columns
    (after-image, or before-image for deletes). `bad` has the raw columns plus `reason`.
    Rows with different writer schemas are decoded separately and unioned by column name,
    which is what lets silver survive schema drift. `ids` (the distinct schema ids in
    `rows`) can be passed in when the caller already knows them, to save a Spark job.
    """
    pk = TABLES[table]["pk"]
    good_parts, bad_parts = [], []

    bad_parts.append(_quarantine_rows(rows.filter(F.col("schema_id").isNull()), "bad_wire_format"))

    if ids is None:
        ids = [r[0] for r in rows.select("schema_id").distinct().collect()]
    for sid in [i for i in ids if i is not None]:
        part = rows.filter(F.col("schema_id") == sid)
        try:
            schema = schema_for(sid)
        except SchemaNotFound:
            bad_parts.append(_quarantine_rows(part, "schema_not_found"))
            continue

        dec = part.select(*RAW_COLS, from_avro(F.expr(PAYLOAD), schema, {"mode": "PERMISSIVE"}).alias("rec"))
        bad_parts.append(_quarantine_rows(dec.filter(F.col("rec.op").isNull()), "avro_decode_failed"))

        # deletes carry the row in `before` (REPLICA IDENTITY FULL); everything else in `after`
        row = F.when(F.col("rec.op") == "d", F.col("rec.before")).otherwise(F.col("rec.after"))
        flat = dec.filter(F.col("rec.op").isNotNull()).select(
            *RAW_COLS,
            F.col("rec.op").alias("_op"),
            F.coalesce(F.col("rec.source.lsn"), F.lit(0).cast("long")).alias("_lsn"),
            F.col("rec.source.ts_ms").alias("_source_ms"),
            F.col("offset").alias("_offset"),
            row.alias("_row"),
        )
        null_pk = reduce(lambda a, b: a | b, [F.col(f"_row.{c}").isNull() for c in pk])
        unusable = F.col("_row").isNull() | null_pk
        reason = F.when(F.col("_row").isNull(), "null_row").otherwise("null_pk")
        bad_parts.append(_quarantine_rows(flat.filter(unusable), reason))
        good_parts.append(flat.filter(~unusable).select("_op", "_lsn", "_source_ms", "_offset", "_row.*"))

    return _union(good_parts), _union(bad_parts)


def dedup_latest(df: DataFrame, pk: list[str]) -> DataFrame:
    """Keep one row per key: the highest LSN, then the highest Kafka offset."""
    w = Window.partitionBy(*pk).orderBy(F.col("_lsn").desc(), F.col("_offset").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def to_silver_rows(flat: DataFrame, table: str) -> DataFrame:
    cfg = TABLES[table]
    df = flat
    for c in cfg["ts_cols"]:
        if c in df.columns:  # Debezium ZonedTimestamp string -> timestamp
            df = df.withColumn(c, F.col(c).cast("timestamp"))
    df = (
        df.withColumn("_source_ts", F.expr("timestamp_millis(_source_ms)"))
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_is_deleted", F.col("_op") == "d")
    )
    return dedup_latest(df, cfg["pk"]).drop("_source_ms", "_offset")


def table_exists(spark, path: str) -> bool:
    try:
        spark.read.format("delta").load(path)
        return True
    except AnalysisException:
        return False


_KNOWN_TABLES: set[str] = set()  # silver paths we already know exist: skips a snapshot load per batch


def merge_into_silver(df: DataFrame, path: str, pk: list[str]) -> None:
    """Soft-delete MERGE guarded by the LSN.

    A delete only flips _is_deleted and keeps the row with its LSN, so replaying an older
    insert (s._lsn <= t._lsn) can't resurrect it. Replays of any older event are no-ops.
    """
    spark = df.sparkSession
    if path not in _KNOWN_TABLES:
        if not table_exists(spark, path):
            df.write.format("delta").save(path)
            _KNOWN_TABLES.add(path)
            return
        _KNOWN_TABLES.add(path)
    view = "_su_" + re.sub(r"\W", "_", path)  # unique per table: tables are merged in parallel threads
    df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.{c} = s.{c}" for c in pk)
    spark.sql(f"""
        MERGE INTO delta.`{path}` t
        USING {view} s
        ON {on}
        WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd' THEN UPDATE SET
            t._is_deleted = true, t._lsn = s._lsn, t._op = s._op,
            t._source_ts = s._source_ts, t._silver_ts = s._silver_ts
        WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def write_quarantine(bad: DataFrame, path: str, batch_id: int) -> None:
    (
        bad.withColumn("batch_id", F.lit(batch_id).cast("long"))
        .withColumn("quarantined_at", F.current_timestamp())
        .write.format("delta").mode("append").save(path)
    )


def _process_table(batch_df, table, ids, batch_id, schema_for, silver_root, quarantine_root) -> str:
    t0 = time.time()
    rows = batch_df.filter(F.col("_table") == table)
    good, bad = decode_table(rows, table, schema_for, ids)
    merged = quarantined = 0
    t_merge = 0.0
    if good is not None:
        silver = to_silver_rows(good, table).persist()
        merged = silver.count()
        if merged:
            t1 = time.time()
            merge_into_silver(silver, f"{silver_root}/{table}", TABLES[table]["pk"])
            t_merge = time.time() - t1
        silver.unpersist()
    if bad is not None:
        bad = bad.persist()
        quarantined = bad.count()
        if quarantined:
            write_quarantine(bad, f"{quarantine_root}/{table}", batch_id)
        bad.unpersist()
    return (f"{table}: merged={merged} quarantined={quarantined} "
            f"merge={t_merge:.1f}s total={time.time() - t0:.1f}s")


def process_batch(batch_df: DataFrame, batch_id: int, schema_for, silver_root: str, quarantine_root: str) -> None:
    """foreachBatch body for silver. batch_df holds bronze rows plus a `_table` column.

    Tables are independent Delta tables, so they are decoded and merged in parallel threads
    (Spark schedules the jobs together; most of the time per table is S3/Delta-log latency,
    not CPU).
    """
    t0 = time.time()
    batch_df.persist()
    try:
        # one job tells us which tables and schema ids this batch holds
        ids_by_table: dict = {}
        for r in batch_df.groupBy("_table", "schema_id").count().collect():
            ids_by_table.setdefault(r["_table"], []).append(r["schema_id"])
        tables = [t for t in TABLES if t in ids_by_table]
        with ThreadPoolExecutor(max_workers=PARALLEL_TABLES) as pool:
            futures = [pool.submit(_process_table, batch_df, t, ids_by_table[t], batch_id, schema_for,
                                   silver_root, quarantine_root) for t in tables]
            summaries = [f.result() for f in futures]  # re-raises: a failed table fails the batch, which retries
        log.warning("silver batch=%s took %.1fs | %s", batch_id, time.time() - t0, " | ".join(summaries))
    finally:
        batch_df.unpersist()
