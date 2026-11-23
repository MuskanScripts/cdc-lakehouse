"""Backfill / repair one silver table from bronze, as a batch job (Airflow DAG `backfill_silver`).

  spark-submit ... streaming/backfill_silver.py --table orders --start 2026-11-20 --end 2026-11-23

Reads the bronze rows whose Kafka timestamp falls in [start, end] (dates, UTC, end inclusive) with
`trigger(availableNow=True)`: a streaming query that processes everything already in bronze and
then stops. Every micro-batch goes through the SAME code as the live stream (`process_batch`:
decode per schema id, dedup by LSN, soft-delete MERGE with the LSN guard), so a backfill cannot
disagree with the stream about how a row is built.

Why it is safe to run at any time (ADR-032): the MERGE only changes a silver row when the bronze
event has a HIGHER LSN than what silver holds, or the key is missing. Replaying history over a
healthy table changes nothing; over a damaged one it restores exactly the missing/stale keys.
What it needs: the streaming driver paused (the S3 log store serialises commits inside one JVM
only, ADR-023). The Airflow DAG does that.

Not done here: quarantine (the stream already quarantined those bronze rows once) and the
event-time side output (late events are an append log, not state to rebuild).
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
import time
import uuid

sys.path.insert(0, "/opt/project")
from pyspark.sql import functions as F  # noqa: E402

from streaming.common.schema_registry import SchemaRegistry  # noqa: E402
from streaming.common.spark_session import SCHEMA_REGISTRY_URL, get_spark  # noqa: E402
from streaming.common.tables import BRONZE, CHECKPOINTS, SILVER, TABLES  # noqa: E402
from streaming.common.transforms import process_batch  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--table", required=True, choices=sorted(TABLES))
    p.add_argument("--start", required=True, help="YYYY-MM-DD, UTC, inclusive")
    p.add_argument("--end", required=True, help="YYYY-MM-DD, UTC, inclusive")
    p.add_argument("--max-files", type=int, default=2000, help="bronze files per micro-batch")
    a = p.parse_args()
    start = dt.date.fromisoformat(a.start)
    end_excl = dt.date.fromisoformat(a.end) + dt.timedelta(days=1)
    if end_excl <= start:
        sys.exit(f"--end {a.end} is before --start {a.start}")

    logging.basicConfig(level=logging.WARNING)
    spark = get_spark("backfill-silver")
    spark.sparkContext.setLogLevel("WARN")
    registry = SchemaRegistry(SCHEMA_REGISTRY_URL)
    run = f"{a.table}-{dt.datetime.utcnow():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    src = (spark.readStream.format("delta").option("maxFilesPerTrigger", a.max_files)
           .load(f"{BRONZE}/{a.table}")
           .filter((F.col("kafka_timestamp") >= F.lit(str(start)).cast("timestamp"))
                   & (F.col("kafka_timestamp") < F.lit(str(end_excl)).cast("timestamp")))
           .withColumn("_table", F.lit(a.table)))

    before = spark.read.format("delta").load(f"{SILVER}/{a.table}").count()
    t0 = time.time()
    q = (src.writeStream
         .foreachBatch(lambda df, bid: process_batch(df, bid, registry.writer_schema, SILVER, None))
         .option("checkpointLocation", f"{CHECKPOINTS}/backfill/{run}")   # fresh: always re-reads the range
         .trigger(availableNow=True)
         .queryName(f"backfill_{a.table}")
         .start())
    q.awaitTermination()
    progress = q.recentProgress
    rows = sum(pr["numInputRows"] for pr in progress)
    after = spark.read.format("delta").load(f"{SILVER}/{a.table}").count()
    print(f"BACKFILL {a.table} {a.start}..{a.end}: {rows} bronze rows in {len(progress)} batches, "
          f"silver rows {before} -> {after} (+{after - before}), {time.time() - t0:.1f}s, run {run}",
          flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
