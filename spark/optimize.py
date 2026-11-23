"""OPTIMIZE (bin-packing compaction) + VACUUM RETAIN 168 HOURS on the Delta tables of a layer.

  spark-submit ... optimize.py [silver] [--no-vacuum] [--zorder]

--zorder clusters each table by its first primary-key column (OPTIMIZE ... ZORDER BY), which the
nightly Airflow `optimize` DAG uses. With one key column this is a sort by key: what the MERGE
joins on, and what Week 6 needs for the key-range idea in ADR-023/028.

Run it ONLY while the streaming driver is stopped (`make optimize` does that). Delta on S3/MinIO
here uses the single-driver log store: it serialises commits inside one JVM only, so an OPTIMIZE
from a second JVM racing the silver MERGE could make two writers both "win" the same log
version. See ADR-023.
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, "/opt/project")
from streaming.common.spark_session import get_spark  # noqa: E402
from streaming.common.tables import BUCKET, TABLES  # noqa: E402


def detail(spark, path):
    r = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    return r["numFiles"], r["sizeInBytes"]


def main() -> None:
    layer = next((a for a in sys.argv[1:] if not a.startswith("--")), "silver")
    vacuum = "--no-vacuum" not in sys.argv
    zorder = "--zorder" in sys.argv
    spark = get_spark("optimize")
    spark.sparkContext.setLogLevel("WARN")
    print(f"{'table':<12}{'files before':>13}{'after':>7}{'avg KiB before':>16}{'after':>8}{'optimize s':>12}{'vacuum s':>10}")
    for t in TABLES:
        path = f"{BUCKET}/{layer}/{t}"
        n0, b0 = detail(spark, path)
        t0 = time.time()
        z = f" ZORDER BY ({TABLES[t]['pk'][0]})" if zorder else ""
        spark.sql(f"OPTIMIZE delta.`{path}`{z}").collect()
        t1 = time.time()
        if vacuum:
            spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS").collect()
        t2 = time.time()
        n1, b1 = detail(spark, path)
        print(f"{t:<12}{n0:>13}{n1:>7}{b0 / max(n0, 1) / 1024:>16.1f}{b1 / max(n1, 1) / 1024:>8.1f}"
              f"{t1 - t0:>12.1f}{t2 - t1:>10.1f}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
