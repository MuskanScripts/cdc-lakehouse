"""Deletion-vector probe on SCRATCH copies of silver/orders (silver itself is not touched).

  spark-submit ... dv_probe.py            (make dv-probe)

1. copies silver/orders twice: s3a://lakehouse/_probe/orders_plain and .../orders_dv
2. enables deletion vectors on the _dv copy (this upgrades THAT copy's protocol)
3. builds one realistic batch: 400 updates of random existing orders (higher _lsn) + 250 inserts
4. runs the silver MERGE (same SQL shape as merge_into_silver) on both copies, 3 rounds each
5. prints Delta's own metrics per MERGE: exec time, rows copied, files added, DVs added
The delta-rs read check runs separately (chaos/dv_read_check.py), because reconcile and dbt read
with delta-rs, not Spark.
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, "/opt/project")
from pyspark.sql import functions as F  # noqa: E402

from streaming.common.spark_session import get_spark  # noqa: E402

SRC = "s3a://lakehouse/silver/orders"
ROOT = "s3a://lakehouse/_probe"
MERGE = """
    MERGE INTO delta.`{path}` t
    USING _probe_src s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s._lsn > t._lsn AND s._op = 'd' THEN UPDATE SET
        t._is_deleted = true, t._lsn = s._lsn, t._op = s._op, t._source_ts = s._source_ts, t._silver_ts = s._silver_ts
    WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
"""
KEYS = ["executionTimeMs", "scanTimeMs", "rewriteTimeMs", "numTargetRowsCopied", "numTargetRowsUpdated",
        "numTargetRowsInserted", "numTargetFilesAdded", "numTargetFilesRemoved", "numTargetDeletionVectorsAdded"]


def batch(spark, base, round_no):
    mx = base.agg(F.max("order_id"), F.max("_lsn")).first()
    upd = (base.filter("not _is_deleted").orderBy(F.rand(42 + round_no)).limit(400)
           .withColumn("status", F.lit("shipped")).withColumn("_op", F.lit("u")))
    ins = (base.orderBy(F.rand(7 + round_no)).limit(250)
           .withColumn("order_id", F.col("order_id") + F.lit(mx[0] + 1_000_000 * round_no))
           .withColumn("_op", F.lit("c")))
    return (upd.unionByName(ins)
            .withColumn("_lsn", F.lit(mx[1] + 1000 * round_no) + F.monotonically_increasing_id())
            .withColumn("_silver_ts", F.current_timestamp()))


def main():
    spark = get_spark("dv-probe")
    spark.sparkContext.setLogLevel("WARN")
    src = spark.read.format("delta").load(SRC)
    n = src.count()
    for name in ("plain", "dv"):
        src.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{ROOT}/orders_{name}")
    spark.sql(f"ALTER TABLE delta.`{ROOT}/orders_dv` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
    print(f"copied silver/orders ({n} rows) to {ROOT}/orders_plain and orders_dv; DVs enabled on orders_dv")

    print(f"{'table':<7}{'round':>6}{'wall s':>8}{'exec s':>8}{'scan s':>8}{'rewrite s':>10}{'copied':>9}"
          f"{'upd':>6}{'ins':>6}{'files +':>8}{'files -':>8}{'DVs +':>7}")
    for r in (1, 2, 3):
        for name in ("plain", "dv"):
            path = f"{ROOT}/orders_{name}"
            base = spark.read.format("delta").load(path)
            batch(spark, base, r).cache().createOrReplaceTempView("_probe_src")
            t0 = time.time()
            spark.sql(MERGE.format(path=path))
            wall = time.time() - t0
            h = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").first()
            m = {k: int(h["operationMetrics"].get(k, -1)) for k in KEYS}
            print(f"{name:<7}{r:>6}{wall:>8.1f}{m['executionTimeMs'] / 1000:>8.1f}{m['scanTimeMs'] / 1000:>8.1f}"
                  f"{m['rewriteTimeMs'] / 1000:>10.1f}{m['numTargetRowsCopied']:>9}{m['numTargetRowsUpdated']:>6}"
                  f"{m['numTargetRowsInserted']:>6}{m['numTargetFilesAdded']:>8}{m['numTargetFilesRemoved']:>8}"
                  f"{m['numTargetDeletionVectorsAdded']:>7}", flush=True)
    for name in ("plain", "dv"):
        d = spark.sql(f"DESCRIBE DETAIL delta.`{ROOT}/orders_{name}`").first()
        rows = spark.read.format("delta").load(f"{ROOT}/orders_{name}").count()
        print(f"{name}: protocol reader={d['minReaderVersion']} writer={d['minWriterVersion']} "
              f"features={d['tableFeatures']} files={d['numFiles']} rows_per_spark={rows}")
    spark.stop()


if __name__ == "__main__":
    main()
