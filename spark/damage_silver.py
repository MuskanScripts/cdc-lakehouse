"""DRILL ONLY (`make silver-damage`): physically delete every Nth key from one silver table, to prove
that the `backfill_silver` DAG repairs it from bronze (ADR-032).

  spark-submit ... damage_silver.py <table> <every_n>

Bronze is not touched, so nothing is lost: bronze has every change ever captured, and Delta time
travel still has the old silver version. Run it only with streaming paused (`make silver-damage`
does that), like any second writer here (ADR-023).
"""
from __future__ import annotations

import sys

sys.path.insert(0, "/opt/project")
from streaming.common.spark_session import get_spark  # noqa: E402
from streaming.common.tables import SILVER, TABLES  # noqa: E402

table, every = sys.argv[1], int(sys.argv[2])
key = TABLES[table]["pk"][0]
path = f"{SILVER}/{table}"
spark = get_spark("damage-silver")
spark.sparkContext.setLogLevel("WARN")
before = spark.read.format("delta").load(path).count()
spark.sql(f"DELETE FROM delta.`{path}` WHERE pmod({key}, {every}) = 0")
after = spark.read.format("delta").load(path).count()
print(f"DAMAGE {table}: deleted {before - after} rows where {key} % {every} = 0 ({before} -> {after})", flush=True)
spark.stop()
