"""The fixed benchmark queries for the OPTIMIZE / ZORDER before/after (bench/results.md).

  python bench_query.py [--runs 5]

Both run through delta-rs + pyarrow (the same reader that reconcile and the dbt delta plugin use),
each RUNS times; the median wall time includes opening the table (reading the Delta log).

Q1 bronze/orders full scan: count rows and distinct (partition, offset). This is what a backfill
   or any reprocessing of bronze reads, and bronze is where small files pile up (one append per
   10 s trigger per table).
Q2 silver/orders key range: orders with order_id in the 1,000 highest ids (fixed at the first
   run). Also reports how many files' min/max statistics overlap that range, i.e. how many files a
   reader that skips by statistics would still have to open. That is what ZORDER is for.
"""
import argparse
import os
import statistics as st
import sys
import time

import pyarrow.compute as pc
from deltalake import DeltaTable

from common import STORAGE

RANGE_FILE = "/app/.bench_q2_range"


def q1():
    dt = DeltaTable("s3://lakehouse/bronze/orders", storage_options=STORAGE)
    t = dt.to_pyarrow_table(columns=["partition", "offset"])
    keys = pc.add(pc.multiply(pc.cast(t["partition"], "int64"), 10**12), t["offset"])
    return t.num_rows, len(pc.unique(keys)), len(dt.files())


def q2(lo, hi):
    dt = DeltaTable("s3://lakehouse/silver/orders", storage_options=STORAGE)
    t = dt.to_pyarrow_table(filters=[("order_id", ">=", lo), ("order_id", "<", hi)],
                            columns=["order_id", "status", "total", "_is_deleted"])
    a = dt.get_add_actions(flatten=True).to_pydict()
    overlap = sum(1 for mn, mx in zip(a["min.order_id"], a["max.order_id"]) if mn < hi and mx >= lo)
    return t.num_rows, overlap, len(a["path"])


def timed(fn, runs):
    times, res = [], None
    for _ in range(runs):
        t0 = time.time()
        res = fn()
        times.append(time.time() - t0)
    return st.median(times), min(times), res


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--label", default="run")
    a = ap.parse_args()
    if os.path.exists(RANGE_FILE):
        lo, hi = map(int, open(RANGE_FILE).read().split())
    else:
        dt = DeltaTable("s3://lakehouse/silver/orders", storage_options=STORAGE)
        mx = pc.max(dt.to_pyarrow_table(columns=["order_id"])["order_id"]).as_py()
        lo, hi = mx - 999, mx + 1
        open(RANGE_FILE, "w").write(f"{lo} {hi}")
    med, best, (rows, distinct, files) = timed(q1, a.runs)
    print(f"Q1 {a.label}: bronze/orders full scan  rows={rows} distinct_offsets={distinct} files={files}"
          f"  median={med:.2f}s best={best:.2f}s ({a.runs} runs)")
    med, best, (rows, overlap, files) = timed(lambda: q2(lo, hi), a.runs)
    print(f"Q2 {a.label}: silver/orders order_id in [{lo},{hi})  rows={rows} files={files}"
          f" files_overlapping_range={overlap}  median={med:.2f}s best={best:.2f}s ({a.runs} runs)")


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
