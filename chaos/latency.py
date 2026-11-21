"""End-to-end latency: Postgres `updated_at` (set by now() in the writing transaction)
vs silver `_silver_ts` (when the MERGE ran), for rows whose latest version arrived through
CDC (`_op` c/u, so snapshot reads are excluded).

  python latency.py --minutes 5     rows updated in the last 5 minutes

Caveat, stated in the README: silver keeps only the LATEST version of each key, so each
row contributes one sample (its last change), and a row that changed again is measured on
that later change.
"""
import argparse
import os
import sys
import math
from datetime import datetime, timedelta, timezone

from common import silver_rows

TABLES_WITH_UPDATED_AT = ["orders", "payments", "users", "products"]


def pct(sorted_vals, p):
    if not sorted_vals:
        return float("nan")
    return sorted_vals[min(len(sorted_vals) - 1, math.ceil(p / 100 * len(sorted_vals)) - 1)]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=5.0)
    args = ap.parse_args()
    since = (datetime.now(timezone.utc) - timedelta(minutes=args.minutes)).replace(tzinfo=None)

    allv, allsrc = [], []
    print(f"{'table':<10}{'n':>8}{'p50 s':>9}{'p95 s':>9}{'p99 s':>9}{'max s':>9}   (silver_ts - updated_at, last {args.minutes:g} min)")
    for t in TABLES_WITH_UPDATED_AT:
        rows = [r for r in silver_rows(t)
                if not r["_is_deleted"] and r["_op"] in ("c", "u") and r["updated_at"] >= since]
        v = sorted((r["_silver_ts"] - r["updated_at"]).total_seconds() for r in rows)
        allv += v
        allsrc += [(r["_silver_ts"] - r["_source_ts"]).total_seconds() for r in rows]
        print(f"{t:<10}{len(v):>8}{pct(v, 50):>9.2f}{pct(v, 95):>9.2f}{pct(v, 99):>9.2f}{(v[-1] if v else float('nan')):>9.2f}")
    allv.sort()
    allsrc.sort()
    print(f"{'ALL':<10}{len(allv):>8}{pct(allv, 50):>9.2f}{pct(allv, 95):>9.2f}{pct(allv, 99):>9.2f}{(allv[-1] if allv else float('nan')):>9.2f}")
    print(f"\nsilver_ts - Debezium source commit ts (Kafka+Spark share): p50={pct(allsrc, 50):.2f}s p95={pct(allsrc, 95):.2f}s")


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)  # delta-rs/arrow threads can abort at interpreter teardown (exit 139)
