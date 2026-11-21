"""Reconcile Postgres against silver current state. Exit code 1 on any difference.

Per table: row counts, sum(total) / sum(amount), and a full outer join on the primary key
comparing every business column. Also reports duplicate keys inside silver.

  python reconcile.py            one check
  python reconcile.py --wait 300 keep re-checking every 10 s until it passes (silver catching up)
"""
import argparse
import os
import sys
import time
from decimal import Decimal

from common import TABLES, pg_rows, silver_rows

SUMS = {"orders": "total", "payments": "amount"}


def key(row, pk):
    return tuple(row[c] for c in pk)


def check_table(table: str) -> dict:
    pk, cols = TABLES[table]
    pg = {key(r, pk): r for r in pg_rows(table)}
    all_silver = silver_rows(table)
    cur = [r for r in all_silver if not r["_is_deleted"]]
    sv = {key(r, pk): r for r in cur}

    missing = [k for k in pg if k not in sv]       # in Postgres, not in silver
    extra = [k for k in sv if k not in pg]         # in silver, gone from Postgres
    mismatched = [k for k in pg if k in sv and any(pg[k][c] != sv[k][c] for c in cols)]

    res = {
        "table": table,
        "pg": len(pg),
        "silver": len(sv),
        "missing": len(missing),
        "extra": len(extra),
        "mismatch": len(mismatched),
        "dupes": len(all_silver) - len({key(r, pk) for r in all_silver}),
        "sum_pg": None,
        "sum_silver": None,
    }
    if table in SUMS:
        c = SUMS[table]
        res["sum_pg"] = sum((r[c] for r in pg.values()), Decimal(0))
        res["sum_silver"] = sum((r[c] for r in sv.values()), Decimal(0))
    res["ok"] = (res["pg"] == res["silver"] and res["missing"] == res["extra"] == res["mismatch"] == res["dupes"] == 0
                 and res["sum_pg"] == res["sum_silver"])
    if not res["ok"]:
        res["sample"] = (missing[:3], extra[:3], mismatched[:3])
    return res


def run() -> list[dict]:
    return [check_table(t) for t in TABLES]


def show(results: list[dict]) -> None:
    hdr = f"{'table':<12}{'postgres':>10}{'silver':>10}{'missing':>9}{'extra':>7}{'mismatch':>10}{'dupes':>7}  {'sum pg':>14}{'sum silver':>14}  result"
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        sp = "" if r["sum_pg"] is None else str(r["sum_pg"])
        ss = "" if r["sum_silver"] is None else str(r["sum_silver"])
        print(f"{r['table']:<12}{r['pg']:>10}{r['silver']:>10}{r['missing']:>9}{r['extra']:>7}{r['mismatch']:>10}{r['dupes']:>7}  {sp:>14}{ss:>14}  {'OK' if r['ok'] else 'DIFF'}")
        if not r["ok"]:
            print(f"    sample keys (missing, extra, mismatched): {r['sample']}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wait", type=int, default=0, help="seconds to keep retrying until silver catches up")
    args = ap.parse_args()

    deadline = time.time() + args.wait
    while True:
        try:
            results = run()
        except Exception as e:  # silver table not created yet, MinIO hiccup, ...
            if time.time() >= deadline:
                raise
            print(f"not ready yet: {type(e).__name__}: {e}", file=sys.stderr)
            time.sleep(10)
            continue
        if all(r["ok"] for r in results) or time.time() >= deadline:
            break
        print(f"... silver not caught up yet ({sum(1 for r in results if not r['ok'])} tables differ), retrying", flush=True)
        time.sleep(10)

    show(results)
    ok = all(r["ok"] for r in results)
    print("RECONCILE PASS: 0 difference on all tables" if ok else "RECONCILE FAIL", file=sys.stdout)
    return 0 if ok else 1


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    os._exit(code)  # delta-rs/arrow threads can abort at interpreter teardown (exit 139)
