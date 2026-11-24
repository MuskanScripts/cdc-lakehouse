"""Delta's own MERGE timer (operationMetrics.executionTimeMs) for silver MERGEs in a time window.

  python merge_window.py --since 2026-11-24T10:00:00Z --until 2026-11-24T10:05:00Z [--label x]

Prints, per table, the number of MERGE commits and the median / max executionTimeMs plus median
source rows per MERGE, and one MERGE line over all tables (used by bench/silver_trigger.sh).
"""
import argparse
import os
import statistics as st
import sys
from datetime import datetime

from deltalake import DeltaTable

from common import STORAGE, TABLES


def ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True)
    ap.add_argument("--until", required=True)
    ap.add_argument("--label", default="run")
    a = ap.parse_args()
    lo, hi = ms(a.since), ms(a.until)
    allx, per = [], []
    for t in TABLES:
        dt = DeltaTable(f"s3://lakehouse/silver/{t}", storage_options=STORAGE)
        hs = [h for h in dt.history(limit=400)
              if h.get("operation") == "MERGE" and lo <= h["timestamp"] <= hi]
        ex = [int(h["operationMetrics"]["executionTimeMs"]) / 1000 for h in hs]
        src = [int(h["operationMetrics"].get("numSourceRows", 0)) for h in hs]
        allx += ex
        if ex:
            per.append(f"{t}={st.median(ex):.1f}")
            print(f"  {t:<12} merges={len(ex):>3} exec_s median={st.median(ex):5.1f} max={max(ex):5.1f}"
                  f" src_rows median={st.median(src):.0f}")
    if not allx:
        print(f"MERGE {a.label}: no MERGE commits in the window")
        return
    print(f"MERGE {a.label}: merges={len(allx)} executionTimeMs median={st.median(allx) * 1000:.0f}"
          f" max={max(allx) * 1000:.0f} | per table median s {' '.join(per)}")


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
