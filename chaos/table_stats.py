"""File counts and sizes of the silver tables, read from the Delta log (delta-rs, no JVM).

  python table_stats.py            silver
  python table_stats.py bronze

active files = files in the current snapshot (what a query reads).
"""
import os
import sys

from deltalake import DeltaTable

from common import STORAGE, TABLES


def main() -> None:
    layer = sys.argv[1] if len(sys.argv) > 1 else "silver"
    print(f"{'table':<12}{'version':>8}{'active files':>14}{'rows':>10}{'MiB':>9}{'avg KiB/file':>14}")
    tot_f = tot_b = 0
    for t in TABLES:
        dt = DeltaTable(f"s3://lakehouse/{layer}/{t}", storage_options=STORAGE)
        acts = dt.get_add_actions(flatten=True).to_pydict()
        n, size = len(acts["path"]), sum(acts["size_bytes"])
        rows = sum(x or 0 for x in acts.get("num_records", []))
        tot_f, tot_b = tot_f + n, tot_b + size
        print(f"{t:<12}{dt.version():>8}{n:>14}{rows:>10}{size / 2**20:>9.2f}{size / max(n, 1) / 1024:>14.1f}")
    print(f"{'TOTAL':<12}{'':>8}{tot_f:>14}{'':>10}{tot_b / 2**20:>9.2f}{tot_b / max(tot_f, 1) / 1024:>14.1f}")


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
