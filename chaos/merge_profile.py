"""Where does a silver MERGE spend its time? Delta's own metrics from the commit history.

  python merge_profile.py [N]      the last N MERGE commits per silver table (default 3)

Delta records per MERGE: executionTimeMs (whole command), scanTimeMs (finding matched rows and
the files that hold them), rewriteTimeMs (writing the new files), plus files/rows touched.
Compare executionTimeMs with the wall-clock `merge=` in the streaming log: the difference is time
spent outside the MERGE command itself (log snapshot, queueing for CPU, the commit).
"""
import os
import sys
from datetime import datetime, timezone

from deltalake import DeltaTable

from common import STORAGE, TABLES

KEYS = ["executionTimeMs", "scanTimeMs", "rewriteTimeMs", "numSourceRows", "numTargetRowsUpdated",
        "numTargetRowsInserted", "numTargetRowsCopied", "numTargetFilesRemoved", "numTargetFilesAdded"]


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    print(f"{'table':<12}{'ver':>5} {'commit (UTC)':<9}{'exec s':>8}{'scan s':>8}{'rewrite s':>10}"
          f"{'src rows':>9}{'upd':>6}{'ins':>6}{'copied':>8}{'files -/+':>11}")
    for t in TABLES:
        dt = DeltaTable(f"s3://lakehouse/silver/{t}", storage_options=STORAGE)
        merges = [h for h in dt.history(limit=40) if h.get("operation") == "MERGE"][:n]
        for h in merges:
            m = {k: h.get("operationMetrics", {}).get(k) for k in KEYS}
            ts = datetime.fromtimestamp(h["timestamp"] / 1000, tz=timezone.utc).strftime("%H:%M:%S")
            f = lambda k: int(m[k]) if m[k] is not None else -1  # noqa: E731
            print(f"{t:<12}{h['version']:>5} {ts:<9}{f('executionTimeMs') / 1000:>8.1f}{f('scanTimeMs') / 1000:>8.1f}"
                  f"{f('rewriteTimeMs') / 1000:>10.1f}{f('numSourceRows'):>9}{f('numTargetRowsUpdated'):>6}"
                  f"{f('numTargetRowsInserted'):>6}{f('numTargetRowsCopied'):>8}"
                  f"{str(f('numTargetFilesRemoved')) + '/' + str(f('numTargetFilesAdded')):>11}")


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
