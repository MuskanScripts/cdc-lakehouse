"""Write /data/last_build.json atomically after every gold cycle (cycle.sh).

  python3 status.py <OK|FAILED|BLOCKED> <duration_s> <summary> <path> <trigger> [dq_verdict.json]

`last_published_at` is carried over from the previous file when this cycle did not publish, so
readers can tell how old the published gold is without opening it.
"""
import datetime
import json
import os
import sys

status, dur, summary, path, trigger = sys.argv[1:6]
dq_path = sys.argv[6] if len(sys.argv) > 6 else ""
now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

previous = {}
try:
    with open(path) as f:
        previous = json.load(f)
except (OSError, ValueError):
    pass

dq = None
if dq_path and os.path.exists(dq_path):
    with open(dq_path) as f:
        v = json.load(f)
    dq = {k: v.get(k) for k in ("status", "failed", "warned", "checks", "checked_at", "duration_s")}

doc = {"status": status, "finished_at": now, "duration_s": float(dur), "summary": summary,
       "trigger": trigger, "dq": dq,
       "last_published_at": now if status == "OK" else previous.get("last_published_at")}
with open(path + ".tmp", "w") as f:
    json.dump(doc, f)
os.replace(path + ".tmp", path)  # atomic: the dashboard never reads half a file
