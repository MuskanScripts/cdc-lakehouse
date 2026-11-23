#!/usr/bin/env bash
# Silver batch timings under a fixed load: `make bench-silver [RATE=50 BENCH_SECONDS=180 LABEL=before]`.
#
# Runs the simulator at RATE for SECONDS, then parses the `silver batch=...` lines that the
# streaming driver logged in that window (docker log timestamps) and prints count / median / p90 /
# max of the batch duration, the median MERGE time per table and the median rows per batch.
# Appends one summary line to bench/silver_bench.log so before/after runs sit next to each other.
set -uo pipefail
cd "$(dirname "$0")/.."
RATE=${RATE:-50}
SECONDS_=${BENCH_SECONDS:-180}   # not $SECONDS: that is a bash built-in (shell uptime)
LABEL=${LABEL:-run}

start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker compose run --rm simulator --rate "$RATE" --duration "$SECONDS_" 2>&1 | grep -E 'stopped after'
sleep 15   # let the last batch of the window finish
end=$(date -u +%Y-%m-%dT%H:%M:%SZ)

docker logs -t --since "$start" --until "$end" streaming 2>&1 | grep 'silver batch=' | python3 -c '
import re, statistics as st, sys
label, rate = sys.argv[1], sys.argv[2]
took, merges, rows = [], {}, []
for line in sys.stdin:
    m = re.search(r"took ([\d.]+)s", line)
    if not m:
        continue
    took.append(float(m.group(1)))
    n = 0
    for t, merged, merge in re.findall(r"(\w+): merged=(\d+) .*?merge=([\d.]+)s", line):
        merges.setdefault(t, []).append(float(merge)); n += int(merged)
    rows.append(n)
if not took:
    sys.exit("no silver batches in the window")
took.sort()
p90 = took[min(len(took) - 1, int(0.9 * len(took)))]
mm = " ".join(f"{t}={st.median(v):.1f}" for t, v in merges.items())
s = (f"{label}: rate={rate}/s batches={len(took)} batch_s median={st.median(took):.1f} p90={p90:.1f} "
     f"max={took[-1]:.1f} | rows/batch median={st.median(rows):.0f} | median merge_s {mm}")
print(s)
open("bench/silver_bench.log", "a").write(s + "\n")
' "$LABEL" "$RATE"
