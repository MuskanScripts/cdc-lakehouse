#!/usr/bin/env bash
# Gold build loop (until Airflow takes over in Week 5).
#
# Every DBT_INTERVAL seconds: `dbt build` (snapshot + models + tests) into the BUILD file. Only if
# everything is green, the build file is copied and atomically renamed to /data/gold.duckdb,
# which is what the dashboard opens (read-only, a fresh connection per refresh). So:
#   - the dashboard never waits on DuckDB's single-writer lock,
#   - it never sees a half-written build,
#   - a failing dbt test leaves the last good gold in place (ADR-024).
# After EVERY build, OK or not, the result goes to /data/last_build.json, so the dashboard can
# show a red banner when builds fail instead of quietly serving old gold (ADR-027).
#
# Must not run as PID 1: the compose service sets `init: true` (ADR-027).
set -u
cd /dbt
INTERVAL=${DBT_INTERVAL:-30}
BUILD=${GOLD_BUILD_PATH:-/data/gold_build.duckdb}
SERVE=${GOLD_SERVE_PATH:-/data/gold.duckdb}
STATUS=${GOLD_STATUS_PATH:-/data/last_build.json}

if [ "$$" = 1 ]; then
  echo "WARNING: run_loop.sh is PID 1; dbt builds are known to break like this (ADR-027). Use init: true."
fi

write_status() {  # status duration summary
  python3 - "$1" "$2" "$3" "$STATUS" <<'PY'
import json, os, sys, datetime
status, dur, summary, path = sys.argv[1], float(sys.argv[2]), sys.argv[3], sys.argv[4]
doc = {"status": status, "finished_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
       "duration_s": dur, "summary": summary}
json.dump(doc, open(path + ".tmp", "w"))
os.replace(path + ".tmp", path)  # atomic: the dashboard never reads half a file
PY
}

dbt deps --profiles-dir . >/dev/null 2>&1 || true
while true; do
  t0=$(date +%s)
  if dbt build --profiles-dir . > /tmp/dbt_build.log 2>&1; then
    cp "$BUILD" "$SERVE.tmp" && mv -f "$SERVE.tmp" "$SERVE"
    done_line=$(sed -E 's/\x1b\[[0-9;]*m//g' /tmp/dbt_build.log | grep -oE 'Done\..*' | tail -1)
    write_status OK "$(( $(date +%s) - t0 ))" "$done_line"
    echo "$(date -u +%FT%TZ) build OK in $(( $(date +%s) - t0 ))s, published $SERVE | $done_line"
  else
    done_line=$(sed -E 's/\x1b\[[0-9;]*m//g' /tmp/dbt_build.log | grep -oE 'Done\..*' | tail -1)
    first_err=$(sed -E 's/\x1b\[[0-9;]*m//g' /tmp/dbt_build.log | grep -E '(Compilation|Runtime|Database) Error|Failure in' | head -1 | sed -E 's/^[0-9:]+ +//')
    write_status FAILED "$(( $(date +%s) - t0 ))" "${done_line:-dbt exited non-zero} | ${first_err}"
    echo "$(date -u +%FT%TZ) build FAILED after $(( $(date +%s) - t0 ))s, gold NOT published | $done_line"
    sed -E 's/\x1b\[[0-9;]*m//g' /tmp/dbt_build.log | grep -E 'ERROR|FAIL|Error|Failure' | head -20
  fi
  # fixed RATE, not fixed pause: start a build every INTERVAL s. With a fixed 30 s pause, a 39 s
  # build under load made the cycle ~70 s and gold 71 s old on the dashboard (ADR-027).
  elapsed=$(( $(date +%s) - t0 ))
  sleep $(( INTERVAL > elapsed + 2 ? INTERVAL - elapsed : 2 ))
done
