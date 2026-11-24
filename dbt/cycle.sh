#!/usr/bin/env bash
# ONE gold cycle: dbt build -> data quality gate -> publish (ADR-029).
#
#   bash /dbt/cycle.sh <trigger>      trigger = loop | airflow | manual (only recorded)
#   exit 0 = published, 1 = dbt build failed, 2 = blocked by data quality (or the DQ runner crashed)
#
# Called by run_loop.sh every 30 s and by Airflow's dbt_run DAG (docker exec). flock makes them
# take turns: DuckDB allows one writer, and the SCD2 snapshot history lives in the build file, so
# there is exactly one build file and one writer at a time.
#
# Publish = copy the build file and rename it over /data/gold.duckdb (atomic). It only happens
# when every dbt test AND every blocking DQ check passed; otherwise the last good gold stays.
# Every cycle writes /data/last_build.json (status OK | FAILED | BLOCKED, dbt summary, DQ verdict),
# which drives the dashboard banner and the Prometheus exporter.
set -u
cd /dbt
TRIGGER=${1:-manual}
BUILD=${GOLD_BUILD_PATH:-/data/gold_build.duckdb}
SERVE=${GOLD_SERVE_PATH:-/data/gold.duckdb}
STATUS=${GOLD_STATUS_PATH:-/data/last_build.json}
LOCK=${GOLD_LOCK_PATH:-/data/gold.lock}
LOG=/tmp/dbt_build_$$.log
DQ_LOG=/tmp/dq_gate_$$.log
DQ_OUT=/tmp/dq_gate_$$.json
trap 'rm -f "$LOG" "$DQ_LOG" "$DQ_OUT"' EXIT

exec 9>"$LOCK"
flock 9                           # wait for a cycle started by the other caller
t0=$(date +%s)
secs() { echo $(( $(date +%s) - t0 )); }
plain() { sed -E 's/\x1b\[[0-9;]*m//g' "$1"; }
status() { python3 /dbt/status.py "$1" "$(secs)" "$2" "$STATUS" "$TRIGGER" "${3:-}"; }
stamp() { date -u +%FT%TZ; }

if ! dbt build --profiles-dir . > "$LOG" 2>&1; then
  done_line=$(plain "$LOG" | grep -oE 'Done\..*' | tail -1)
  first_err=$(plain "$LOG" | grep -E '(Compilation|Runtime|Database) Error|Failure in' | head -1 | sed -E 's/^[0-9:]+ +//')
  status FAILED "${done_line:-dbt exited non-zero} | ${first_err}"
  echo "$(stamp) [$TRIGGER] build FAILED after $(secs)s, gold NOT published | $done_line"
  plain "$LOG" | grep -E 'ERROR|FAIL|Error|Failure' | head -20
  exit 1
fi
done_line=$(plain "$LOG" | grep -oE 'Done\..*' | tail -1)

/opt/soda/bin/python /quality/dq.py --source build --db "$BUILD" --out "$DQ_OUT" > "$DQ_LOG" 2>&1
dq_rc=$?
dq_line=$(grep -E '^DQ ' "$DQ_LOG" | tail -1)
if [ "$dq_rc" -ne 0 ]; then
  [ -n "$dq_line" ] || dq_line="DQ ERROR: runner exited $dq_rc: $(tail -1 "$DQ_LOG")"
  status BLOCKED "$dq_line" "$DQ_OUT"
  echo "$(stamp) [$TRIGGER] build OK in $(secs)s but gold NOT published | $dq_line"
  exit 2
fi

cp "$BUILD" "$SERVE.tmp" && mv -f "$SERVE.tmp" "$SERVE"
status OK "$done_line | $dq_line" "$DQ_OUT"
echo "$(stamp) [$TRIGGER] build OK in $(secs)s, published $SERVE | $done_line | $dq_line"
exit 0
