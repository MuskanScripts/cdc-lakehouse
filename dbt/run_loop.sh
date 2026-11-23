#!/usr/bin/env bash
# Gold build loop (until Airflow takes over in Week 5).
#
# Every DBT_INTERVAL seconds: `dbt build` (snapshot + models + tests) into the BUILD file. Only if
# everything is green, the build file is copied and atomically renamed to /data/gold.duckdb,
# which is what the dashboard opens (read-only, a fresh connection per refresh). So:
#   - the dashboard never waits on DuckDB's single-writer lock,
#   - it never sees a half-written build,
#   - a failing dbt test leaves the last good gold in place (ADR-024).
set -u
cd /dbt
INTERVAL=${DBT_INTERVAL:-30}
BUILD=${GOLD_BUILD_PATH:-/data/gold_build.duckdb}
SERVE=${GOLD_SERVE_PATH:-/data/gold.duckdb}

dbt deps --profiles-dir . >/dev/null 2>&1 || true
while true; do
  t0=$(date +%s)
  if dbt build --profiles-dir . > /tmp/dbt_build.log 2>&1; then
    cp "$BUILD" "$SERVE.tmp" && mv -f "$SERVE.tmp" "$SERVE"
    echo "$(date -u +%FT%TZ) build OK in $(( $(date +%s) - t0 ))s, published $SERVE | $(grep -E '^Done\.' /tmp/dbt_build.log)"
  else
    echo "$(date -u +%FT%TZ) build FAILED after $(( $(date +%s) - t0 ))s, gold NOT published"
    grep -E 'ERROR|FAIL|Error|Failure' /tmp/dbt_build.log | head -20
  fi
  sleep "$INTERVAL"
done
