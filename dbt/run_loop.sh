#!/usr/bin/env bash
# Gold loop: one gold cycle (cycle.sh: dbt build -> DQ gate -> publish) every DBT_INTERVAL seconds.
#
# The loop is the fast path for dashboard freshness. Airflow's dbt_run DAG runs the SAME cycle
# every 5 minutes for history and alerting; flock in cycle.sh makes the two take turns (ADR-029).
# What a cycle guarantees (ADR-024, ADR-027, ADR-029):
#   - the dashboard never waits on DuckDB's single-writer lock and never sees a half-written build,
#   - a failing dbt test OR a failing blocking DQ check leaves the last good gold in place,
#   - every cycle, OK or not, is recorded in /data/last_build.json (red banner when not OK).
#
# Must not run as PID 1: the compose service sets `init: true` (ADR-027).
set -u
cd /dbt
INTERVAL=${DBT_INTERVAL:-30}

if [ "$$" = 1 ]; then
  echo "WARNING: run_loop.sh is PID 1; dbt builds are known to break like this (ADR-027). Use init: true."
fi

dbt deps --profiles-dir . >/dev/null 2>&1 || true
while true; do
  t0=$(date +%s)
  bash /dbt/cycle.sh loop
  # fixed RATE, not fixed pause: start a cycle every INTERVAL s. With a fixed 30 s pause, a 39 s
  # build under load made the cycle ~70 s and gold 71 s old on the dashboard (ADR-027).
  elapsed=$(( $(date +%s) - t0 ))
  sleep $(( INTERVAL > elapsed + 2 ? INTERVAL - elapsed : 2 ))
done
