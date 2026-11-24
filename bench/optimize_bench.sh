#!/usr/bin/env bash
# OPTIMIZE / ZORDER before/after: `make bench-optimize` (bench/results.md).
#
#   before:  file count + avg file size (bronze and silver) + the fixed queries (chaos/bench_query.py)
#   then:    streaming stopped; OPTIMIZE every bronze table (compaction; bronze has no business key
#            to ZORDER by) and OPTIMIZE ... ZORDER BY <first pk> every silver table, no VACUUM
#            (VACUUM only deletes unreferenced files, it does not change what a query reads)
#   after:   the same stats and queries, streaming still stopped
#   decay:   streaming restarted, simulator at 50/s for DECAY seconds, then silver stats + queries
#            again: does the ZORDER layout survive the next MERGEs?
# Run it with the simulator stopped.
set -uo pipefail
cd "$(dirname "$0")/.."
OUT=bench/optimize_results.log
DECAY=${DECAY:-180}
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }
stats() {   # $1 = label
  for layer in bronze silver; do
    echo "-- $layer ($1)"; docker compose run --rm reconcile table_stats.py "$layer" 2>/dev/null
  done
  docker compose run --rm reconcile bench_query.py --runs 5 --label "$1" 2>/dev/null
}
rm -f chaos/.bench_q2_range   # fix the Q2 key range at the "before" run
{
  echo "=================== OPTIMIZE bench   $(ts)"
  stats before
  docker compose --profile stream stop streaming
  docker compose up -d spark
  for args in "bronze --no-vacuum" "silver --no-vacuum --zorder"; do
    echo "-- OPTIMIZE $args"
    docker compose exec -T spark bash /opt/project/spark/exclusive.sh --driver-memory 1500m \
      --packages io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4 \
      --conf spark.jars.ivy=/root/.ivy2 /opt/project/spark/optimize.py $args 2>&1 \
      | grep -vE 'WARN|INFO|^\s*$|::|resolving|found |downloading|artifacts|confs|---|\| '
  done
  stats after-optimize
  docker compose --profile stream up -d streaming
  echo "-- decay: simulator 50/s for ${DECAY}s with streaming running   $(ts)"
  docker compose run --rm simulator --rate 50 --duration "$DECAY" 2>&1 | grep -E 'stopped after'
  make --no-print-directory reconcile WAIT=600 2>&1 | grep -E '^RECONCILE'
  echo "-- silver (after ${DECAY}s of MERGEs)"; docker compose run --rm reconcile table_stats.py silver 2>/dev/null
  docker compose run --rm reconcile bench_query.py --runs 5 --label "after-${DECAY}s-of-merges" 2>/dev/null
  echo "=================== done   $(ts)"
} 2>&1 | tee -a "$OUT"
