#!/usr/bin/env bash
# Silver trigger experiment (ADR-034): `make bench-trigger [TRIGGERS="10 30 60" RATE=50 BENCH_SECONDS=300]`.
#
# For each silver trigger interval, the same workload:
#   1. recreate the streaming driver with SILVER_TRIGGER_INTERVAL="<n> seconds" (bronze stays 10 s),
#      so every run starts from the same cold JVM;
#   2. warm up: simulator at RATE for WARMUP seconds (not measured);
#   3. measure: simulator at RATE for BENCH_SECONDS (bench/silver_bench.sh: batch times), while
#      chaos/bench_sampler.py samples Kafka lag and silver lag every 15 s;
#   4. Delta's MERGE executionTimeMs for the commits in that window (chaos/merge_window.py);
#   5. wait until silver has caught up (make reconcile = also a correctness check), then latency
#      p50/p95 for exactly the rows changed in the measured window.
# Needs the main stack, streaming and `make monitoring-up` (Kafka lag comes from the exporter).
# At the end the driver is recreated with the default 10 s trigger unless KEEP=<n> is given.
set -uo pipefail
cd "$(dirname "$0")/.."
TRIGGERS=${TRIGGERS:-10 30 60}
RATE=${RATE:-50}
SECS=${BENCH_SECONDS:-300}
WARMUP=${WARMUP:-120}
OUT=bench/trigger_results.log
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

for T in $TRIGGERS; do
  echo "=================== silver trigger ${T}s   $(ts)" | tee -a "$OUT"
  SILVER_TRIGGER="$T seconds" docker compose --profile stream up -d --no-deps --force-recreate streaming
  created=$(ts)
  for i in $(seq 1 60); do   # wait until the new driver has started its 3 queries
    [ "$(docker logs --since "$created" streaming 2>&1 | grep -c 'ResolveWriteToStream')" -ge 3 ] && break
    sleep 5
  done
  sleep 20
  docker exec streaming printenv SILVER_TRIGGER_INTERVAL | sed 's/^/  SILVER_TRIGGER_INTERVAL=/' | tee -a "$OUT"
  echo "  warm-up ${WARMUP}s at ${RATE}/s"
  docker compose run --rm simulator --rate "$RATE" --duration "$WARMUP" 2>&1 | grep -E 'stopped after'

  start=$(ts); t0=$(date +%s)
  docker compose run --rm reconcile bench_sampler.py --seconds $((SECS + 30)) --interval 15 \
    --label "trigger-${T}s" > "bench/.sampler_${T}.log" 2>&1 &
  sampler=$!
  RATE=$RATE BENCH_SECONDS=$SECS LABEL="trigger-${T}s" ./bench/silver_bench.sh | tee -a "$OUT"
  end=$(ts)
  wait $sampler
  grep '^LAG' "bench/.sampler_${T}.log" | tee -a "$OUT"
  sed 's/^/    /' "bench/.sampler_${T}.log" | grep '  t=' >> "$OUT"
  docker compose run --rm reconcile merge_window.py --since "$start" --until "$end" --label "trigger-${T}s" | tee -a "$OUT"

  make --no-print-directory reconcile WAIT=600 2>&1 | grep -E '^RECONCILE' | sed "s/^/  trigger-${T}s /" | tee -a "$OUT"
  mins=$(python3 -c "import time; print(round((time.time() - $t0) / 60, 2))")
  docker compose run --rm reconcile latency.py --minutes "$mins" 2>&1 | grep -E '^(ALL|silver_ts)' \
    | sed "s/^/  trigger-${T}s latency /" | tee -a "$OUT"
  rm -f "bench/.sampler_${T}.log"
done

KEEP=${KEEP:-10}
echo "=================== restoring silver trigger ${KEEP}s   $(ts)" | tee -a "$OUT"
SILVER_TRIGGER="$KEEP seconds" docker compose --profile stream up -d --no-deps --force-recreate streaming
