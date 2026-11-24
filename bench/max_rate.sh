#!/usr/bin/env bash
# Highest sustainable rate: `make bench-rate [RATES="100 200 400" BENCH_SECONDS=300]`.
#
# For each rate: run the simulator for BENCH_SECONDS while chaos/bench_sampler.py samples Kafka lag
# (Kafka -> bronze) and silver lag (now - newest change in silver) every 15 s. Then wait until
# silver has caught up (make reconcile, also a correctness check) before the next rate.
#
# "Flat" (the rule used in bench/results.md): the median of the last third of the samples is at most
# the median of the first third plus one bronze trigger of input (RATE * 10 messages). Lag is a
# sawtooth (it fills for 10 s, then a batch drains it), so single samples and the slope are noisy.
# Needs `make monitoring-up` for the Kafka lag.
set -uo pipefail
cd "$(dirname "$0")/.."
RATES=${RATES:-100 200 400}
SECS=${BENCH_SECONDS:-300}
SIM_ARGS=${SIM_ARGS:-}   # e.g. --fast (synchronous_commit=off) above ~400/s, where one connection tops out
OUT=bench/rate_results.log
ts() { date -u +%Y-%m-%dT%H:%M:%SZ; }

for R in $RATES; do
  echo "=================== rate ${R}/s for ${SECS}s   $(ts)" | tee -a "$OUT"
  docker compose run --rm reconcile bench_sampler.py --seconds "$SECS" --interval 15 \
    --label "rate-${R}" > "bench/.rate_${R}.log" 2>&1 &
  sampler=$!
  docker compose run --rm simulator --rate "$R" --duration "$SECS" $SIM_ARGS 2>&1 | grep -E 'stopped after' \
    | sed "s/^/  rate-${R} simulator: /" | tee -a "$OUT"
  wait $sampler
  grep '^LAG' "bench/.rate_${R}.log" | tee -a "$OUT"
  grep '  t=' "bench/.rate_${R}.log" | sed 's/^/    /' >> "$OUT"
  rm -f "bench/.rate_${R}.log"
  t0=$(date +%s)
  make --no-print-directory reconcile WAIT=1200 2>&1 | grep -E '^RECONCILE' | sed "s/^/  rate-${R} /" | tee -a "$OUT"
  echo "  rate-${R} silver caught up $(( $(date +%s) - t0 )) s after the simulator stopped" | tee -a "$OUT"
done
