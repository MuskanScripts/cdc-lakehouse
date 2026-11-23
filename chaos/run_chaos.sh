#!/usr/bin/env bash
# `make chaos`: simulator running + 10 random container kills + reconcile at the end.
#
#   RATE=100 KILLS=10 ./chaos/run_chaos.sh
#
# 1. checks the pipeline is healthy and already reconciled (so a diff afterwards is ours)
# 2. starts the simulator in the background
# 3. chaos/kill_random.sh kills streaming / connect / kafka at random
# 4. stops the simulator, waits for silver to catch up, runs reconcile (exit code = result)
set -uo pipefail
cd "$(dirname "$0")/.."

RATE=${RATE:-100}
KILLS=${KILLS:-10}
LOG=${LOG:-chaos/last_run.log}
exec > >(tee "$LOG") 2>&1

for s in postgres kafka connect streaming; do
  [ "$(docker inspect -f '{{.State.Running}}' "$s" 2>/dev/null)" = "true" ] || { echo "$s is not running: make up && make stream-up"; exit 2; }
done

echo "== before: reconcile must already be clean"
docker compose run --rm --build reconcile reconcile.py --wait 120 || { echo "not clean before the run, aborting"; exit 2; }
echo "== before: bronze duplicate offsets (baseline)"
docker compose run --rm reconcile bronze_check.py || true

echo "== simulator at $RATE changes/s"
docker compose run -d --rm --build simulator --rate "$RATE" --duration 0 >/dev/null 2>&1
sleep 15
docker ps --filter name=simulator-run --format '  simulator container: {{.Names}} ({{.Status}})'

KILLS=$KILLS ./chaos/kill_random.sh

echo "== stopping simulator (graceful SIGTERM)"
docker ps -q --filter name=simulator-run | xargs -r docker stop -t 30 >/dev/null
echo "== waiting for silver to catch up, then reconcile"
docker compose run --rm reconcile reconcile.py --wait 900
code=$?
echo "== bronze duplicate offsets (after)"
docker compose run --rm reconcile bronze_check.py || true
echo "== order-event delay"
docker compose run --rm reconcile event_delay.py --minutes 60 || true
echo "== streaming restarts / errors in the log"
docker logs streaming 2>&1 | grep -ciE "exception|error" | sed 's/^/error-ish log lines: /'
exit $code
