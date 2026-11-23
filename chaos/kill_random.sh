#!/usr/bin/env bash
# Kill random pipeline containers at random intervals (SIGKILL, no graceful shutdown).
#
#   KILLS=10 MIN_GAP=20 MAX_GAP=60 ./chaos/kill_random.sh
#
# Targets: streaming (the Spark driver: bronze + silver + aggregate), connect (Debezium) and
# kafka. Each kill is followed by a random downtime, then the container is started again.
# Postgres and MinIO are NOT killed: Postgres is the source of truth we reconcile against, and
# the task is to show the pipeline survives losing its processing and transport layers.
#
# A small watchdog runs alongside, because Kafka Connect does not restart a FAILED task by
# itself (a task can fail when its producer can't reach a broker that was just killed). It plays
# the part of the operator / orchestrator: it restarts failed connector tasks through the REST
# API and logs every time it had to. The count is printed at the end.
set -uo pipefail

KILLS=${KILLS:-10}
MIN_GAP=${MIN_GAP:-20}
MAX_GAP=${MAX_GAP:-60}
MIN_DOWN=${MIN_DOWN:-3}
MAX_DOWN=${MAX_DOWN:-12}
TARGETS=(${TARGETS:-streaming connect kafka})
CONNECT=${CONNECT_URL:-http://localhost:8083}
CONNECTOR=${CONNECTOR:-ecom-postgres}
HEALS=$(mktemp)

ts()   { date +%H:%M:%S; }
rand() { echo $(( $1 + RANDOM % ($2 - $1 + 1) )); }
running() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]; }

watchdog() {
  while true; do
    status=$(curl -s -m 5 "$CONNECT/connectors/$CONNECTOR/status" 2>/dev/null || true)
    if grep -q '"state":"FAILED"' <<<"$status"; then
      curl -s -m 10 -X POST "$CONNECT/connectors/$CONNECTOR/restart?includeTasks=true&onlyFailed=true" >/dev/null 2>&1
      echo "$(ts) [watchdog] connector task was FAILED -> restart requested"
      echo x >> "$HEALS"
    fi
    sleep 5
  done
}

watchdog &
WD=$!
trap 'kill $WD 2>/dev/null; rm -f "$HEALS"' EXIT

echo "$(ts) chaos start: $KILLS kills among [${TARGETS[*]}], gap ${MIN_GAP}-${MAX_GAP}s, downtime ${MIN_DOWN}-${MAX_DOWN}s"
for i in $(seq 1 "$KILLS"); do
  sleep "$(rand "$MIN_GAP" "$MAX_GAP")"
  target=${TARGETS[$((RANDOM % ${#TARGETS[@]}))]}
  down=$(rand "$MIN_DOWN" "$MAX_DOWN")
  echo "$(ts) kill $i/$KILLS: docker kill $target (down ${down}s)"
  docker kill "$target" >/dev/null 2>&1 || echo "$(ts)   (kill failed: $target was not running)"
  sleep "$down"
  running "$target" || docker start "$target" >/dev/null
  echo "$(ts)   $target is back"
done

# let everything settle and make sure all targets are up
for t in "${TARGETS[@]}"; do running "$t" || docker start "$t" >/dev/null; done
sleep 20
echo "$(ts) chaos done: $KILLS kills, watchdog restarted a failed connector task $(wc -l < "$HEALS") time(s)"
