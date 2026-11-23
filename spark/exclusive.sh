#!/usr/bin/env bash
# Run at most ONE silver-writing Spark job in the spark container at a time (ADR-033).
#
#   bash exclusive.sh <spark-submit args...>    run the job, holding /tmp/silver_writer.lock
#   bash exclusive.sh --kill                    stop the job that holds the lock (if any)
#
# Why: Airflow starts these jobs with `docker exec`. If the Airflow task process dies (container
# restart, timeout, SIGKILL), the exec'd job keeps running; the retry would start a SECOND writer.
# Airflow's pool already allows one writer task, so whoever holds this lock when a new job starts
# is an orphan: it is stopped (TERM, then KILL after 30 s) before the new job takes the lock.
# Killing a Delta writer mid-job is a crash, which Delta handles: an uncommitted job leaves only
# unreferenced files.
set -u
LOCK=/tmp/silver_writer.lock
PIDF=/tmp/silver_writer.pid

stop_holder() {
  local pid
  pid=$(cat "$PIDF" 2>/dev/null || true)
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || return 0
  echo "exclusive.sh: stopping writer job pid $pid"
  kill -TERM "$pid" 2>/dev/null
  for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || return 0; sleep 1; done
  kill -KILL "$pid" 2>/dev/null
  sleep 1
}

if [ "${1:-}" = "--kill" ]; then
  stop_holder
  exit 0
fi

exec 8>"$LOCK"
if ! flock -n 8; then
  echo "exclusive.sh: lock held by an orphaned job"
  stop_holder
  flock -w 60 8 || { echo "exclusive.sh: could not get the writer lock"; exit 75; }
fi
echo $$ > "$PIDF"
# exec keeps this PID (spark-submit execs java too), so the pid file and the lock stay with the job
exec /opt/spark/bin/spark-submit "$@"
