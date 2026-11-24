#!/usr/bin/env bash
# Airflow in one container (ADR-029): scheduler + webserver, LocalExecutor, metadata in a SEPARATE
# database `airflow` on the existing Postgres. No triggerer (no deferrable operators here).
# The compose service sets `init: true`, so docker-init is PID 1 and reaps task processes (ADR-027).
set -eu

python - <<'PY'
# role + database for Airflow's metadata, next to (not inside) the CDC source database `ecom`
import os, time
import psycopg2
for attempt in range(30):
    try:
        c = psycopg2.connect(host=os.environ["SRC_PGHOST"], user=os.environ["SRC_PGUSER"],
                             password=os.environ["SRC_PGPASSWORD"], dbname=os.environ["SRC_PGDATABASE"])
        break
    except psycopg2.OperationalError:
        time.sleep(2)
else:
    raise SystemExit("postgres not reachable")
c.autocommit = True
cur = c.cursor()
cur.execute("select 1 from pg_roles where rolname = 'airflow'")
if not cur.fetchone():
    cur.execute("create role airflow login password 'airflow'")
cur.execute("select 1 from pg_database where datname = 'airflow'")
if not cur.fetchone():
    cur.execute("create database airflow owner airflow")
print("airflow metadata database ready")
PY

airflow db migrate
airflow users list 2>/dev/null | grep -qw admin || \
  airflow users create --username admin --password admin --role Admin \
    --firstname Lakehouse --lastname Admin --email admin@example.com
# one slot: optimize and backfill both pause streaming; they must never overlap
airflow pools set streaming_writer 1 "jobs that pause the streaming driver and write silver"

# The SCHEDULER owns the running tasks, so only its death restarts the container. The webserver
# is just the UI: under full CPU load its gunicorn master once missed the 120 s start timeout,
# the webserver exited, and (when that ended the container) it killed a running optimize task
# whose Spark job then kept going as an orphan (ADR-033). Now it is simply restarted.
airflow scheduler &
SCHED=$!
( while kill -0 "$SCHED" 2>/dev/null; do
    airflow webserver --port 8080
    echo "webserver exited ($?); restarting it in 5 s (scheduler and tasks unaffected)"
    sleep 5
  done ) &
wait "$SCHED"
echo "scheduler exited; restarting the container"
exit 1
