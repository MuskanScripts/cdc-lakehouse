# 2-minute demo script

Four things in two minutes: live data → dashboard, a schema change, a crash with recovery, and a
bad batch that gets blocked. Some steps take longer than their slot in real time (CDC + silver
take a minute or two), so start them **before** the slot and cut the recording. Every number
I say out loud comes from the README.

## Before recording (about 10 minutes)

```bash
make up && make stream-up && make gold-up && make monitoring-up
make simulate RATE=50              # terminal 1: leave it running for the whole demo
```

- Wait until the dashboard (http://localhost:8501) shows revenue per minute moving and "Gold
  built" under 30 s.
- Open four browser tabs: dashboard :8501, Grafana :3000 (dashboard "CDC Lakehouse: pipeline
  health", last 15 min), the README's architecture diagram, and a terminal with a large font.
- Pick a column name that has never been used, e.g. `COL=demo_$(date +%H%M)`. The drift drill
  only means something the first time for a column.

## The recording

| time | screen | what I do | what I say |
|---|---|---|---|
| 0:00-0:15 | README diagram | point at the flow | "Every row change in Postgres is read from the WAL by Debezium, goes through Kafka into Delta Lake on MinIO with an idempotent MERGE, then dbt builds gold behind a data quality gate." |
| 0:15-0:35 | dashboard | let it refresh once | "This is live. A simulator is writing orders, payments and refunds into Postgres at 50 changes a second. Revenue per minute and refund rate come from gold, which rebuilds every 30 seconds." |
| 0:35-0:55 | terminal | `make drift COL=demo_1234` (started about 60 s before this slot; show its output) | "I add a column to the live orders table. The schema registry has a new version about 2 seconds later, and silver has the column within the next micro-batches: 27 s when idle, 74 s under load in my measurements. Old rows get null, and nothing restarts." |
| 0:55-1:20 | terminal | `docker kill streaming`, then `docker ps \| grep streaming` (Up again), then the output of `make reconcile WAIT=300` started earlier | "Now I kill the Spark driver mid-batch. It comes back by itself and resumes from its checkpoint. Reconcile compares every row with Postgres: zero missing, zero extra, zero mismatched, zero duplicates. I ran this with ten random kills, twice." |
| 1:20-1:50 | dashboard + Grafana | `make bad-batch` was run about 2.5 min earlier; show the red banner and Grafana's "Gold: last cycle = BLOCKED" | "Here I injected five orders with negative totals directly into Postgres. The quality gate caught them, quarantined the rows and alerted, and gold was NOT published: the dashboard keeps the last good numbers and says so in red." |
| 1:50-2:00 | terminal → dashboard | `make bad-batch-fix`, cut to the banner gone (about 1 min later in real time) | "I fix the data at the source, it flows through CDC like any other change, and the next cycle publishes again. That's the project." |

## Notes for the edit

- Cut points: after `make drift` starts, after `docker kill`, and between `make bad-batch-fix`
  and the green dashboard.
- In the README drill, the bad batch took 121 s from insert to "gold blocked" and 66 s from the
  fix to the next publish. Start `make bad-batch` early enough.
- If the reconcile shows DIFF, silver is still catching up: wait for `WAIT=300` to finish
  instead of retaking.
- After recording: `make bad-batch-fix` (if not already), stop the simulator with Ctrl+C.
