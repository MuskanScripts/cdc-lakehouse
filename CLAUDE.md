# CLAUDE.md — CDC Lakehouse (portfolio project 1)

You are helping Rohit build a real-time CDC lakehouse as a data-engineering portfolio
project. It must run with one command, be correct under failure, and be explainable in an
interview. Read this whole file before doing anything.

## Goal in one line
Capture every row change from Postgres' WAL with Debezium, stream it through Kafka, apply it
to Delta Lake on MinIO with an idempotent MERGE, enforce quality gates, and serve gold tables
to a live dashboard. Numbers must match Postgres exactly after crashes, schema changes and late
events.

## Environment (important)
- Windows laptop, **WSL2 Ubuntu**, Docker Engine 29 + Compose, repo at `~/cdc-lakehouse`.
  Always run commands inside WSL in this folder. Rohit works from an open Ubuntu terminal, so
  WSL stays up; no keep-alive tricks needed.
- **RAM is still limited (~10 GB for WSL; it was 7.5 GB).** Before adding a long-running service,
  check `free -h` and `docker stats --no-stream`. Stop services not needed for the current week
  (e.g. `docker compose stop kafka-ui`; `make up` starts it, so stop it again). Prefer one Spark
  JVM over many. The streaming driver uses `local[4]`, `--driver-memory 1500m`, and a 2.8 GB
  container limit; it was measured at ~2.6 GiB. Re-measure before changing any of these.
- If containers are gone anyway (WSL restarted), run `make up && make stream-up`. Volumes and
  streaming checkpoints persist, so everything resumes where it left off. Long jobs should be
  compose services with `restart: unless-stopped`.
- MinIO comes from `cgr.dev/chainguard/minio`, because `minio/minio` was removed from Docker
  Hub. That image is distroless, with no shell.
- The Spark image runs **Python 3.8**: use `from __future__ import annotations` for `X | None`
  and `list[str]` hints in anything that runs there.
- Corporate laptop: if a download is blocked, stop and tell Rohit. Don't silently switch tools.

## Current state (Weeks 1-6 DONE: project complete)
**Week 6 (done):** benchmarks in `bench/results.md` (all measured, raw logs `bench/*_results.log`).
Silver trigger 10 vs 30 vs 60 s (`make bench-trigger`, `SILVER_TRIGGER_INTERVAL`, ADR-034): no gain,
**kept 10 s** (batches ~29 s run back to back; MERGE ~12.5 s fixed; 60 s worse latency). Max sustainable
rate (`make bench-rate`): **~400/s**; above that bronze falls behind (one 4-core JVM is CPU-bound), and
the ceiling falls as silver grows (ADR-023). OPTIMIZE (`make bench-optimize`): bronze 2,462 → 5 files,
full scan 1.97 → 0.06 s; silver ZORDER undone by MERGEs within 3 min. The nightly optimize DAG now
also compacts bronze (pause ~15 min, mostly VACUUM). README final (Mermaid, failure modes, results,
quick start), week-by-week numbers moved to `docs/measurements.md`; CI in `.github/workflows/ci.yml`
(unit + DQ tests + dbt parse); `docs/demo-script.md`, blog/LinkedIn drafts; screenshots go in
`docs/img/`. Makefile: `BENCH_RATE` because the global `RATE ?= 200` used to override per-target
defaults (`make chaos` too). The laptop can sleep mid-run: check `date -u` against the logs.
**Week 5 (done):** gold cycle = `dbt/cycle.sh` (build → Soda DQ gate → publish, flock), run by the
30 s loop and by Airflow `dbt_run` (ADR-029). Soda lives in venv `/opt/soda` in the gold image
(ADR-030); freshness/volume only warn. Airflow (`make airflow-up`, :8088 admin/admin, profile
`orchestration`, LocalExecutor, DB `airflow` on the same Postgres) runs tasks via `docker exec`
over the Docker socket; DAGs dq_checks, dbt_run, optimize (nightly), backfill_silver (ADR-032).
Spark writer jobs go through `spark/exclusive.sh`; pausing streaming is recorded in an Airflow
Variable (ADR-033: incident with orphaned writers). Monitoring (`make monitoring-up`, Grafana
:3000 admin/lakehouse): `monitoring/exporter.py` + listener in the driver on :9109 (ADR-031).
kafka-ui is now profile `ui` (`make kafka-ui`). `make test` = 42. Whole stack under load: 7.1 of
9.7 GiB. Drills: `make bad-batch` / `bad-batch-fix`, `make silver-damage` + `make backfill`.

**Week 4 (done):** gold = dbt-duckdb (not dbt-spark, ADR-024) reading silver via the delta plugin,
`make gold-up` runs a 30 s build loop (publishes `/data/gold.duckdb` by atomic rename only when
all tests pass) + Streamlit dashboard on :8501 (not Metabase, ADR-024). SCD2 `users_snapshot` →
`dim_user` (ADR-025); refund rate = refunds issued / orders placed per minute (ADR-026). dbt: 29
nodes green, 19 tests. `make test` = 24. Streaming now 4 GB limit / 2200m heap (ADR-022), ~3.5
GiB used warm. **Open, waiting on Rohit:** every silver MERGE rewrites the whole table
(ADR-023, OPTIMIZE does not help); fix options listed there, nothing applied.

**Week 3 (done):** schema drift via autoMerge + per-schema-id decode, type changes quarantined as
`schema_incompatible` (ADR-018); idempotent `txnAppId`/`txnVersion` appends, bump `TXN_EPOCH` if
a checkpoint is deleted (ADR-019); orders-per-minute with a 10 min watermark, third query in the
same driver, `late_events` table (ADR-020); `make chaos` = 10 SIGKILLs, run twice, reconcile 0 /
0 / 0 / 0 both times (ADR-021, `chaos/run_*.log`). `make test` = 21 tests. Open issue: silver
batches take 19-26 s at 50/s, slower than Week 2 (cause not found, see ADR-021); the driver sits
at ~2.6 of 2.73 GiB. Do not use `bash -l` through `wsl -e` on this laptop: the profile runs
`sudo` and hangs, use `bash -c`.

**Week 1: capture.** `make up` starts postgres, kafka (KRaft), schema-registry, connect
(+Debezium 2.7, Avro), minio (+bucket `lakehouse`), spark (client container,
apache/spark:3.5.3-python3), kafka-ui. Connector `ecom-postgres` publishes
`ecom.public.{users,products,orders,order_items,payments}`, Avro values and keys, Confluent wire
format, keyed by PK, 3 partitions. `make simulate RATE=200` generates c/u/d on every table.
`make check` and `make spark-smoke` pass.

**Week 2: bronze and silver.** `make stream-up` starts one Spark driver (`streaming/run_all.py`)
running both queries, with `restart: unless-stopped`; `make stream-logs` and `make stream-down`
control it.
- **Bronze** (`streaming/bronze_ingest.py`): raw Kafka bytes plus `schema_id` into Delta
  `s3a://lakehouse/bronze/<table>`, 10 s trigger, append-only, tables written in parallel.
- **Silver** (`streaming/silver_merge.py`, `streaming/common/transforms.py`): decodes Confluent
  Avro per schema id (registry cached), flattens the Debezium envelope, dedups in the batch
  (highest LSN, then offset), and MERGEs into `s3a://lakehouse/silver/<table>` with **soft
  deletes guarded by the LSN** (ADR-012). Columns added: `_lsn, _op, _source_ts, _silver_ts,
  _is_deleted`. Current state = `WHERE NOT _is_deleted`. Tables are merged in parallel threads.
- **Quarantine**: `s3a://lakehouse/quarantine/<table>` with raw bytes, `reason`
  (`bad_wire_format`, `avro_decode_failed`, `schema_not_found`, `null_pk`, `null_row`) and batch
  id. Registry outages are NOT quarantined; the batch fails and retries (ADR-014).
- **`make reconcile [WAIT=300]`**: Postgres vs silver, per-table count, sums, mismatched keys
  and duplicate keys; exit 1 on any diff (`chaos/reconcile.py`, delta-rs, no JVM).
  **`make latency [MINUTES=5]`**: p50/p95/p99 of `_silver_ts - updated_at`.
- **`make test`** runs all 12 unit tests inside the spark container (no pytest on the host).
- **Results** (3 min at 200 changes/s, 36,000 changes): reconcile = 0 missing, 0 extra, 0
  mismatched, 0 duplicates on all 5 tables, repeated 4 times. Latency p50 24 s, p95 47 s, p99
  51 s after tuning (first run: p50 67 s). Silver batches take 10-12 s against a 10 s trigger, so
  200/s is close to this machine's limit (ADR-015). Remaining MERGE cost was not profiled.
- Known gaps for Week 3: bronze is at-least-once (a replayed batch can duplicate bronze rows;
  silver absorbs it via the LSN guard), no `txnAppId` yet, no schema-drift handling yet.

## Working rules
1. Work **one week at a time**. A week is done only when its "Done when" check passes and
   you have shown Rohit the real output.
2. **Test everything you write** by running it. Never claim a number you didn't measure.
   Put measured numbers in README "Measured so far" with how they were measured.
3. Add an ADR to `DECISIONS.md` for every real choice or problem you hit (what, why,
   rejected alternative, trade-off). This file is Rohit's interview script, so write it in
   first person and keep it plain.
4. Don't change the simulator, connector config or `postgres/init.sql` unless needed; if
   needed, say why first.
5. Add a `make` target for every runnable thing. Keep `make up` as the one-command start.
6. At the end of each week: update README (status, how to run, results), run `make test`,
   then `git add -A && git commit -m "Week N: <summary>"` and `git push` if a remote exists.
7. Finish each week with a short summary for Rohit: what was built, the proof output,
   problems and fixes, and 3 interview questions this week prepares him for, with answers.
   He is a backend engineer learning DE, so explain new concepts in plain words.

---

## Week 2 — Bronze and silver (the correctness core) — DONE

**Spark packages** (Spark 3.5.3 / Scala 2.12):
`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3, org.apache.spark:spark-avro_2.12:3.5.3,
io.delta:delta-spark_2.12:3.2.1, org.apache.hadoop:hadoop-aws:3.3.4`. S3A settings as in
`spark/smoke_delta.py`. Put shared config in `streaming/common/spark_session.py`.

**Bronze (`streaming/bronze_ingest.py`)**: one streaming query, `subscribePattern
ecom\.public\..*`, `startingOffsets=earliest`, trigger 10 s, `maxOffsetsPerTrigger` set.
Store bytes unchanged so bronze survives any schema change: `topic, partition, offset,
kafka_timestamp, key (binary), value (binary), schema_id` (bytes 2–5 of value, big-endian int),
`ingest_ts`. Write to Delta `s3a://lakehouse/bronze/<table>` (partition by table or one table
per topic via foreachBatch). Append-only. Checkpoint under `s3a://lakehouse/_checkpoints/`.

**Silver (`streaming/silver_merge.py`)**: streams from bronze Delta, `foreachBatch` per table:
1. Decode Confluent Avro: strip the 5-byte header (`substring(value, 6, length(value)-5)`),
   and for each distinct `schema_id` in the batch fetch the writer schema from Schema Registry
   (`GET /schemas/ids/{id}`, cache it) and `from_avro` it. Group by schema_id, which is needed
   for schema drift in Week 3.
2. Flatten the Debezium envelope: `op`, `source.lsn`, `ts_ms`, and `after.*` (or `before.*`
   for deletes). Convert Debezium types: `io.debezium.time.ZonedTimestamp` (string) to
   timestamp, decimals stay `DecimalType(12,2)`. Snapshot `op='r'` is an upsert.
3. Dedup inside the batch: `row_number() over (partition by <pk> order by lsn desc,
   kafka offset desc) = 1`. PKs: users.user_id, products.product_id, orders.order_id,
   payments.payment_id, order_items.(order_id, product_id).
4. MERGE into `s3a://lakehouse/silver/<table>` using **soft delete** plus an LSN guard:
   `WHEN MATCHED AND s.lsn > t._lsn AND s.op='d' THEN UPDATE SET _is_deleted=true, _lsn=s.lsn`
   `WHEN MATCHED AND s.lsn > t._lsn THEN UPDATE SET * , _is_deleted=false`
   `WHEN NOT MATCHED THEN INSERT (with _is_deleted = (s.op='d'))`.
   Soft delete is deliberate: with a hard delete, replaying an old insert after a delete would
   bring the row back. Keeping a tombstone with its LSN makes replays harmless. Write this
   up as an ADR.
5. Records that fail decoding or have a null PK go to `s3a://lakehouse/quarantine/<table>`
   with raw payload, reason and batch id. They never stop the stream.
6. Add metadata columns `_lsn, _op, _source_ts, _silver_ts, _is_deleted`. Expose current
   state as `silver_<table>_current` (filter `_is_deleted = false`), e.g. a DuckDB/Spark view
   or documented query.

**Run model**: add compose services `bronze` and `silver` (spark image, `spark-submit`,
profile `stream`, `restart: unless-stopped`) and targets `make stream-up`, `make stream-logs`,
`make stream-down`. If memory doesn't allow two JVMs, run both queries in one driver
(`streaming/run_all.py`) and record the decision.

**`chaos/reconcile.py`**: for each table compare Postgres vs silver current state:
`count(*)`, and for orders/payments `sum(total)` / `sum(amount)` and the count of mismatched
PKs (full outer join on PK, compare status/total/updated_at). Print a table, exit non-zero on
any difference. Add `make reconcile`.

**Unit tests (`tests/`, pytest + local SparkSession)**: dedup keeps the highest LSN; MERGE on
insert → update → delete → re-insert for one key; replaying an older batch changes nothing;
null PK goes to quarantine.

**Done when**: simulator runs at 200/s for 3 min, you stop it, wait for silver to catch up,
and `make reconcile` shows 0 difference on all 5 tables. Also measure end-to-end latency
(Postgres `updated_at` vs `_silver_ts`, p50/p95) and report it.

---

## Week 3 — Schema drift, late events, exactly-once under crashes
- **Schema drift**: `make drift` runs `ALTER TABLE orders ADD COLUMN coupon_code TEXT` while the
  simulator runs, and the simulator fills it sometimes (small, documented change). Silver picks
  up the new column via `mergeSchema` / `spark.databricks.delta.schema.autoMerge.enabled`
  within one micro-batch. A breaking change (e.g. type change) is rejected by the
  registry or quarantined with reason `schema_incompatible`. Measure the time from ALTER to
  the column appearing in silver.
- **Late events**: in the windowed aggregate (orders per minute) use `withWatermark(event_time,
  '10 minutes')`; events past the watermark go to `late_events`. Measure the
  event-vs-processing delay p99 from bronze and justify the watermark.
- **Exactly-once**: idempotent writes in foreachBatch (`txnAppId`/`txnVersion` where supported,
  plus the LSN guard). `chaos/kill_random.sh` kills spark/connect/kafka containers 10 times at
  random intervals during a simulator run; then `make reconcile` must show 0 difference.
  `make chaos` runs the whole thing.
- **Done when**: 10 random kills, reconcile = 0 missing, 0 duplicate rows. Put the result in
  README.

## Week 4 — Gold with dbt + live dashboard
- dbt project in `dbt/`. Preferred: `dbt-spark` with `method: session` inside the spark
  container, so gold stays Delta on MinIO. If memory or complexity is too high, use
  `dbt-duckdb` reading silver Delta via the `delta` extension, and record the trade-off.
- Models: `fact_orders` (one row per order, joined to payments, revenue net of refunds),
  `dim_user` as an SCD Type 2 **dbt snapshot** on silver users (`strategy='timestamp'`,
  `updated_at`), `agg_revenue_minute` (minute × city, joined to dim_user on order time between
  valid_from and valid_to). Use the city at order time, not the current city.
- dbt tests: unique, not_null, relationships, accepted_values on status, and exactly one
  `is_current` row per user.
- Serving: DuckDB (delta + httpfs extensions) as the query engine. Dashboard: Metabase with
  the DuckDB driver, or Superset. If neither runs within memory, use a Streamlit app and record
  why. Panels: revenue/min, orders/min, refund rate, revenue by city, pipeline health
  (Kafka lag, quarantine count).
- **Done when**: the dashboard shows revenue/min and refund rate updating while the simulator
  runs.

## Week 5 — Orchestration, data quality, monitoring
- Airflow in the lightest workable form (single container, `airflow standalone` or
  LocalExecutor) under profile `orchestration`. DAGs: `dbt_run` (every 5 min), `dq_checks`
  (before gold publish), `optimize` (nightly `OPTIMIZE ... ZORDER BY` on silver and gold, plus
  `VACUUM` with 7-day retention), `backfill_silver` (params start_date, end_date, table; rebuilds
  silver from bronze with the same transform as a batch job using `trigger(availableNow=True)`).
- Data quality (Great Expectations or Soda, pick one and record why): not-null keys, order_id
  unique in silver, total >= 0, status in the allowed set, every order has a user, freshness
  (max updated_at within 5 min), volume within ±3σ of the trailing same hour. Record-level
  failures go to quarantine. Table-level failures alert (a Slack webhook from env var
  `SLACK_WEBHOOK_URL`, or log only if unset) and **skip the gold build**.
- Monitoring: Prometheus + Grafana (profile `monitoring`). Panels: Kafka consumer lag (kafka
  exporter), Spark batch duration and rows/sec (StreamingQueryListener pushing metrics, or the
  Spark Prometheus servlet), quarantine count, replication slot lag.
- **Done when**: a deliberately bad batch (e.g. negative totals injected via `make bad-batch`)
  is quarantined and alerts, and gold is not updated.

## Week 6 — Benchmarks, docs, demo
- `bench/`: small-file problem. Measure file count, average file size and a fixed benchmark
  query's time before and after `OPTIMIZE`/`ZORDER`, then publish the real ratio in
  `bench/results.md`. Throughput: highest simulator rate where Kafka lag stays flat. Latency
  p50/p95.
- Silver trigger (deferred from Week 4, ADR-023/028; do NOT tune silver before this): measure a
  30 s and a 60 s silver trigger (`TRIGGER_INTERVAL`) against the current 10 s, before/after, same
  workload (`make bench-silver`): batch time, MERGE `executionTimeMs`, Kafka lag trend, and
  latency p50/p95. Keep the change only if the numbers justify it; record it as an ADR.
- README: architecture diagram (Mermaid) at the top, a "Failure modes handled" section
  covering all 8 hard problems with how each is proven, a benchmark table, and a quick start a
  stranger can run in under 15 minutes (test this from a fresh clone with `make clean`).
- GitHub Actions CI: unit tests + `dbt parse`; optionally a compose integration test.
- Help Rohit record a 2-minute demo (simulator → dashboard, schema change, crash and
  recovery) and draft a blog post and LinkedIn post from DECISIONS.md. Terraform cloud variant
  is optional; only do it if Rohit asks.
