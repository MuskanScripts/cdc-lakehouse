# Real-time CDC Lakehouse

> Capture every row-level change from Postgres' write-ahead log, stream it through Kafka,
> MERGE it into a Delta lakehouse, enforce quality gates, and serve live revenue and refund
> numbers that match the source exactly, even after crashes, schema changes and late events.

**Status:** Week 3 of 6. Capture (Postgres → Debezium → Kafka), bronze and silver
(Kafka → Delta on MinIO, idempotent MERGE), schema drift handling, an event-time aggregate with a
late-event table, and a chaos test. `make reconcile` shows 0 difference against Postgres on all
5 tables after a 3-minute 200 changes/s run (Week 2) and after two runs with 10 random SIGKILLs
each (Week 3). Known limit: this laptop cannot keep up with 100 changes/s (see Measured so far).

```
 Simulator ──► Postgres 16 ──WAL──► Debezium ──► Kafka (KRaft) ──► Spark Structured Streaming
 (Python)      wal_level=logical    (Connect)    + Schema Registry        │
                                                   (Avro, BACKWARD)       ▼
                                                              Delta on MinIO: bronze → silver → gold
                                                                          │            (weeks 2–4)
                                                              dbt · Airflow · GE/Soda · Trino · Metabase
```

## Quick start

Needs Docker with about 6 GB RAM, `make` and `curl`.

```bash
make up                      # build + start everything, register the Debezium connector
make simulate RATE=200       # in a 2nd terminal: write orders/payments/users (Ctrl+C to stop)
make check                   # Week-1 acceptance: c/u/d events on every table's topic

make stream-up               # Week 2: start bronze + silver (one Spark driver, ~2.7 GB)
make reconcile WAIT=300      # Postgres vs silver; retries while silver catches up; exit 1 on any diff
make latency MINUTES=5       # end-to-end p50/p95/p99 over the last 5 minutes
make test                    # 21 unit tests (run inside the spark container)

# Week 3
make drift                   # ALTER TABLE orders ADD COLUMN coupon_code, time it until silver has it
make late-demo               # a 30-minute-old order must land in late_events, not in the aggregate
make late-stats MINUTES=60   # event delay + disorder percentiles (the basis for the watermark)
make chaos                   # simulator + 10 random SIGKILLs of streaming/connect/kafka, then reconcile
make bronze-check            # every Kafka offset exactly once in bronze?
```

`make stream-up` is separate from `make up` because the Spark driver is the biggest memory
consumer. On a ~7.5 GB WSL, stop `kafka-ui` first if memory gets tight.
WSL shuts down when idle and takes the containers with it; run `make up && make stream-up`
and everything resumes from the checkpoints.

### Reading silver

Silver keeps soft-deleted rows (tombstones), so "current state" is a filter:

```sql
SELECT * FROM delta.`s3a://lakehouse/silver/orders` WHERE NOT _is_deleted
```

Silver columns = source columns + `_lsn, _op, _source_ts, _silver_ts, _is_deleted`.
Records that can't be decoded or have a null key land in `s3a://lakehouse/quarantine/<table>`
with the raw bytes, a reason and the batch id.

Event-time tables (Week 3): `events/orders_created` (one row per order, on time),
`late_events/orders` (older than the watermark), `agg/orders_per_minute` (Spark stateful
aggregate, `withWatermark(event_time, '10 minutes')`).

| UI | URL | Login |
|---|---|---|
| Kafka UI (topics, schemas, connector) | http://localhost:8080 | – |
| MinIO console | http://localhost:9001 | minio / minio12345 |
| Kafka Connect REST | http://localhost:8083/connectors/ecom-postgres/status | – |
| Schema Registry | http://localhost:8081/subjects | – |

Other useful targets: `make consume TABLE=payments`, `make status`, `make slot-lag`,
`make spark-smoke`, `make simulate-fast`, `make psql`, `make clean`. Run `make help` for the full list.

## What the simulator does

Each business action is one Postgres transaction, so the CDC stream looks like a real app's.
Every table gets inserts, updates and deletes:

| Table | Insert | Update | Delete |
|---|---|---|---|
| users | signup | city move (feeds SCD2 later), tier change | inactive account removed |
| products | new product | price change | discontinued, never sold |
| orders | checkout | paid → shipped → delivered / cancelled / refunded; cart change | cancelled order purged |
| order_items | with order | cart quantity change | cascade from purge |
| payments | pending with order | success / failed / refunded; amount re-total | cascade from purge |

`--rate` throttles on row changes per second, which is what Debezium sees.
`--fast` sets `synchronous_commit=off` for the simulator session. Without it, one connection
tops out at about 4k changes/s.

## Repo layout

```
docker-compose.yml       full local stack
connect/                 Connect image (Confluent + Debezium plugin), connector config
postgres/init.sql        source schema, REPLICA IDENTITY FULL, publication, signal table
simulator/               Faker-based OLTP load generator
scripts/                 connector registration, Week-1 event check
spark/                   Delta-on-MinIO smoke test
streaming/               bronze_ingest.py, silver_merge.py, run_all.py, common/ (decode, MERGE, registry, late_events)
chaos/                   reconcile.py, latency.py, drift.py, late_demo.py, event_delay.py, bronze_check.py,
                         kill_random.sh, run_chaos.sh, run_1.log / run_2.log (the recorded chaos runs)
tests/                   simulator tests + streaming tests (decode, dedup, MERGE, replay, quarantine,
                         schema drift, late events, idempotent writes)
dbt/  airflow/  quality/ gold marts, DAGs, DQ suites                 (weeks 4–5)
monitoring/ bench/       Prometheus+Grafana, benchmarks                (weeks 5–6)
DECISIONS.md             architecture decision records
```

## Measured so far

| Metric | Result | How |
|---|---|---|
| Simulator throughput (source side) | 5,000 changes/s sustained with `--fast`; ~3,950/s without | 10 s run, Postgres 16, single connection |
| WAL change counts vs simulator counts | exact match on all 5 tables | `test_decoding` slot, 20 s run at 400/s |

| Silver vs Postgres after a 3 min run at 200 changes/s (36,000 changes) | **0 missing, 0 extra, 0 mismatched, 0 duplicate keys** on all 5 tables; `sum(orders.total)` and `sum(payments.amount)` identical to the cent. Silver finished catching up within ~35 s of the simulator stopping | `make reconcile`; repeated 4 times, same result each time |
| End-to-end latency, Postgres `updated_at` → silver `_silver_ts`, steady 200 changes/s | **p50 24 s, p95 47 s, p99 51 s** (18,868 rows) | `make latency MINUTES=5`, Spark `local[4]`, 7.5 GB WSL, 10 s triggers |
| Same measurement before tuning | p50 67 s, p95 106 s | first run: tables merged one after another, Delta default `snapshotPartitions=50` |
| Streaming driver memory | 2.6 GiB of a 2.73 GiB limit, no OOM kills | `docker stats`, `local[4]`, `--driver-memory 1500m` |

How the tuning went (each row is one measured change, same 3-minute workload):
sequential per-table MERGE 67 s p50 → tables merged in parallel threads 51 s → bronze also
parallel plus `local[4]` 31 s → `spark.databricks.delta.snapshotPartitions=2` 24 s.

Latency caveat: silver keeps only the latest version of each key, so every row is measured on
its last change, and the figure includes up to one bronze trigger plus one silver trigger of
queueing (10 s each). At 200 changes/s silver batches take 10–12 s, which is right at the
trigger interval, so this is close to this machine's ceiling and not a comfortable margin.
Post-crash correctness is Week 3, below.

### Week 3

| What | Result | How |
|---|---|---|
| **Chaos, run 1**: 10 random SIGKILLs (connect x5, streaming x4, kafka x1) at 100 changes/s | **0 missing, 0 extra, 0 mismatched, 0 duplicate keys** on all 5 tables (31,737 orders; `sum(total)` and `sum(amount)` equal to the cent); 0 repeated Kafka offsets in bronze | `make chaos`, log in `chaos/run_1.log` |
| **Chaos, run 2**: 10 SIGKILLs, Kafka-weighted (kafka x8, streaming x2) | same: **0 / 0 / 0 / 0** on all 5 tables (40,104 orders), 0 repeated bronze offsets | `TARGETS="kafka kafka connect streaming" make chaos`, `chaos/run_2.log` |
| Connector task failures needing a restart during chaos | 0 in both runs | watchdog in `kill_random.sh` |
| Schema drift, idle system | Debezium registered the new schema 2.8 s after `ALTER`; column in silver at **26.6 s** | `make drift TBL=products COL=note`, nothing else running |
| Schema drift, simulator at 100 changes/s | new schema version at 1.8 s; column in silver at **73.6 s** (silver batches were taking 37-72 s, so this is mostly queueing) | `make drift` during `make simulate RATE=100` |
| Schema registry verdict (BACKWARD) | adding an optional column: compatible; `qty` int to string: **not** compatible | printed by `make drift` against the live registry |
| Late event path | order created 30 min ago: in `late_events`, not in `events/orders_created`, `orders_per_minute` for its minute unchanged | `make late-demo` |
| Aggregate vs events | sum of `orders_per_minute` = 9,920 = rows in `events/orders_created` = 9,920 distinct order ids | after chaos run 1 |
| Order event delay (bronze `ingest_ts` - `created_at`), no faults, 50 changes/s | p50 7.1 s, p99 22.6 s, max 24.5 s (1,548 orders) | `make late-stats`, 4 min window |
| Same, no faults, 100 changes/s | p50 12.0 s, p99 30.2 s, max 31.6 s (2,780 orders) | window before chaos run 1 |
| Same, during chaos at 100 changes/s | p50 220-235 s, **p99 446-452 s, max 451-457 s** (7,139 and 6,955 orders) | the pipeline is overloaded at 100/s on top of the kills; windows are the chaos periods only |
| Out-of-order arrival ("disorder": how far behind the newest processed event a new one is) | max **0.0 s** (100/s, no kills), **2.1 s** (50/s, no kills), **0.8 s** and **2.4 s** (chaos runs 1 and 2) | `make late-stats` |
| Events later than the 10 min watermark | 0 real ones in any run (the only one is the injected demo order) | `make late-stats` |

Why a 10 minute watermark, in one line: the real disorder was at most 2.4 s (in all four windows) and the worst
processing delay under chaos was 7.6 min, so 10 minutes covers both with margin; if delay ever
exceeded it the event is routed to `late_events`, not lost (ADR-020).

Caveats I want to be upfront about:
- **Throughput regressed against Week 2 and I don't know why.** Silver batches take 19-26 s at
  50 changes/s and 20-70 s at 100/s; Week 2 recorded 10-12 s at 200/s. Batches that ran on the
  Week 2 code earlier in this session were already 20-43 s, so it is not simply the new code.
  My guess (not verified) is silver table growth with many small files and no `OPTIMIZE` yet.
  The new event side output makes the orders thread about 2.6 s slower per batch (ADR-020).
  So the Week 2 latency row above (p50 24 s at 200/s) was NOT reproduced today.
- Streaming driver memory: 2.59 GiB of the 2.73 GiB limit after the drift run (`docker stats`),
  now with three queries in the JVM. No OOM kill seen, but there is almost no headroom.
- 10 kills per run is a smoke test. Postgres and MinIO were not killed. I have no proof that a
  kill landed between two bronze commits; the idempotent write is proven by a unit test (ADR-019).
- The breaking-type-change quarantine (`schema_incompatible`) is covered by unit tests, not by a
  live drill: I did not break the live connector.

## Troubleshooting

- **Connector FAILED**: check `make logs`. The usual cause is Postgres not running with
  `wal_level=logical`. Confirm with `make psql` → `SHOW wal_level;`.
- **`make check` says `d` is missing**: deletes are about 2% of actions. Let the simulator run
  for a minute.
- **Topic shows `op: r`**: these are snapshot reads, Debezium's initial copy of rows that
  existed before streaming started. This is expected.
- **`make reconcile` says DIFF right after the simulator stops**: silver is still catching up.
  Use `make reconcile WAIT=300`. Check progress with `make stream-logs` (lines `silver batch=...`).
- **Streaming container exited**: `make stream-logs`. It restarts itself (`unless-stopped`) and
  resumes from its checkpoints. If WSL restarted, run `make up && make stream-up`.
- **A message in `quarantine/<table>`**: look at its `reason` (`bad_wire_format`,
  `avro_decode_failed`, `schema_not_found`, `schema_incompatible`, `null_pk`, `null_row`); the
  raw bytes are kept.
- **Deleted a streaming checkpoint?** Also set `TXN_EPOCH` to a new value (env of the `streaming`
  service) or wipe the Delta tables. Batch ids restart at 0 and the idempotent writes would
  skip them as "already done" (ADR-019).
- **`make drift` says the column already exists**: the drill only means something the first time;
  use `make drift COL=other_name` (or `TBL=products`).
- **Decimal fields look like `"\u0004Ò"`** in the console consumer: Avro `decimal` is
  encoded as bytes. Spark's `from_avro` decodes it to `DecimalType(12,2)`.
