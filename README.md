# Real-time CDC Lakehouse

> Capture every row-level change from Postgres' write-ahead log, stream it through Kafka,
> MERGE it into a Delta lakehouse, enforce quality gates, and serve live revenue and refund
> numbers that match the source exactly, even after crashes, schema changes and late events.

**Status:** Week 2 of 6. Capture (Postgres → Debezium → Kafka) plus bronze and silver
(Kafka → Delta on MinIO, idempotent MERGE) are running. `make reconcile` shows 0 difference
against Postgres on all 5 tables after a 3-minute, 200 changes/s run.

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
make test                    # unit tests (run inside the spark container)
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
streaming/               bronze_ingest.py, silver_merge.py, run_all.py, common/ (decode, MERGE, registry)
chaos/                   reconcile.py (Postgres vs silver), latency.py; kill script comes in week 3
tests/                   simulator tests + streaming tests (decode, dedup, MERGE, replay, quarantine)
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
Post-crash correctness comes in week 3.

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
  `avro_decode_failed`, `schema_not_found`, `null_pk`, `null_row`); the raw bytes are kept.
- **Decimal fields look like `"\u0004Ò"`** in the console consumer: Avro `decimal` is
  encoded as bytes. Spark's `from_avro` decodes it to `DecimalType(12,2)`.
