# Architecture Decision Records

Each record says what was chosen, why, and what was rejected. This file doubles as an
interview script, so it keeps the reasoning rather than a list of tools.

---

## ADR-001: Log-based CDC with Debezium, not query polling or triggers
**Context.** Analysts need source-exact numbers within seconds, deletes included.
**Decision.** Debezium reads the Postgres WAL through a logical replication slot (`pgoutput`).
**Why.** It captures hard deletes and every intermediate update, and adds almost no load on
the DB because it reads the log, not the tables.
**Rejected.** Polling (`WHERE updated_at > x`) misses hard deletes and rows updated twice
between polls, and it needs an index while still hammering the DB. Triggers add write latency
to every transaction.

## ADR-002: `REPLICA IDENTITY FULL` on source tables
**Decision.** Every table has `REPLICA IDENTITY FULL`.
**Why.** With the default setting, update and delete events carry only the primary key in
`before`. FULL gives the complete before-image, so bronze is a full audit history and deletes
can be quarantined or inspected with all their columns.
**Trade-off.** More WAL volume per update. In production I'd limit FULL to tables where the
before-image matters.

## ADR-003: Explicit publication, `publication.autocreate.mode=disabled`
**Why.** Auto-creating a publication for all tables requires superuser. Creating it in the
schema migration keeps the CDC user least-privileged and makes the captured table set
reviewable in code.

## ADR-004: Kafka in KRaft mode, CDC topics keyed by primary key
**Why Kafka.** It is durable, replayable by offset, and lets many consumers read without
coupling to the producer.
**Why keyed by PK.** Kafka orders messages only within a partition. Debezium keys each event
by primary key, so all changes to one order land on one partition in commit order. Ordering
across different orders doesn't matter.
**Why KRaft.** ZooKeeper was removed in Kafka 4.0, so KRaft is the only forward path, and it
means one fewer container.
**Rejected.** RabbitMQ and SQS delete messages once consumed, so there's no replay or
backfill. Going directly from DB to Spark leaves no buffer when Spark is down.

## ADR-005: Avro + Schema Registry with BACKWARD compatibility
**Why.** Schemas are versioned contracts. A producer can't ship a change that breaks
consumers, and additive columns flow through (this is tested in week 3 with `ALTER TABLE ADD
COLUMN` while the simulator runs). Avro messages are also much smaller than JSON with an
embedded schema.
**Rejected.** Plain JSON has no contract and larger messages, and drift is only discovered at
read time.

## ADR-006: Confluent Connect image + Debezium plugin, not the Debezium image
**Why.** Since 2.0, Debezium's container images no longer ship the Confluent Avro converter.
Starting from `cp-kafka-connect`, which has Avro built in, and adding the Debezium Postgres
plugin from Maven Central means only one jar set to manage.

## ADR-007: `decimal.handling.mode=precise` and `NUMERIC(12,2)` for money
**Why.** Money must never be a float, because reconciliation (`sum(total)` source vs silver)
must match to the paisa. With a fixed precision and scale, Debezium emits Avro `decimal(12,2)`,
which Spark reads as `DecimalType(12,2)`.
**Rejected.** `double`, because rounding drift breaks exact reconciliation. `string` works but
pushes casting into every consumer.

## ADR-008: Protect the source DB from a stuck slot
**Decision.** `max_slot_wal_keep_size=2GB`, a Debezium heartbeat every 10 s, and `make slot-lag`
to watch retained WAL.
**Why.** While Debezium is down, the replication slot keeps WAL so no change is lost, but disk
grows without limit. The cap protects production. If it's exceeded, the slot is invalidated
and Debezium must re-snapshot, which is an alertable event and not silent data loss.
Heartbeats advance the slot even when the captured tables are quiet.

## ADR-009: Signal table for incremental snapshots
**Why.** `public.debezium_signal` lets Debezium re-snapshot a table in PK chunks while
streaming continues, without locking the table. That's the answer to "how do you load an
existing 50 GB table?".

## ADR-010: The simulator models business actions as transactions
**Why.** Real CDC streams contain multi-row transactions: an order with its items and
payment, or a cascade delete. Generating isolated single-row writes would hide ordering and
transaction-boundary problems that silver has to handle. Rate is measured in row changes/s
because that's the unit Debezium and Kafka see.

---

## ADR-011: Bronze stores raw bytes, decoding happens in silver
**Decision.** Bronze keeps `topic, partition, offset, kafka_timestamp, key, value, schema_id,
ingest_ts` exactly as Kafka delivered it. Only the schema id (bytes 2-5 of the Confluent
header) is pulled out.
**Why.** If bronze decoded Avro, a bad message or a schema change would break the ingest job
and I could lose or stall data. With raw bytes, ingest cannot fail on content, and any bug in
silver is fixed by fixing the code and replaying from bronze.
**Rejected.** Decoding in bronze: faster to query, but it couples ingest to every schema change.
**Trade-off.** Bronze is not directly queryable. Bronze can also hold a duplicate if a batch is
replayed after a crash (at-least-once). The LSN guard in silver makes that harmless, and I
tighten the bronze write in week 3.

## ADR-012: Soft deletes with an LSN guard in the silver MERGE
**Decision.** A delete flips `_is_deleted = true` and keeps the row and its `_lsn`. Every
UPDATE needs `s._lsn > t._lsn`. Current state is `WHERE NOT _is_deleted`.
**Why.** With a hard delete, replaying an old insert after the delete would bring the row
back, because nothing remembers it was deleted. The tombstone and its LSN make any replay (a
retried batch, a backfill from bronze) a no-op. The LSN is Postgres' own commit-order
position, so it is the right ordering key, better than Kafka offsets or timestamps.
**Rejected.** Hard delete (resurrection bug). Ordering by `updated_at` (equal timestamps
inside one transaction, and a delete has no new `updated_at`).
**Trade-off.** Tombstones accumulate, so a later cleanup job has to purge old ones. Unit tests
cover insert, update, delete, re-insert, and replaying an older batch.

## ADR-013: Bronze and silver run in one Spark driver
**Decision.** `streaming/run_all.py` starts both streaming queries in one JVM (`local[4]`,
`--driver-memory 1500m`, container limit 2.8 GB) as one compose service with
`restart: unless-stopped`.
**Why.** WSL has about 7.5 GB, and Postgres, Kafka, Connect, Schema Registry and MinIO already
use around 2.5 GB. Two Spark JVMs would not fit. If either query fails, the process exits and
Docker restarts it from the checkpoints.
**Rejected.** Separate `bronze` and `silver` services: cleaner isolation, but about 1.5 GB more
memory. Silver also reads all five bronze tables as one unioned stream, so there is one
micro-batch and one checkpoint instead of five.
**Trade-off.** A crash in either query restarts both, and they compete for the same cores.

## ADR-014: Quarantine only what is permanently bad
**Decision.** Rows go to `quarantine/<table>` with raw bytes, reason and batch id for: wrong
wire format, undecodable Avro, unknown schema id (registry HTTP 404), null primary key, empty
row. A registry outage or timeout is not quarantined: the batch fails and Spark retries it.
**Why.** A poison message must never stop the stream, but a transient outage must never turn
good data into quarantine. Proven live: I produced a garbage message into
`ecom.public.orders`, it landed in quarantine as `bad_wire_format`, and the stream carried on
with 0 restarts.
**Rejected.** Failing the stream on bad data (one bad message blocks everything), and
quarantining on any exception (a registry blip would silently divert good rows).

## ADR-015: Silver latency tuning, measured one change at a time
**Problem.** First run: p50 67 s, p95 106 s at 200 changes/s, with silver batches of 40-60 s
against a 10 s trigger. Correctness was fine (reconcile 0 diff), but it was not "real-time".
**What I changed, in order, with p50 for the same 3-minute workload:**
1. Merge the 5 tables in parallel threads instead of one after another, with a unique temp
   view per table (a shared view name would be a race): 67 s to 51 s.
2. Write the 5 bronze tables in parallel and move `local[2]` to `local[4]`: 51 s to 31 s.
3. `spark.databricks.delta.snapshotPartitions=2` (default 50): 31 s to 24 s. Delta replays its
   log with that many tasks on every table access, which is pure overhead for small tables.
**Rejected.** Raising the trigger interval (it doesn't cut the per-batch fixed cost) and a
second Spark JVM (no memory).
**Trade-off.** Batches take 10-12 s against a 10 s trigger, so this machine is near its limit at
200/s. I did not profile below the Spark level. I don't know how much of the remaining ~8 s per
MERGE is S3A round trips and how much is Delta's two-pass MERGE. That is the first thing to look
at before claiming a higher rate.

## ADR-016: Reconcile with delta-rs, no second Spark JVM
**Decision.** `chaos/reconcile.py` and `latency.py` read silver with the `deltalake` Python
package and Postgres with psycopg, in a small container.
**Why.** A Spark job for the check would need another 1+ GB JVM next to the streaming driver.
It also verifies the table the way an outside reader would, through the Delta log alone.
**Rejected.** Spark SQL (memory). DuckDB's delta extension (extra download, and it can be
blocked on a corporate laptop).
**Trade-off.** It loads whole tables into Python. That is fine for tens of thousands of rows and
not for millions; at that size I would compare aggregates in the engine.

## ADR-017: Fixed `make check` (SIGPIPE under `pipefail`)
**Problem.** The Week-1 check marked every op as missing even though the counts showed c, u
and d.
**Cause.** `printf "$out" | grep -q` exits at the first match, `printf` then gets SIGPIPE, and
under `set -o pipefail` the pipeline reports failure.
**Fix.** `grep -q ... <<<"$out"`, no pipe. Only the check script changed.
