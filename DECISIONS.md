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

## ADR-018: Schema drift: decode per schema id, let the MERGE add columns, quarantine type changes
**Decision.** Silver already decoded each distinct schema id with its own writer schema and
unioned the results by column name. For Week 3 I set
`spark.databricks.delta.schema.autoMerge.enabled=true`, so a MERGE whose source has a column the
silver table lacks adds it (`UPDATE SET *` / `INSERT *`). Old rows get NULL. A column silver has
but the batch lacks (an old-schema batch replayed later) is left unchanged. A change to the TYPE
of an existing column is not merged: all rows of that schema id go to quarantine with reason
`schema_incompatible` (rules in `incompatible_columns()`: int to long and a decimal that fits are
fine, long to string is not). The verdict is cached per schema id.
**Why.** The Schema Registry is set to BACKWARD compatibility, so an additive change (new
optional column with a default) is accepted and a type change is not. I checked both against the
live registry: adding an optional field to `order_items` is `compatible=True`, `qty int to string`
is `compatible=False`. So the registry is the first gate. I did not break the live connector to
see what Debezium does when registration is refused (I expect the task to fail, which is loud,
but that is an expectation, not something I ran). The quarantine rule is the second gate for
changes the registry lets through, such as a decimal that got wider, and it is covered by unit
tests (`test_breaking_type_change_is_quarantined_as_schema_incompatible`), not by a live drill.
It keeps one bad schema from failing the MERGE for the whole batch.
**Measured** (`make drift`, `ALTER TABLE ... ADD COLUMN`, probe row updated right after):
- Debezium registered the new schema version 1.8 s after the ALTER (orders) and 2.8 s (products).
- Idle system: the column was in the silver Delta schema **26.6 s** after the ALTER
  (`products.note`, nothing else running). That is one bronze trigger plus one silver batch.
- While the simulator ran at 100 changes/s: **73.6 s** (`orders.coupon_code`). Silver batches were
  taking 37-72 s then, so most of that is the backlog in front of the first batch with the new
  schema, not the schema handling. I did not run it at 200/s.
- Reconcile after each drill: 0 difference, and reconcile now compares every column Postgres has,
  so a column silver never received would show up as a mismatch.
**Rejected.** `mergeSchema` on a write path (our silver write is a MERGE, so autoMerge is the
setting that applies), and failing the stream on any schema change (drift is normal, a stalled
pipeline is not).
**Trade-off.** autoMerge is table-wide: it would also accept a column added by mistake. A dropped
source column stays in silver as an all-NULL column forever (I dropped `products.note` after the
drill to confirm silver keeps working). The type check is by name and type only; it does not know
that a rename is a rename.

## ADR-019: Idempotent writes with `txnAppId` / `txnVersion`
**Decision.** Every append made from a `foreachBatch` (bronze tables, quarantine, order events,
late events) uses `txnAppId = <name>-<TXN_EPOCH>` and `txnVersion = batch id`. Delta records the
pair in the commit and silently skips a later write with the same app id and a version it has
already seen. The silver MERGE does not need it: the LSN guard already makes a replay a no-op.
**Why.** Spark replays a micro-batch with the same batch id and the same Kafka offsets after a
crash. Bronze writes five tables in one batch, so a crash after the `users` commit and before the
`orders` commit replays all five, and a plain append would put the `users` rows in twice. That
was the known Week 2 gap. `test_bronze_batch_replayed_after_a_partial_commit_is_not_duplicated`
reproduces exactly that sequence and has a control showing the old plain append does duplicate.
Live, `bronze-1` shows up as a `txn` action in the bronze Delta log.
**What I can and cannot claim.** After the chaos runs `make bronze-check` shows 0 repeated
(topic, partition, offset) in bronze. I cannot show that any of those kills landed in the narrow
window between two commits, so the live runs are consistent with the fix but the unit test is the
proof that it works.
**Rejected.** De-duplicating bronze on read (it hides the problem and every reader has to
remember), and MERGE-ing bronze on (topic, partition, offset) (a MERGE per table per 10 s trigger
for a table that is append-only by design).
**Trade-off.** The version is the batch id, and batch ids restart from 0 if a checkpoint is
deleted. Delta would then skip "old" versions and quietly lose data. So deleting a checkpoint
means bumping `TXN_EPOCH` (an env var that is part of the app id) or wiping the table too.
Debezium re-delivering an event after a Connect crash is a different thing: it is a new Kafka
offset, so bronze stores it twice by design and silver's LSN guard absorbs it.

## ADR-020: Orders per minute with a 10 minute watermark; late events go to a table
**Decision.** Event time is `orders.created_at`. Silver's orders batch hands its decoded rows to
`process_order_events()`, which keeps the `c` events (snapshot `r` rows are not events), drops
re-deliveries, and appends them to `events/orders_created`. A separate Spark stateful query
(`orders_per_minute`) reads that table with `withWatermark(event_time, '10 minutes')`, counts per
1-minute window in update mode, and upserts into `agg/orders_per_minute`. An event older than
`max(event time of earlier batches) - 10 min` is written to `late_events/orders` instead and
never reaches the aggregate.
**Why the split is done by hand.** Spark's watermark silently drops late rows from an aggregate;
there is no side output. So I classify late events before Spark sees them, with the same
definition, and keep `withWatermark` for what it is good at: bounding state. The watermark value
is rebuilt after a restart from the events table (`batch_id < current`), so replaying batch N
gives the same answer as the first attempt. Proven live with `make late-demo`: a fresh order
shows up in `events/orders_created`, then an order created 30 minutes ago lands in
`late_events/orders` (watermark 07:04:29 vs event 06:44:43) and `orders_per_minute` for its minute
does not change. After the chaos run `sum(orders_per_minute.orders)` = 9,920 = rows in
`events/orders_created` = 9,920 distinct order ids.
**Why 10 minutes (`make late-stats`).** Two different things are measured. (1) Delay = bronze
`ingest_ts` - `created_at`. With no faults at 50 changes/s: p50 7.1 s, p99 22.6 s, max 24.5 s
(1,548 orders). At 100 changes/s with no kills: p50 12.0 s, p99 30.2 s. During the first chaos run
(10 kills, and a pipeline that cannot keep up with 100/s): p50 220 s, p99 446 s, max 451 s. (2)
Disorder = how far behind the newest event already processed a new event is. This is what a
watermark must cover: the maximum was 0.0 s (100/s, no kills), 2.1 s (50/s, no kills), 0.8 s and
2.4 s (the two chaos runs). So 10 minutes is about 20x the fault-free
p99 delay and above the worst delay I saw under chaos, and it is vastly above the real disorder.
Nothing real went late in any run. The only late events are the ones I injected.
**Rejected.** A 1-2 minute watermark (it would be enough fault-free but the chaos run shows
backlog delays of 7 minutes), a 1 hour watermark (more state and a later cut-off for no benefit
here, though in update mode it would not delay results), and using Kafka's timestamp as event time
(that is when Connect produced the record, so a Connect outage would not look late at all).
**Cost.** The side output runs in its own thread next to the orders MERGE. From the batch logs
(27 batches) the orders thread finishes 2.6 s after the slowest other table on average (max
10.8 s). That includes orders simply having more rows, so it is an upper bound on the cost.
**Trade-off.** A `max(event time)` watermark can be pushed forward by one event with a bad
future timestamp, after which real events look late. Here `created_at` comes from Postgres
`now()`, so I accepted that. Late events are routed, not applied: correcting the minute they
belong to is a batch job (the Week 5 backfill). A re-delivered event that is itself late is not
reported twice (checked against `late_events`), and a duplicate older than the watermark whose
first copy was on time is dropped by the `events` check.

## ADR-021: Chaos test: SIGKILL the processing and transport layers, then reconcile
**Decision.** `make chaos` starts the simulator at 100 changes/s, then `chaos/kill_random.sh`
does `docker kill` (SIGKILL, no graceful shutdown) on a random one of `streaming`, `connect`,
`kafka`, 10 times, 20-60 s apart, restarting each after 3-12 s. A watchdog restarts a FAILED
connector task through the Connect REST API (Connect does not do that itself) and counts how often
it had to. At the end the simulator is stopped, `make reconcile WAIT=900` waits for silver to
catch up, and `make bronze-check` looks for repeated Kafka offsets in bronze. Postgres and MinIO
are not killed: Postgres is the source of truth we compare against.
**Result (both runs: reconcile 0 missing, 0 extra, 0 mismatched, 0 duplicate keys on all 5
tables, `sum(total)` and `sum(amount)` equal to the cent, 0 repeated bronze offsets):**
- Run 1, random draw: connect x5, streaming x4, kafka x1. 31,737 orders in Postgres at the end.
  Silver needed 7 retry rounds (about 15 s each) after the simulator stopped.
- Run 2, Kafka-weighted mix (`TARGETS="kafka kafka connect streaming"`): kafka x8, streaming x2.
  40,104 orders at the end. 19 retry rounds.
- The watchdog restarted a failed connector task 0 times in both runs. Connect and Debezium came
  back on their own after every kill, including 8 SIGKILLs of the only Kafka broker.
Logs: `chaos/run_1.log`, `chaos/run_2.log`.
**Why these kills.** SIGKILL is the realistic failure (OOM killer, host crash). A graceful
`docker stop` lets Spark and Connect flush and says nothing about recovery. Killing Kafka tests
Debezium's at-least-once behaviour (re-delivered events with new offsets, absorbed by the LSN
guard); killing the Spark driver tests the checkpoint, the replayed batch and the idempotent
writes; killing Connect tests the slot and offset resume.
**Not covered, so I am not claiming it:** killing Postgres or MinIO; two failures inside one
recovery window (gaps were at least 20 s); a kill during a schema change; deleting a checkpoint;
and any proof that a kill landed between two bronze commits (see ADR-019). I did not check which
batch each kill interrupted. Ten kills per run is a smoke test, not a soak test.
**What it also showed.** At 100 changes/s this laptop cannot keep up: bronze-to-event delay
reached about 450 s and the backlog needed 1.5-5 minutes to drain after the simulator stopped.
So the chaos runs recovered under backlog, which is harder than recovering when idle, but the
latency numbers from those runs are not steady state. Silver batches take 19-26 s at 50 changes/s
and 20-70 s at 100/s against the 10 s trigger. In Week 2 they were 10-12 s at 200/s. I have not
found why: batches 15-22 of this session ran on the Week 2 code (before any Week 3 change) and
already took 20-43 s, so it is not simply the new code. My unverified guess is table growth: the
silver tables now hold 25-40k rows in many small files with no OPTIMIZE yet, and a MERGE rewrites
files all over the table. That needs profiling before I claim a throughput number (Week 6).
**A bug the unit tests caught before deploy.** My first version of the new code dropped bronze's
`ingest_ts` before the flatten step, which would have broken every silver batch. The tests
failed on it. I fixed it by carrying `ingest_ts` through (`PASSTHROUGH_COLS`).
**Rejected.** Killing from inside the containers (needs tooling in the images), and a fixed kill
schedule (a random one is less likely to line up with the 10 s trigger by accident).
