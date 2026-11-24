# Building a CDC lakehouse that has to match Postgres to the cent

*Draft. Every number is from the project README; the reasoning is from DECISIONS.md.*

I'm a backend engineer learning data engineering. Instead of another tutorial pipeline I gave
myself one hard rule: after crashes, schema changes and late events, the lakehouse must match
the source database **exactly**: same rows, same sums, no duplicates. Then I spent six weeks
trying to break it on a laptop.

## What it is

A simulator writes orders, payments, refunds and user moves into Postgres. Debezium reads every
change from Postgres' write-ahead log and publishes it to Kafka as Avro. Spark Structured
Streaming lands the raw bytes in a Delta "bronze" table, then decodes, deduplicates and MERGEs
them into "silver" on MinIO (S3). dbt builds the gold tables in DuckDB, a Soda data quality
gate decides whether they get published, and a Streamlit dashboard shows revenue and refund
rate per minute. Airflow runs the maintenance jobs; Prometheus and Grafana watch all of it.

Everything runs with `docker compose` on a Windows laptop with WSL2 and about 10 GB of RAM.
That limit shaped more decisions than any architecture diagram did.

## Three ideas that made it correct

**1. Order by the database's own clock.** Every change carries its Postgres LSN, the position in
the write-ahead log. The silver MERGE only applies a change if its LSN is newer than the row's
current LSN. A retried batch, a replayed Kafka partition or a full rebuild from bronze then
changes nothing it shouldn't. I rejected ordering by `updated_at`: two updates in one
transaction share a timestamp, and a delete has no new timestamp at all.

**2. Deletes are tombstones, not deletes.** If you really delete a row, a replay of its old
insert brings it back from the dead, because nothing remembers it was deleted. So a delete sets
`_is_deleted = true` and keeps the LSN. "Current state" is a filter. It costs a little storage
and buys replay safety everywhere.

**3. Appends need to be idempotent too.** The MERGE was safe, but bronze is an append of five
tables in one micro-batch. A crash after the first table's commit and before the last one means
Spark replays the whole batch, and a plain append would write the first table twice. Delta's
`txnAppId`/`txnVersion` lets a writer say "this is batch 812 of app X", and Delta skips it if it
has seen it before.

## Proving it, not claiming it

`make reconcile` compares every table in Postgres with silver: row counts, sums of money, every
key and every column. It exits non-zero on any difference.

Then I tried to break it. `make chaos` runs the simulator at 100 changes per second and
`docker kill`s (SIGKILL, no graceful shutdown) a random one of Spark, Kafka Connect and Kafka
ten times. Two full runs, one Kafka-heavy: **0 missing, 0 extra, 0 mismatched, 0 duplicate
keys** on all five tables both times, and zero repeated Kafka offsets in bronze.

Other drills, same rule (if I didn't measure it, it isn't in the README):
- A new column added to the live `orders` table appeared in silver **26.6 s** later on an idle
  system and **73.6 s** under load, with no restart. A type change goes to quarantine instead of
  failing the stream.
- An order created 30 minutes late lands in a `late_events` table instead of silently changing a
  minute that was already reported. The real disorder I measured was at most 2.4 s, so a 10
  minute watermark is generous on purpose.
- I deleted 1,305 rows from silver on purpose; a backfill replayed 183,110 bronze rows through
  the stream's own code in **47.5 s** and reconcile was back to zero.

## A quality gate that refuses to publish

Correct replication is not the same as correct data. Before every gold publish, 14 Soda checks
run on exactly the tables about to be published: keys not null and unique, totals not
negative, status in the allowed set, every order has a user. A failure keeps the last good gold
online and turns the dashboard banner red.

I tested it the honest way: five orders with negative totals inserted directly into Postgres.
They were caught and quarantined with their LSNs, an alert fired, and gold stayed frozen at the
last good build, **121 s** after the insert. After fixing the rows at the source, the next cycle
published **66 s** later.

One choice I'd defend in an interview: freshness and volume checks only *warn*. Blocking a late
gold build makes it later, not more correct.

## Where I was wrong (twice)

Silver got slower every week. I guessed "small files", ran OPTIMIZE, and Delta's own MERGE timer
did not improve. The real cause was that every MERGE rewrote the whole table: one orders batch
with 315 changed rows copied all 43,432 unchanged ones.

So I tried deletion vectors, which avoid exactly that copy. The copy went to zero, the MERGE got
*slower*, and the non-Spark readers (delta-rs, used by dbt and reconcile) couldn't read the
table at all. The tables are only megabytes, so the cost is fixed overhead per MERGE, not
rewriting. I wrote both experiments up as decision records instead of quietly reverting them.

In the last week I tested the obvious "cheap" fix: if each MERGE costs the same, make them less
often. I ran a 10 s, 30 s and 60 s silver trigger under the same load, twice for the first two.
10 s and 30 s turned out to be the same thing: a batch takes about 29 s, so Spark already runs
them back to back. 60 s doubled the rows per MERGE at the same 12.5 s per MERGE, which proved the
fixed-cost theory, and made latency worse because silver then sat idle half the time. I kept
10 s. Measuring the change I *didn't* make was the most useful benchmark of the project.

The load test found the real ceiling somewhere else: about **400 changes per second**. Above that,
it's bronze, not silver, that falls behind. All the queries share the 4 cores of one Spark
driver. And the small-file problem I had wrongly blamed silver for in week 3 was real, just in
bronze: compacting it took 2,462 files down to 5 and a full scan from 1.97 s to 0.06 s.

## The incident

The first time Airflow started, it ran a missed nightly OPTIMIZE immediately. Under that load its
web server timed out, my start script restarted the whole container, and the task died. The
Spark job it had started with `docker exec` did not. The retry started another one, and the next
restart another. I ended up with several OPTIMIZE jobs writing silver at once, and streaming
left stopped because every retry thought "it wasn't running, don't resume it". Reconcile showed no damage, but only because OPTIMIZE rewrites files without changing
rows. Fixes: a lock that stops any leftover writer before a new one starts, a "streaming paused
by me" marker that survives a crash, and a web server that no longer takes the scheduler down
with it. Then I restarted Airflow mid-job on purpose to prove it.

The lesson: whatever an orchestrator starts outside its own process tree, it must be able to
find and stop again.

## What I'd do differently

- Measure the baseline before the first optimisation, not after the second.
- Budget memory per container from day one. The Spark driver uses 3.5 GiB of its 3.9 GiB limit,
  and that margin decided several designs.
- Write the reconcile tool in week one. Every later claim leaned on it.

The code, the decision records (written as my interview script) and every measurement are in
the repo.
