# LinkedIn post (draft)

*Numbers from the README only. About 220 words; trim the bullets if needed.*

---

I spent six weeks building a real-time CDC lakehouse on my laptop. The one rule: after crashes,
schema changes and late events, it has to match the source database exactly.

Postgres → Debezium → Kafka → Spark → Delta Lake on MinIO → dbt + DuckDB → a live dashboard,
with Airflow, Soda, Prometheus and Grafana around it. All in Docker on about 10 GB of RAM.

What I'm proudest of is the proof, not the stack:

🔁 10 random SIGKILLs of Spark, Kafka and Kafka Connect mid-stream, run twice. Reconcile against
Postgres: 0 missing, 0 extra, 0 mismatched, 0 duplicate rows, sums equal to the cent.

🧱 Added a column to a live table: in the lakehouse 27 s later (74 s under load), no restart.

🚫 Injected 5 orders with negative totals. The quality gate quarantined them and blocked the gold
publish; the dashboard kept the last good numbers and showed a red banner. Fixed at the source,
published again 66 s later.

🩹 Deleted 1,305 rows from silver on purpose; a backfill from the raw layer repaired them in
47.5 s.

📏 Benchmarked before tuning: a longer streaming trigger gave no gain (I kept the old setting),
the laptop sustains about 400 changes/s, and compacting the raw layer made a full scan 33x faster.

The lessons that stuck: order changes by the database's own log position (LSN), keep deletes as
tombstones so replays are harmless, and measure before you optimise. I guessed the cause of a
slowdown twice and was wrong both times. Each wrong guess is written up as a decision record.

Code, decision records and every measurement: <repo link>

#DataEngineering #ChangeDataCapture #DeltaLake #ApacheSpark #Kafka #dbt
