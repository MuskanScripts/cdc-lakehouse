# Week 6 benchmarks

All on the project laptop: Windows, WSL2 Ubuntu with 9.7 GB RAM, Docker 29, one Spark driver
`local[4]` with a 2200m heap in a 4 GB container. MinIO, Kafka, Postgres and Connect share the
same machine. Airflow was stopped for every benchmark so its 5-minute DAGs don't add CPU noise.
The gold loop (dbt every 30 s) and monitoring kept running, the same in every run. Raw output is in
`bench/trigger_results.log`, `bench/optimize_results.log` and `bench/rate_results.log`.
Measured 2026-11-23/24.

## 1. Silver trigger: 10 s vs 30 s vs 60 s (ADR-034)

`make bench-trigger`. Silver alone gets the new interval (bronze stays at 10 s). For each run: the
driver is recreated (cold JVM), 120 s warm-up at 50 changes/s, then **300 s measured at 50
changes/s**. During the measured window Kafka lag and silver lag are sampled every 15 s, and
Delta's `executionTimeMs` is read for every MERGE commit in it. Then reconcile, and latency for
exactly the rows changed in the window. 10 s and 30 s were run twice, the second pass in reverse
order (30 then 10), to see the noise.

| silver trigger | 10 s | 10 s (repeat) | 30 s | 30 s (repeat) | 60 s |
|---|---|---|---|---|---|
| silver batches in 300 s | 10 | 10 | 10 | 10 | 7 |
| batch time median / p90 / max | 29.1 / 41.6 / 41.6 s | 27.6 / 32.7 / 32.7 s | 29.2 / 32.9 / 32.9 s | 32.2 / 40.5 / 40.5 s | 30.3 / 72.5 / 72.5 s |
| rows per batch, median | 1,676 | 1,564 | 1,426 | 1,752 | 2,921 |
| MERGE `executionTimeMs` median (max), all 5 tables | 12.8 s (22.8) | 12.2 s (16.8) | 12.9 s (16.6) | 12.6 s (21.9) | 12.5 s (20.9) |
| orders source rows per MERGE, median | 648 | 637 | 546 | 649 | 1,145 |
| Kafka lag (Kafka → bronze), median / max | 857 / 1,651 | 878 / 1,498 | 947 / 1,335 | 932 / 1,827 | 797 / 1,862 |
| silver lag (now − newest change in silver), median / max | 60.9 / 78.6 s | 48.3 / 79.2 s | 52.2 / 75.6 s | 57.2 / 76.6 s | 67.8 / 127.1 s |
| **latency p50 / p95** (Postgres commit → silver) | 42.8 / 63.5 s | **36.2 / 53.6 s** | 36.1 / 56.1 s | 40.8 / 60.1 s | **48.2 / 73.1 s** |
| rows in the latency sample | 10,284 | 10,401 | 10,131 | 10,295 | 10,280 |
| reconcile after the run | PASS | PASS | PASS | PASS | PASS |

**Lag trend.** In every run both lags were flat while the simulator ran and drained when it
stopped. All slopes were negative: the sampler runs 30 s past the simulator. In the repeat pass the
median of the first vs last third of the samples was: 10 s: Kafka 909 → 920, silver 59.2 → 45.4 s;
30 s: Kafka 930 → 842, silver 61.2 → 55.2 s. The per-15 s samples are in the raw log.

**Result: no change, the trigger stays 10 s.**
- 10 s and 30 s behave identically. A batch takes about 29 s, longer than a 10 s trigger, so Spark
  starts each batch right after the previous one: 10 batches in 300 s either way. Their latency
  difference (p50 36-43 s) is as large between two runs of one setting as between the settings.
- 60 s doubles the rows per MERGE (orders 1,145 vs about 640) at the **same** MERGE time (12.5 s),
  which confirms the cost is fixed per MERGE (ADR-028). But silver then idles about half the time,
  and latency gets worse by 5-12 s at p50 and 10-20 s at p95.
- Because batches already run back to back, silver batches grow on their own when load grows (up to
  `maxFilesPerTrigger=20` bronze files per table), so a longer trigger doesn't buy capacity either.

## 2. OPTIMIZE / ZORDER before and after

`make bench-optimize`, with the simulator stopped. Before: stats plus two fixed queries. Then, with
streaming stopped (required, ADR-023): `OPTIMIZE` on every bronze table (compaction; bronze has no
business key to ZORDER by) and `OPTIMIZE ... ZORDER BY <first pk>` on every silver table. Then the
same stats and queries again. No VACUUM, because it removes only unreferenced files and doesn't
change what a query reads. Finally streaming was restarted and the simulator ran for 180 s at
50/s, then silver was measured again (*decay*).

Fixed queries (`chaos/bench_query.py`, delta-rs + pyarrow, the reader that reconcile and the dbt
delta plugin use; median of 5 runs, including opening the table):
- **Q1** `bronze/orders` full scan: count rows and distinct (partition, offset). This is what a
  backfill reads.
- **Q2** `silver/orders` key range: the 1,000 highest `order_id`s at the start (80755-81754),
  plus how many files' min/max stats overlap that range.

| | before | after OPTIMIZE | after 180 s of streaming at 50/s |
|---|---|---|---|
| **bronze**, active files (all 5 tables) | **2,462** | **5** | (orders: 14) |
| bronze avg file size | **28.3 KiB** | **12,408 KiB** | |
| bronze/orders files | 496 (avg 57.5 KiB) | 1 (26.3 MiB) | 14 |
| **Q1** bronze/orders full scan (228,688 rows) | **1.97 s** (best 1.82) | **0.06 s** (best 0.05) | 0.11 s (232,227 rows) |
| **silver**, active files (all 5 tables) | 31 | 5 | 21 |
| silver avg file size | 343.5 KiB | 2,040.7 KiB | 513.6 KiB |
| silver/orders files / files overlapping the Q2 range | 4 / 4 | 1 / 1 | 4 / 4 |
| **Q2** silver/orders key range (1,000 rows) | 0.08 s | 0.05 s | 0.08 s |
| OPTIMIZE time per table | | bronze 9.5-18.5 s, silver ZORDER 2.7-5.8 s | |

**Ratios.** Bronze: 492x fewer files, and Q1 is **33x faster** (1.97 → 0.06 s). Silver: 6x
fewer files and Q2 1.6x faster, but only on a 0.08 s query.

**What it means.**
- **The small-file problem is in bronze, not silver.** Bronze appends one file per table per 10 s
  trigger, so it had 496 files averaging 57 KiB for orders. Opening each file costs a round trip
  to S3, so a full read (a backfill, or any reprocessing) took 1.97 s for just 28 MB. Compaction fixes
  that completely, and it stays mostly fixed: 180 s of streaming added 13 files, which cost 0.05 s.
- **ZORDER on silver doesn't last.** It made the key range hit 1 file out of 1, but 180 s of
  MERGEs brought silver/orders back to 4 files, each covering the whole id range again. That is
  ADR-023's root cause: the MERGE writes through a 4-partition shuffle and every batch touches
  keys everywhere. On a table of a few MB it doesn't matter (0.08 s). On a big table the nightly
  ZORDER would need the key-range MERGE idea from ADR-028 to keep its value.
- So the nightly `optimize` DAG is worth it for bronze. **I added bronze to it** (compaction only,
  ADR-035). Its first run through Airflow compacted 137 → 1 file per bronze table, and streaming
  was paused 15 min 24 s in total, most of it VACUUM listing old files.

## 3. Highest sustainable rate

`make bench-rate RATES="..." [SIM_ARGS=--fast]`. For each rate the simulator runs 300 s while
Kafka lag and silver lag are sampled every 15 s. Then reconcile waits for silver to catch up. The
silver trigger is 10 s.

**Rule for "flat"** (in `bench/max_rate.sh`): the median of the last third of the samples is at most
the median of the first third **plus one bronze trigger of input** (rate × 10 messages). Lag is a
sawtooth: it fills for one trigger, then a batch drains it. A single sample or the slope is
therefore noisy.

Run order: 100, 200, 400, 800, 600, 400 again. Above about 400/s one simulator connection can't
keep the pace with synchronous commits, so I used `--fast` (`synchronous_commit=off` for the
simulator's session only) for 600, 800 and the second 400.

| rate asked → achieved | Kafka lag median | first → last third (median) | allowed growth | flat? | bronze batch median | silver batch median, rows per batch | silver caught up after stop | reconcile |
|---|---|---|---|---|---|---|---|---|
| 100 → 100.0/s | 2,054 | 1,687 → 1,415 | 1,000 | **yes** | 11.2 s | 27.6 s, 2,774 | 58 s | PASS |
| 200 → 199.9/s | 3,250 | 2,737 → 2,976 | 2,000 | **yes** | 11.4 s | 27.6 s, 4,725 | 69 s | PASS |
| 400 → 383.8/s | 6,894 | 6,236 → 6,304 | 4,000 | **yes** | 11.5 s | 28.5 s, 10,512 | 100 s | PASS |
| 400 → 400.0/s (`--fast`, run last) | 12,334 | 9,073 → 11,505 | 4,000 | yes, just (+2,432) | 17.4 s | 45.0 s, 18,926 | 125 s | PASS |
| 600 → 600.0/s (`--fast`) | 18,274 | 12,510 → 19,379 | 6,000 | **no** (+6,869) | 19.6 s | 48.1 s, 24,826 | 197 s | PASS |
| 800 → 800.0/s (`--fast`) | 22,090 | 11,314 → 28,866 | 8,000 | **no** (+17,552) | 16.8 s | 41.6 s, 17,445 | 122 s | PASS |

Silver lag (now − newest change in silver) was flat or falling in every run. Its medians were
51.5 / 53.3 / 52.4 s at 100 / 200 / 400 and 85-93 s from 400-fast upwards. Its first samples are
inflated by the idle gap before each run.

**Result: about 400 changes/s is the highest sustainable rate on this laptop.** 600/s grows
(slowly), and 800/s grows fast. Every run still reconciled to 0 difference after the simulator
stopped.

**What limits it.**
- **Up to 400/s silver isn't the limit.** Its batch time stayed at about 28 s while the rows per
  batch grew from 2.8k to 10.5k: the fixed MERGE cost again (ADR-028/034). Bronze stayed at about
  11.5 s against its 10 s trigger.
- **Above that, the one JVM runs out of CPU.** At 600-800/s the driver used 420-535% CPU on
  `local[4]`. Bronze batches took 17-20 s against a 10 s trigger, so Kafka lag grew. It is bronze
  (Kafka → Delta) that falls behind, because bronze, silver and the aggregate share 4 cores.
  `maxOffsetsPerTrigger=50000` was not reached.
- **The ceiling falls as silver grows.** The second 400/s run came after the 600 and 800 runs had
  added about 420k changes. Bronze then took 17.4 s instead of 11.5 s, and the lag only just passed
  the rule. That is ADR-023's write amplification: every MERGE rewrites the whole table, so CPU per
  batch grows with table size.
- Memory was not the limit: the streaming container peaked at 3.60 of 3.91 GiB, with 0 restarts
  and no OOM kill.

**Compared with earlier weeks.** Week 2 wrote "200/s is close to this machine's limit", when silver
batches were 10-12 s against a 10 s trigger. With batches running back to back (ADR-034) silver
absorbs more per batch, so the limit moved to bronze's CPU share.

## 4. Fresh clone, README quick start only (ADR-036)

The main stack was stopped with `make down` (volumes kept). The repo was cloned to
`/tmp/fresh-clone` and run with `COMPOSE_PROJECT_NAME=fresh` (separate volumes), following only the
quick start. Log: `bench/fresh_clone.log`.

| step | elapsed |
|---|---|
| `git clone` | 0 s |
| `make up` done (connector registered) | 1 min 24 s |
| `make stream-up`, `make gold-up` done | 1 min 29 s |
| dashboard live (gold published, orders > 0, no red banner, DQ PASS) | **4 min 1 s** |
| simulator done (300 s at 50/s) | 6 min 37 s |
| `make reconcile WAIT=300`: PASS, 0 difference on all 5 tables (2,037 orders) | 7 min 1 s |
| `make test`: 30 + 12 passed | **10 min 17 s** |

The Docker image cache was warm (same machine). The volumes were new, so Postgres was initialised
from `init.sql` and the Spark jars were downloaded into an empty Ivy cache. The **first attempt
failed**: every gold build errored on `coupon_code`, a column that only the Week 3 drift drill
creates. It was fixed (ADR-036) and the timings above are from a second, completely new clone.
