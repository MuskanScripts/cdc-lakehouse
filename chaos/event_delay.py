"""How late do order events arrive? Data for choosing the watermark.

delay = bronze `ingest_ts` (when the event reached the lakehouse) - `created_at` (event time).
Read from events/orders_created (on-time events) and late_events/orders (past the watermark),
so the late ones are included in the distribution.

  python event_delay.py                 all events seen so far
  python event_delay.py --minutes 30    only events created in the last 30 minutes
"""
import argparse
import math
import os
import sys
from datetime import datetime, timedelta, timezone

from deltalake import DeltaTable

from common import STORAGE

ROOT = "s3://lakehouse"


def read(path):
    try:
        return DeltaTable(f"{ROOT}/{path}", storage_options=STORAGE).to_pyarrow_table().to_pylist()
    except Exception as e:  # table not created yet
        print(f"(no {path}: {type(e).__name__})", file=sys.stderr)
        return []


def pct(v, p):
    return v[min(len(v) - 1, math.ceil(p / 100 * len(v)) - 1)] if v else float("nan")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=0, help="only events created in the last N minutes")
    ap.add_argument("--from", dest="start", help="only events created at/after this UTC time, 'YYYY-MM-DD HH:MM'")
    ap.add_argument("--to", dest="end", help="only events created before this UTC time")
    ap.add_argument("--watermark", type=float, default=600, help="seconds, to count events beyond it")
    args = ap.parse_args()

    def naive(ts):
        return ts.replace(tzinfo=None)

    def keep(r):
        t = naive(r["event_time"])
        if args.minutes and t < (datetime.now(timezone.utc) - timedelta(minutes=args.minutes)).replace(tzinfo=None):
            return False
        if args.start and t < datetime.fromisoformat(args.start):
            return False
        return not (args.end and t >= datetime.fromisoformat(args.end))

    ev = [r for r in read("events/orders_created") if keep(r)]
    late = [r for r in read("late_events/orders") if keep(r)]
    seen, rows = set(), []
    for r in ev + late:                      # Debezium re-deliveries: count an order once
        if r["order_id"] in seen or r["ingest_ts"] is None:
            continue
        seen.add(r["order_id"])
        rows.append(r)
    d = sorted((r["ingest_ts"] - r["event_time"]).total_seconds() for r in rows)
    if not d:
        print("no events in this window")
        return
    over = sum(1 for x in d if x > args.watermark)
    print(f"order-created events: {len(d)}   (on time {len(ev)}, routed to late_events {len(late)})")
    print("delay = bronze ingest_ts - created_at, seconds:")
    for p in (50, 90, 95, 99, 99.9):
        print(f"  p{p:<5} {pct(d, p):8.1f}")
    print(f"  max    {d[-1]:8.1f}")
    print(f"events later than the {args.watermark:g}s watermark: {over} ({100 * over / len(d):.3f}%)")
    for r in late:
        print(f"  late: order {r['order_id']} created {naive(r['event_time']):%H:%M:%S}, "
              f"arrived {(r['ingest_ts'] - r['event_time']).total_seconds():.0f}s later")

    # What the watermark has to cover is DISORDER, not delay: how far behind the newest event
    # already processed (earlier silver batches) a newly arriving event is. A pipeline that is
    # 7 minutes behind but delivers in order needs no watermark at all.
    by_batch = {}
    for r in rows:
        by_batch.setdefault(r["batch_id"], []).append(r["event_time"])
    prior, dis = None, []
    for b in sorted(by_batch):
        dis += [max(0.0, (prior - t).total_seconds()) if prior else 0.0 for t in by_batch[b]]
        prior = max(by_batch[b] + ([prior] if prior else []))
    dis.sort()
    print("disorder = (max event time of earlier silver batches) - created_at, seconds:")
    print(f"  p50 {pct(dis, 50):.1f}   p99 {pct(dis, 99):.1f}   p99.9 {pct(dis, 99.9):.1f}   max {dis[-1]:.1f}"
          f"   ({sum(1 for x in dis if x > 0)} of {len(dis)} events arrived behind a newer one)")


if __name__ == "__main__":
    main()
    sys.stdout.flush()
    os._exit(0)
