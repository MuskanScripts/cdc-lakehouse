"""Prove the late-event path end to end on the live pipeline.

  1. insert a fresh order (created_at = now) so the watermark is current
  2. wait until it is in events/orders_created
  3. insert an order that was "created" 30 minutes ago (a late arrival)
  4. it must show up in late_events/orders and must NOT change orders_per_minute for its minute

The watermark is in event time, so step 1 matters: on an idle system the watermark is old and a
30 minute old event would still be on time.
"""
import os
import sys
import time

import psycopg
from deltalake import DeltaTable

from common import STORAGE

ROOT = "s3://lakehouse"


def table(path):
    return DeltaTable(f"{ROOT}/{path}", storage_options=STORAGE).to_pyarrow_table().to_pylist()


def insert_order(conn, age_minutes):
    return conn.execute(
        "INSERT INTO orders (user_id, status, total, created_at, updated_at) "
        "VALUES ((SELECT min(user_id) FROM users), 'created', 1.00, now() - make_interval(mins => %s), now()) "
        "RETURNING order_id, created_at", (age_minutes,)).fetchone()


def wait_for(path, order_id, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            hit = [r for r in table(path) if r["order_id"] == order_id]
        except Exception:
            hit = []
        if hit:
            return time.time() - t0, hit[0]
        time.sleep(1)
    return None, None


def minute_count(created_at):
    start = created_at.replace(second=0, microsecond=0, tzinfo=None)
    try:
        rows = [r for r in table("agg/orders_per_minute") if r["window_start"].replace(tzinfo=None) == start]
    except Exception:
        rows = []
    return rows[0]["orders"] if rows else 0


def main() -> int:
    with psycopg.connect(autocommit=True) as conn:
        fresh_id, fresh_ts = insert_order(conn, 0)
        print(f"fresh order {fresh_id} created_at={fresh_ts:%H:%M:%S}")
        secs, _ = wait_for("events/orders_created", fresh_id)
        if secs is None:
            print("fresh order never reached events/orders_created: is `make stream-up` running?")
            return 1
        print(f"  on time, in events/orders_created after {secs:.0f}s")

        late_id, late_ts = insert_order(conn, 30)
        before = minute_count(late_ts)
        print(f"late order  {late_id} created_at={late_ts:%H:%M:%S} (30 min ago); orders_per_minute for that minute = {before}")
    secs, row = wait_for("late_events/orders", late_id)
    if secs is None:
        print("late order was NOT routed to late_events/orders")
        return 1
    time.sleep(25)  # let the aggregate query run a few more triggers
    after = minute_count(late_ts)
    in_events = [r for r in table("events/orders_created") if r["order_id"] == late_id]
    print(f"  in late_events/orders after {secs:.0f}s: watermark={row['watermark']:%H:%M:%S}, event_time={row['event_time']:%H:%M:%S}")
    print(f"  in events/orders_created: {len(in_events)} row(s);  orders_per_minute for its minute: {before} -> {after}")
    ok = not in_events and after == before
    print("LATE EVENT PATH OK" if ok else "LATE EVENT PATH FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    os._exit(code)
