"""E-commerce OLTP simulator for the CDC lakehouse.

Writes realistic inserts, updates and deletes into Postgres so Debezium has a
stream of row-level changes for every table:

  users        c: signups        u: city/tier changes        d: deleting inactive accounts
  products     c: new products   u: price changes            d: discontinuing unsold products
  orders       c: checkout       u: paid/shipped/delivered/cancelled/refunded
                                                             d: purging cancelled orders
  order_items  c: with each order u: cart quantity changes   d: cascaded from order purge
  payments     c: with each order u: success/failed/refunded d: cascaded from order purge

Each business action is one transaction, like a real app. --rate throttles on
row-level changes per second (what Debezium sees), not on actions.

Usage:
  python simulator.py --rate 200 --duration 120
"""

from __future__ import annotations

import argparse
import os
import random
import signal
import sys
import time
from collections import Counter
from decimal import Decimal

import psycopg
from faker import Faker

CITIES = ["Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Chennai", "Pune",
          "Kolkata", "Ahmedabad", "Jaipur", "Chandigarh", "Patiala", "Lucknow"]
TIERS = ["bronze", "silver", "gold", "platinum"]
CATEGORIES = ["electronics", "fashion", "home", "grocery", "beauty", "books", "sports", "toys"]
PAY_METHODS = ["upi", "card", "netbanking", "wallet", "cod"]

# action -> relative weight. Tuned so every table sees c, u and d events.
ACTION_WEIGHTS = {
    "new_order": 32,
    "change_cart_qty": 3,
    "pay_order": 23,
    "ship_order": 12,
    "deliver_order": 10,
    "cancel_order": 4,
    "refund_order": 4,
    "update_user": 4,
    "update_product_price": 3,
    "new_user": 2,
    "new_product": 1,
    "purge_cancelled_order": 1,
    "delete_inactive_user": 0.5,
    "delete_unsold_product": 0.5,
}


class Pool:
    """Set of ids with O(1) random pick and removal."""

    def __init__(self, items=()):
        self._items: list[int] = []
        self._index: dict[int, int] = {}
        for i in items:
            self.add(i)

    def add(self, item: int) -> None:
        if item not in self._index:
            self._index[item] = len(self._items)
            self._items.append(item)

    def remove(self, item: int) -> None:
        idx = self._index.pop(item, None)
        if idx is None:
            return
        last = self._items.pop()
        if idx < len(self._items):
            self._items[idx] = last
            self._index[last] = idx

    def pick(self) -> int | None:
        return random.choice(self._items) if self._items else None

    def pop_random(self) -> int | None:
        item = self.pick()
        if item is not None:
            self.remove(item)
        return item

    def __len__(self) -> int:
        return len(self._items)


class Simulator:
    def __init__(self, conn: psycopg.Connection, fake: Faker):
        self.conn = conn
        self.fake = fake
        self.users = Pool()
        self.products: dict[int, Decimal] = {}
        self.product_pool = Pool()
        self.orders_by_status: dict[str, Pool] = {
            s: Pool() for s in ("created", "paid", "shipped", "delivered", "cancelled")
        }
        self.new_users = Pool()      # candidates for "delete inactive user"
        self.new_products = Pool()   # candidates for "discontinue unsold product"
        self.row_changes = Counter()  # (table, op) -> count

    # ---------- setup ----------

    def seed(self, n_users: int, n_products: int) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM users")
            if cur.fetchone()[0] == 0:
                print(f"seeding {n_users} users and {n_products} products ...", flush=True)
                for _ in range(n_users):
                    cur.execute(
                        "INSERT INTO users (name, email, city, tier) VALUES (%s, %s, %s, %s)",
                        self._fake_user(),
                    )
                for _ in range(n_products):
                    cur.execute(
                        "INSERT INTO products (name, category, price) VALUES (%s, %s, %s)",
                        self._fake_product(),
                    )
                self.conn.commit()
        self._load_state()

    def _load_state(self) -> None:
        """Rebuild in-memory pools from the DB so the simulator can be restarted."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            for (uid,) in cur:
                self.users.add(uid)
            cur.execute("SELECT product_id, price FROM products")
            for pid, price in cur:
                self.products[pid] = price
                self.product_pool.add(pid)
            cur.execute(
                "SELECT order_id, status FROM orders "
                "WHERE status IN ('created','paid','shipped','delivered','cancelled')"
            )
            for oid, status in cur:
                self.orders_by_status[status].add(oid)
        self.conn.commit()
        print(f"state: {len(self.users)} users, {len(self.products)} products, "
              + ", ".join(f"{k}={len(v)}" for k, v in self.orders_by_status.items()), flush=True)

    # ---------- fake data ----------

    def _fake_user(self):
        name = self.fake.name()
        email = f"{self.fake.user_name()}.{random.randint(0, 10**9)}@example.com"
        return name, email, random.choice(CITIES), random.choices(TIERS, [60, 25, 10, 5])[0]

    def _fake_product(self):
        category = random.choice(CATEGORIES)
        name = f"{self.fake.word().title()} {category[:-1] if category.endswith('s') else category}"
        price = Decimal(random.randint(49, 49999)) + Decimal(random.choice([0, 0.49, 0.99])).quantize(Decimal("0.01"))
        return name, category, price

    # ---------- actions: each returns list of (table, op) row changes ----------

    def new_order(self, cur):
        uid = self.users.pick()
        if uid is None or len(self.product_pool) == 0:
            return []
        n_items = random.choices([1, 2, 3, 4], [50, 30, 15, 5])[0]
        chosen = {self.product_pool.pick() for _ in range(n_items)}
        items = [(pid, random.randint(1, 3), self.products[pid]) for pid in chosen]
        total = sum(qty * price for _, qty, price in items)

        cur.execute(
            "INSERT INTO orders (user_id, status, total) VALUES (%s, 'created', %s) RETURNING order_id",
            (uid, total),
        )
        oid = cur.fetchone()[0]
        cur.executemany(
            "INSERT INTO order_items (order_id, product_id, qty, price) VALUES (%s, %s, %s, %s)",
            [(oid, pid, qty, price) for pid, qty, price in items],
        )
        cur.execute(
            "INSERT INTO payments (order_id, method, status, amount) VALUES (%s, %s, 'pending', %s)",
            (oid, random.choice(PAY_METHODS), total),
        )
        self.orders_by_status["created"].add(oid)
        self.new_users.remove(uid)  # has an order now, not a deletion candidate
        return [("orders", "c"), *[("order_items", "c")] * len(items), ("payments", "c")]

    def change_cart_qty(self, cur):
        """Customer changes quantity before paying: item, order total and payment amount all update."""
        oid = self.orders_by_status["created"].pick()
        if oid is None:
            return []
        cur.execute(
            "UPDATE order_items SET qty = %s "
            "WHERE (order_id, product_id) = (SELECT order_id, product_id FROM order_items "
            "                                WHERE order_id=%s LIMIT 1)",
            (random.randint(1, 5), oid),
        )
        if not cur.rowcount:
            return []
        cur.execute(
            "UPDATE orders SET total = (SELECT sum(qty * price) FROM order_items WHERE order_id=%s), "
            "updated_at = now() WHERE order_id=%s RETURNING total",
            (oid, oid),
        )
        total = cur.fetchone()[0]
        cur.execute("UPDATE payments SET amount=%s, updated_at=now() WHERE order_id=%s", (total, oid))
        return [("order_items", "u"), ("orders", "u"), ("payments", "u")]

    def pay_order(self, cur):
        oid = self.orders_by_status["created"].pop_random()
        if oid is None:
            return []
        if random.random() < 0.06:  # payment failure -> order cancelled
            cur.execute("UPDATE payments SET status='failed', updated_at=now() WHERE order_id=%s", (oid,))
            cur.execute("UPDATE orders SET status='cancelled', updated_at=now() WHERE order_id=%s", (oid,))
            self.orders_by_status["cancelled"].add(oid)
        else:
            cur.execute("UPDATE payments SET status='success', updated_at=now() WHERE order_id=%s", (oid,))
            cur.execute("UPDATE orders SET status='paid', updated_at=now() WHERE order_id=%s", (oid,))
            self.orders_by_status["paid"].add(oid)
        return [("payments", "u"), ("orders", "u")]

    def _advance(self, cur, from_status, to_status):
        oid = self.orders_by_status[from_status].pop_random()
        if oid is None:
            return []
        cur.execute("UPDATE orders SET status=%s, updated_at=now() WHERE order_id=%s", (to_status, oid))
        if to_status in self.orders_by_status:
            self.orders_by_status[to_status].add(oid)
        return [("orders", "u")]

    def ship_order(self, cur):
        return self._advance(cur, "paid", "shipped")

    def deliver_order(self, cur):
        return self._advance(cur, "shipped", "delivered")

    def cancel_order(self, cur):
        oid = self.orders_by_status["created"].pop_random()
        if oid is None:
            return []
        cur.execute("UPDATE orders SET status='cancelled', updated_at=now() WHERE order_id=%s", (oid,))
        cur.execute("UPDATE payments SET status='failed', updated_at=now() WHERE order_id=%s", (oid,))
        self.orders_by_status["cancelled"].add(oid)
        return [("orders", "u"), ("payments", "u")]

    def refund_order(self, cur):
        oid = self.orders_by_status["delivered"].pop_random()
        if oid is None:
            return []
        cur.execute("UPDATE orders SET status='refunded', updated_at=now() WHERE order_id=%s", (oid,))
        cur.execute("UPDATE payments SET status='refunded', updated_at=now() WHERE order_id=%s", (oid,))
        return [("orders", "u"), ("payments", "u")]

    def update_user(self, cur):
        uid = self.users.pick()
        if uid is None:
            return []
        if random.random() < 0.6:  # moved city -> matters for SCD2 dim_user later
            cur.execute("UPDATE users SET city=%s, updated_at=now() WHERE user_id=%s",
                        (random.choice(CITIES), uid))
        else:
            cur.execute("UPDATE users SET tier=%s, updated_at=now() WHERE user_id=%s",
                        (random.choice(TIERS), uid))
        return [("users", "u")]

    def update_product_price(self, cur):
        pid = self.product_pool.pick()
        if pid is None:
            return []
        factor = Decimal(random.choice(["0.80", "0.90", "0.95", "1.05", "1.10", "1.20"]))
        new_price = (self.products[pid] * factor).quantize(Decimal("0.01"))
        cur.execute("UPDATE products SET price=%s, updated_at=now() WHERE product_id=%s", (new_price, pid))
        self.products[pid] = new_price
        return [("products", "u")]

    def new_user(self, cur):
        cur.execute(
            "INSERT INTO users (name, email, city, tier) VALUES (%s, %s, %s, %s) RETURNING user_id",
            self._fake_user(),
        )
        uid = cur.fetchone()[0]
        self.users.add(uid)
        self.new_users.add(uid)
        return [("users", "c")]

    def new_product(self, cur):
        row = self._fake_product()
        cur.execute(
            "INSERT INTO products (name, category, price) VALUES (%s, %s, %s) RETURNING product_id",
            row,
        )
        pid = cur.fetchone()[0]
        self.products[pid] = row[2]
        self.product_pool.add(pid)
        self.new_products.add(pid)
        return [("products", "c")]

    def purge_cancelled_order(self, cur):
        oid = self.orders_by_status["cancelled"].pop_random()
        if oid is None:
            return []
        cur.execute("SELECT count(*) FROM order_items WHERE order_id=%s", (oid,))
        n_items = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM payments WHERE order_id=%s", (oid,))
        n_pay = cur.fetchone()[0]
        cur.execute("DELETE FROM orders WHERE order_id=%s", (oid,))  # cascades to items + payments
        return [("orders", "d"), *[("order_items", "d")] * n_items, *[("payments", "d")] * n_pay]

    def delete_inactive_user(self, cur):
        uid = self.new_users.pop_random()
        if uid is None:
            return []
        cur.execute(
            "DELETE FROM users WHERE user_id=%s AND NOT EXISTS (SELECT 1 FROM orders WHERE user_id=%s)",
            (uid, uid),
        )
        if cur.rowcount:
            self.users.remove(uid)
            return [("users", "d")]
        return []

    def delete_unsold_product(self, cur):
        pid = self.new_products.pop_random()
        if pid is None:
            return []
        # Take it off sale first so no concurrent new_order picks it.
        self.product_pool.remove(pid)
        cur.execute(
            "DELETE FROM products WHERE product_id=%s "
            "AND NOT EXISTS (SELECT 1 FROM order_items WHERE product_id=%s)",
            (pid, pid),
        )
        if cur.rowcount:
            del self.products[pid]
            return [("products", "d")]
        self.product_pool.add(pid)  # it had sales; keep selling it
        return []

    # ---------- main loop ----------

    def run(self, rate: float, duration: float, stats_every: float = 5.0) -> None:
        actions = list(ACTION_WEIGHTS)
        weights = list(ACTION_WEIGHTS.values())
        stop = {"flag": False}

        def _stop(*_):
            stop["flag"] = True
        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

        start = last_stats = time.monotonic()
        events = events_at_last_stats = 0
        print(f"running at ~{rate:g} row changes/sec"
              + (f" for {duration:g}s" if duration else " until Ctrl+C"), flush=True)

        while not stop["flag"]:
            now = time.monotonic()
            if duration and now - start >= duration:
                break

            action = random.choices(actions, weights)[0]
            try:
                with self.conn.cursor() as cur:
                    changes = getattr(self, action)(cur)
                self.conn.commit()
            except psycopg.Error as exc:
                self.conn.rollback()
                print(f"[warn] {action} failed: {exc.__class__.__name__}: {exc}".strip(), flush=True)
                continue

            for change in changes:
                self.row_changes[change] += 1
            events += len(changes)

            # Throttle: stay on the line events = rate * elapsed.
            ahead = events / rate - (time.monotonic() - start)
            if ahead > 0:
                time.sleep(ahead)

            if now - last_stats >= stats_every:
                window = now - last_stats
                print(f"[{now - start:6.0f}s] {(events - events_at_last_stats) / window:7.1f} changes/s "
                      f"| total {events}", flush=True)
                last_stats, events_at_last_stats = now, events

        elapsed = time.monotonic() - start
        print(f"\nstopped after {elapsed:.1f}s, {events} row changes "
              f"({events / max(elapsed, 1e-9):.1f}/s)")
        self.print_summary()

    def print_summary(self) -> None:
        tables = ["users", "products", "orders", "order_items", "payments"]
        print(f"{'table':<12} {'c':>8} {'u':>8} {'d':>8}")
        for t in tables:
            print(f"{t:<12} {self.row_changes[(t, 'c')]:>8} {self.row_changes[(t, 'u')]:>8} "
                  f"{self.row_changes[(t, 'd')]:>8}")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rate", type=float, default=float(os.getenv("SIM_RATE", 100)),
                   help="target row changes per second (default 100)")
    p.add_argument("--duration", type=float, default=float(os.getenv("SIM_DURATION", 0)),
                   help="seconds to run; 0 = until Ctrl+C (default 0)")
    p.add_argument("--seed-users", type=int, default=1000)
    p.add_argument("--seed-products", type=int, default=200)
    p.add_argument("--random-seed", type=int, default=None,
                   help="make a run reproducible")
    p.add_argument("--fast", action="store_true",
                   help="SET synchronous_commit=off for this session (needed above ~4k changes/s; "
                        "WAL content is identical, commits just don't wait for fsync)")
    args = p.parse_args(argv)

    if args.random_seed is not None:
        random.seed(args.random_seed)
        Faker.seed(args.random_seed)

    # Connection settings come from the standard PG* env vars.
    for attempt in range(30):
        try:
            conn = psycopg.connect(autocommit=False)
            break
        except psycopg.OperationalError as exc:
            print(f"waiting for postgres ({exc.__class__.__name__}) ...", flush=True)
            time.sleep(2)
    else:
        print("could not connect to postgres", file=sys.stderr)
        return 1

    with conn:
        if args.fast:
            conn.execute("SET synchronous_commit = off")
            conn.commit()
        sim = Simulator(conn, Faker("en_IN"))
        sim.seed(args.seed_users, args.seed_products)
        sim.run(args.rate, args.duration)
    return 0


if __name__ == "__main__":
    sys.exit(main())
