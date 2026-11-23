"""DQ runner tests: severity rules, verdicts, quarantine dedup, and real Soda scans over an
in-memory DuckDB with the same stg_* tables the gate sees. Run in the gold image's Soda venv:
    docker compose run --rm --no-deps dbt /opt/soda/bin/python -m pytest -q /quality/tests
"""
import datetime as dt
import os
import sys

import duckdb
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dq  # noqa: E402

NOW = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def make_con(orders, users=(1, 2, 3), payments=None):
    """orders: list of (order_id, user_id, status, total, created_at, updated_at)."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("create table stg_users (user_id bigint, city varchar, tier varchar, updated_at timestamp, _lsn bigint)")
    con.executemany("insert into stg_users values (?, 'Pune', 'gold', ?, 1)", [(u, NOW) for u in users])
    con.execute("""create table stg_orders (order_id bigint, user_id bigint, status varchar, total decimal(12,2),
                   created_at timestamp, updated_at timestamp, _lsn bigint)""")
    con.executemany("insert into stg_orders values (?, ?, ?, ?, ?, ?, ?)",
                    [(*o, 100 + i) for i, o in enumerate(orders)])
    con.execute("""create table stg_payments (payment_id bigint, order_id bigint, method varchar, status varchar,
                   amount decimal(12,2), updated_at timestamp, _lsn bigint)""")
    pays = payments if payments is not None else [(o[0], o[0], "upi", "pending", abs(o[3]), NOW) for o in orders]
    if pays:
        con.executemany("insert into stg_payments values (?, ?, ?, ?, ?, ?, 1)", pays)
    return con


def good_orders(n=5, age_min=10):
    t = NOW - dt.timedelta(minutes=age_min)
    return [(i, 1 + i % 3, "paid", 100.0 + i, t, t) for i in range(1, n + 1)]


def scan(con, src_max=None):
    src = (src_max or con.execute("select max(updated_at) from stg_orders").fetchone()[0])
    v = dq.classify(dq.run_scan(con, {"SRC_ORDERS_MAX": src.strftime("%Y-%m-%d %H:%M:%S.%f")}, "test"),
                    dq.load_rules())
    return v, {c["name"]: c for c in v["checks"]}


# ---------------------------------------------------------------- rules and verdicts (pure)

def test_rules_freshness_and_volume_warn_everything_else_blocks():
    rules = dq.load_rules()
    non_blocking = {n for n, r in rules.items() if not r["blocking"]}
    assert non_blocking == {"orders_freshness_vs_source", "orders_volume_vs_trailing_hours"}
    assert rules["orders_total_non_negative"]["kind"] == "failed_rows"
    assert rules["orders_total_non_negative"]["condition"].startswith("total < 0")
    assert "left join stg_users" in rules["orders_have_a_user"]["query"]


def test_classify_fails_closed_on_errors_and_missing_checks():
    rules = {"a": {"table": "t", "blocking": True}, "b": {"table": "t", "blocking": False},
             "c": {"table": "t", "blocking": True}}
    v = dq.classify([{"name": "a", "outcome": None}, {"name": "b", "outcome": None}], rules)
    assert v["status"] == "FAIL"
    assert v["failed"] == ["a", "c"]          # a errored (blocking), c never reported
    assert v["warned"] == ["b"]               # a warn-only check that errored only warns
    v = dq.classify([{"name": "a", "outcome": "pass"}, {"name": "b", "outcome": "warn"},
                     {"name": "c", "outcome": "pass"}], rules)
    assert v["status"] == "WARN" and v["failed"] == []


def test_new_records_keeps_each_bad_version_once():
    rows = [{"check": "x", "pk": '{"order_id": 1}', "lsn": 10},
            {"check": "x", "pk": '{"order_id": 1}', "lsn": 10},     # same run sees it twice
            {"check": "x", "pk": '{"order_id": 1}', "lsn": 11},     # a newer bad version
            {"check": "x", "pk": '{"order_id": 2}', "lsn": 5}]
    existing = {("x", '{"order_id": 2}', 5)}                         # quarantined by an earlier run
    assert [(r["pk"], r["lsn"]) for r in dq.new_records(rows, existing)] == [
        ('{"order_id": 1}', 10), ('{"order_id": 1}', 11)]


def test_summary_line_names_blocking_and_warning_checks():
    v = {"status": "FAIL", "failed": ["orders_total_non_negative"], "warned": ["orders_volume_vs_trailing_hours"],
         "checks": [{"name": "orders_total_non_negative", "value": 5},
                    {"name": "orders_volume_vs_trailing_hours", "value": 4.04}], "duration_s": 1.25}
    assert dq.summary_line(v) == ("DQ FAIL | blocking: orders_total_non_negative=5 | "
                                  "warn: orders_volume_vs_trailing_hours=4.0 | 2 checks in 1.2s")


# ---------------------------------------------------------------- real Soda scans

def test_clean_data_passes():
    v, by = scan(make_con(good_orders()))
    assert v["status"] == "PASS", v
    assert by["orders_volume_vs_trailing_hours"]["value"] == 0     # < 6 hours of history: not judged


def test_negative_total_blocks_and_is_extracted_for_quarantine():
    orders = good_orders() + [(99, 1, "created", -250.0, NOW, NOW)]
    con = make_con(orders, payments=[])
    v, by = scan(con)
    assert v["status"] == "FAIL"
    assert v["failed"] == ["orders_total_non_negative"]
    assert by["orders_total_non_negative"]["value"] == 1
    rows = dq.bad_records(con, dq.load_rules(), v["failed"])
    assert [(r["check"], r["table"], r["pk"], r["lsn"]) for r in rows] == [
        ("orders_total_non_negative", "orders", '{"order_id": 99}', 105)]


def test_invalid_status_and_orphan_order_block():
    old = NOW - dt.timedelta(minutes=30)
    orders = good_orders() + [(50, 1, "lost", 10.0, old, old), (51, 777, "paid", 10.0, old, old),
                              (52, 888, "paid", 10.0, NOW, NOW)]   # orphan but < 5 min old: grace
    v, by = scan(make_con(orders))
    assert set(v["failed"]) == {"orders_status_allowed", "orders_have_a_user"}
    assert by["orders_have_a_user"]["value"] == 1


def test_duplicate_key_blocks():
    orders = good_orders() + [good_orders()[0]]
    v, _ = scan(make_con(orders, payments=[]))
    assert "orders_order_id_unique" in v["failed"]


def test_stale_silver_only_warns():
    v, by = scan(make_con(good_orders()), src_max=NOW + dt.timedelta(minutes=10))
    assert v["status"] == "WARN" and v["failed"] == []
    assert v["warned"] == ["orders_freshness_vs_source"]
    assert by["orders_freshness_vs_source"]["value"] > 300


def test_volume_drop_against_trailing_hours_only_warns():
    orders, oid = [], 0
    for ago_h, n in [(1, 100), (2, 104), (3, 96), (4, 101), (5, 99), (6, 103), (7, 97), (0, 10)]:
        t = NOW - dt.timedelta(hours=ago_h, minutes=20)
        for _ in range(n):
            oid += 1
            orders.append((oid, 1, "paid", 10.0, t, NOW))
    v, by = scan(make_con(orders, payments=[]))
    assert v["status"] == "WARN" and v["warned"] == ["orders_volume_vs_trailing_hours"]
    assert by["orders_volume_vs_trailing_hours"]["value"] > 3


@pytest.mark.parametrize("value,expect", [(-0.01, "FAIL"), (0, "PASS")])
def test_negative_payment_amount_blocks(value, expect):
    orders = good_orders(2)
    pays = [(1, 1, "upi", "success", value, NOW), (2, 2, "upi", "success", 5.0, NOW)]
    v, _ = scan(make_con(orders, payments=pays))
    assert v["status"] == expect
