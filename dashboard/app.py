"""Live revenue dashboard (Streamlit) over the published gold DuckDB file.

Every refresh opens /data/gold.duckdb READ-ONLY with a new connection and closes it again, so it
always sees the latest complete build that the dbt loop published (atomic rename) and never holds
a lock. Times are UTC.
"""
import os
import time

import duckdb
import pandas as pd
import streamlit as st

import health

GOLD = os.environ.get("GOLD_SERVE_PATH", "/data/gold.duckdb")
WINDOW_MIN = int(os.environ.get("DASH_WINDOW_MINUTES", "60"))
REFRESH_S = int(os.environ.get("DASH_REFRESH_SECONDS", "10"))

st.set_page_config(page_title="CDC Lakehouse: live revenue", layout="wide")
st.title("CDC Lakehouse: live revenue")
st.caption(f"Postgres → Debezium → Kafka → Delta silver → dbt gold (DuckDB). Last {WINDOW_MIN} min, UTC, "
           f"auto-refresh every {REFRESH_S}s.")


def query(sql: str) -> pd.DataFrame:
    con = duckdb.connect(GOLD, read_only=True)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


@st.fragment(run_every=REFRESH_S)
def live():
    if not os.path.exists(GOLD):
        st.warning("No gold build published yet: is `make gold-up` running?")
        return
    since = f"now()::timestamp - interval {WINDOW_MIN} minute"
    per_min = query(f"""
        select minute, sum(orders) as orders, sum(net_revenue) as net_revenue,
               sum(paid_orders) as paid_orders, sum(refunded_orders) as refunded_orders
        from agg_revenue_minute where minute >= {since}
        group by minute order by minute""")
    refunds = query(f"""
        select minute, refunds, refund_amount from agg_refunds_minute
        where minute >= {since} order by minute""")
    by_city = query(f"""
        select city, sum(net_revenue) as net_revenue, sum(orders) as orders
        from agg_revenue_minute where minute >= {since}
        group by city order by net_revenue desc""")
    info = query("select * from gold_build_info").iloc[0]

    # refund rate = refunds ISSUED in the window / orders PLACED in the window: two flows over the
    # same minutes. "Paid orders placed" is not used as the base: new orders get paid at random
    # later times, so that base lags and the ratio overshoots (ADR-026).
    placed = per_min["orders"].sum()
    refund_rate = refunds["refunds"].sum() / placed if placed else 0.0
    age = (pd.Timestamp.utcnow().tz_localize(None) - pd.Timestamp(info["built_at"])).total_seconds()

    k = st.columns(5)
    k[0].metric(f"Net revenue, {WINDOW_MIN} min", f"₹{per_min['net_revenue'].sum():,.0f}")
    k[1].metric(f"Orders, {WINDOW_MIN} min", f"{int(per_min['orders'].sum()):,}")
    k[2].metric(f"Refund rate, {WINDOW_MIN} min (refunds issued / orders placed)", f"{refund_rate:.1%}")
    k[3].metric("Gold built", f"{age:.0f}s ago")
    k[4].metric("Orders in gold (all time)", f"{int(info['orders']):,}")

    pm = per_min.set_index("minute").join(refunds.set_index("minute")[["refunds"]], how="left").fillna({"refunds": 0})
    pm["refund_rate"] = (pm["refunds"] / pm["orders"].where(pm["orders"] > 0)).fillna(0.0)
    c1, c2 = st.columns(2)
    c1.subheader("Net revenue per minute")
    c1.line_chart(pm[["net_revenue"]])
    c2.subheader("Orders per minute")
    c2.bar_chart(pm[["orders"]])
    c3, c4 = st.columns(2)
    c3.subheader("Refund rate per minute")
    c3.caption("Refunds issued in the minute / orders placed in the minute.")
    c1.caption("Counted once an order is paid, so the newest minutes keep rising for a while.")
    c3.line_chart(pm[["refund_rate"]])
    c4.subheader("Net revenue by city (city at order time)")
    c4.bar_chart(by_city.set_index("city")[["net_revenue"]])

    st.subheader("Pipeline health")
    h = st.columns(4)
    try:
        lag = health.kafka_lag(health.bronze_committed(), health.kafka_end_offsets())
        h[0].metric("Kafka lag (msgs not yet in bronze)", f"{lag:,}")
    except Exception as e:  # Kafka restarting etc.: show it, don't break the page
        h[0].metric("Kafka lag", "n/a")
        h[0].caption(f"{type(e).__name__}: {e}")
    q = health.quarantine_rows()
    h[1].metric("Quarantined rows", f"{sum(q.values()):,}")
    h[1].caption(", ".join(f"{t}={n}" for t, n in q.items() if n) or "none")
    h[2].metric("Latest order change in gold", f"{pd.Timestamp(info['max_order_updated_at']):%H:%M:%S}")
    h[3].metric("Page rendered", time.strftime("%H:%M:%S", time.gmtime()))


live()
