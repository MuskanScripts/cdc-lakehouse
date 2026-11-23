"""Render the dashboard headless (Streamlit AppTest) and print what a viewer would see.

  docker compose exec dashboard python /app/snapshot.py

Runs app.py once in-process, fails loudly on any exception in the page, and prints every
metric tile plus the last rows behind the per-minute charts. Used to verify the page without a
browser.
"""
import sys

from streamlit.testing.v1 import AppTest

at = AppTest.from_file("/app/app.py", default_timeout=90).run()
if at.exception:
    for e in at.exception:
        print("EXCEPTION:", e.message)
    sys.exit(1)
for w in at.warning:
    print("WARNING:", w.value)
for m in at.metric:
    print(f"{m.label:<40} {m.value}")

import duckdb  # noqa: E402

con = duckdb.connect("/data/gold.duckdb", read_only=True)
print("\nlast 5 minutes (all cities), refunds by minute issued:")
# group and sort by the TIMESTAMP; the label is aliased `hhmm` so it cannot shadow `minute`
print(con.execute("""
    with a as (select minute, sum(orders) as orders, round(sum(net_revenue)) as net_revenue,
                      sum(paid_orders) as paid from agg_revenue_minute group by minute)
    select strftime(a.minute, '%H:%M') as hhmm, a.orders, a.net_revenue, a.paid,
           coalesce(r.refunds, 0) as refunds_issued,
           round(100.0 * coalesce(r.refunds, 0) / a.orders, 1) as refund_pct
    from a left join agg_refunds_minute r on r.minute = a.minute
    order by a.minute desc limit 5""").df().to_string(index=False))
con.close()
