-- The minute x city aggregate must count every order exactly once. It is a LEFT join, so a gap
-- in the SCD2 versions lands in 'unknown' (see assert_older_orders_have_a_city), and overlapping
-- versions would match an order twice and make the aggregate bigger than the fact table.
select a.orders_in_agg, f.orders_in_fact
from (select sum(orders) as orders_in_agg from {{ ref('agg_revenue_minute') }}) a,
     (select count(*)   as orders_in_fact from {{ ref('fact_orders') }}) f
where a.orders_in_agg <> f.orders_in_fact
