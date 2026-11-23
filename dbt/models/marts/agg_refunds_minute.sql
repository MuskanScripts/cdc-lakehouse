-- Refunds by the minute they were ISSUED (not the minute the order was placed).
-- The simulator refunds any delivered order, often hours old, so a refund rate by order minute
-- stays near 0 for recent minutes. This one moves with the business: refunds issued per minute
-- against paid orders placed per minute. See ADR-026.
select
    date_trunc('minute', refunded_at)  as minute,
    count(*)                           as refunds,
    sum(refund_amount)                 as refund_amount
from {{ ref('fact_orders') }}
where is_refunded
group by 1
