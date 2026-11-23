-- One row per current order, with its payment and item counts.
-- Revenue is net of refunds: an order counts as revenue once paid (paid, shipped, delivered,
-- refunded); a refunded order gives it back. Cancelled / still-created orders are 0.
with o as (select * from {{ ref('stg_orders') }}),
     p as (select * from {{ ref('stg_payments') }}),
     i as (
        select order_id, count(*) as item_count, sum(qty) as units
        from {{ ref('stg_order_items') }}
        group by order_id
     ),
     j as (
        select
            o.order_id,
            o.user_id,
            o.status,
            o.total,
            o.coupon_code,
            o.created_at,
            date_trunc('minute', o.created_at)                         as order_minute,
            o.updated_at,
            p.payment_id,
            p.method                                                   as payment_method,
            p.status                                                   as payment_status,
            o.status in ('paid', 'shipped', 'delivered', 'refunded')   as is_paid,
            o.status = 'refunded'                                      as is_refunded,
            coalesce(i.item_count, 0)                                  as item_count,
            coalesce(i.units, 0)                                       as units
        from o
        left join p on p.order_id = o.order_id
        left join i on i.order_id = o.order_id
     )
select
    *,
    case when is_paid then total else 0 end                               as gross_revenue,
    case when is_refunded then total else 0 end                           as refund_amount,
    case when is_paid and not is_refunded then total else 0 end           as net_revenue,
    -- `refunded` is terminal in the source app, so updated_at of a refunded order is the refund time
    case when is_refunded then updated_at end                             as refunded_at
from j
