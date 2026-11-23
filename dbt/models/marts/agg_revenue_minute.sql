-- Minute x city. The city is the user's city AT ORDER TIME (SCD2 join on valid_from/valid_to),
-- not their current city: a user who moved from Delhi to Pune keeps their old orders in Delhi.
select
    f.order_minute                                     as minute,
    coalesce(u.city, 'unknown')                        as city,
    count(*)                                           as orders,
    count(*) filter (where f.is_paid)                  as paid_orders,
    count(*) filter (where f.is_refunded)              as refunded_orders,
    sum(f.gross_revenue)                               as gross_revenue,
    sum(f.refund_amount)                               as refunds,
    sum(f.net_revenue)                                 as net_revenue
from {{ ref('fact_orders') }} f
left join {{ ref('dim_user') }} u
       on u.user_id = f.user_id
      and f.created_at >= u.valid_from
      and f.created_at <  u.valid_to
group by 1, 2
