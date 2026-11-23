-- Orders older than 5 minutes must resolve to a city version (no 'unknown'): proves the SCD2
-- join on [valid_from, valid_to) has no gaps. The 5 minute grace is for a new user whose silver
-- row lands a batch after their first order (see the relationships test).
select f.order_id, f.user_id, f.created_at
from {{ ref('fact_orders') }} f
left join {{ ref('dim_user') }} u
       on u.user_id = f.user_id and f.created_at >= u.valid_from and f.created_at < u.valid_to
where u.user_id is null
  and f.created_at < now()::timestamp - interval 5 minute
