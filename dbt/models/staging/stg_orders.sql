-- current orders. coupon_code is added to the live table by the Week 3 `make drift` drill, so a
-- fresh clone's silver doesn't have it (Week 6 fresh-clone test: every build failed with
-- "Referenced column coupon_code not found"). UNION ALL BY NAME with an empty, typed row makes the
-- column exist either way: NULL until the drift happens, real values after it.
with src as (
    select * from {{ source('silver', 'orders') }}
    union all by name
    select null::varchar as coupon_code where false
)
select order_id, user_id, status, total, coupon_code,
       cast(created_at as timestamp) as created_at,
       cast(updated_at as timestamp) as updated_at
from src
where not _is_deleted
