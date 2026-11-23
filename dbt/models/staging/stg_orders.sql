-- current orders. coupon_code arrived with the Week 3 schema drift (NULL for older orders)
select order_id, user_id, status, total, coupon_code,
       cast(created_at as timestamp) as created_at,
       cast(updated_at as timestamp) as updated_at
from {{ source('silver', 'orders') }}
where not _is_deleted
