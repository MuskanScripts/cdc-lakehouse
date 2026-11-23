-- current payments, one per order (the latest one if there were ever several)
select payment_id, order_id, method, status, amount,
       cast(updated_at as timestamp) as updated_at
from {{ source('silver', 'payments') }}
where not _is_deleted
qualify row_number() over (partition by order_id order by updated_at desc, payment_id desc) = 1
