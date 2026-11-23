select order_id, product_id, qty, price
from {{ source('silver', 'order_items') }}
where not _is_deleted
