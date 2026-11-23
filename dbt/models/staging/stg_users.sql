-- current users (soft-deleted tombstones removed)
select user_id, name, email, city, tier,
       cast(created_at as timestamp) as created_at,
       cast(updated_at as timestamp) as updated_at
from {{ source('silver', 'users') }}
where not _is_deleted
