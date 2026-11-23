-- Every user that exists now has EXACTLY one is_current row; a deleted user has none.
-- Returns the offending users (dbt fails the test when any row comes back).
with cur as (
    select user_id, count(*) filter (where is_current) as current_rows
    from {{ ref('dim_user') }}
    group by user_id
),
live as (select user_id from {{ ref('stg_users') }})
select coalesce(c.user_id, l.user_id) as user_id,
       coalesce(c.current_rows, 0)    as current_rows,
       l.user_id is not null          as exists_now
from cur c
full outer join live l on l.user_id = c.user_id
where (l.user_id is not null and coalesce(c.current_rows, 0) <> 1)
   or (l.user_id is null and coalesce(c.current_rows, 0) <> 0)
