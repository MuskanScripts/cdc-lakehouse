-- One row per user version. valid_from / valid_to are half-open: [valid_from, valid_to).
-- The FIRST version of each user starts at 1900-01-01: the snapshot only began observing users
-- when it first ran, and orders placed before that still need a city (the earliest one we know).
with versions as (
    select *, row_number() over (partition by user_id order by dbt_valid_from) as version
    from {{ ref('users_snapshot') }}
)
select
    dbt_scd_id                                   as user_version_key,
    user_id,
    version,
    name,
    email,
    city,
    tier,
    case when version = 1 then timestamp '1900-01-01' else dbt_valid_from end as valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31')                              as valid_to,
    dbt_valid_to is null                                                        as is_current
from versions
