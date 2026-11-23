{% snapshot users_snapshot %}
{#
  SCD Type 2 over silver users: a new version whenever updated_at moves (city or tier change).
  A user deleted in Postgres disappears from stg_users, and the snapshot closes its last version.
  Granularity = how often the snapshot runs (every build, ~30-60 s): two changes to one user
  inside one interval keep only the second. See ADR-025.
#}
{{ config(
    target_schema='snapshots',
    unique_key='user_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True,
) }}
select user_id, name, email, city, tier, created_at, updated_at
from {{ ref('stg_users') }}
{% endsnapshot %}
