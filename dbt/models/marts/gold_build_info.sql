-- When this gold build ran and how fresh its input was: the dashboard's "gold age" panel.
select
    now()::timestamp                                   as built_at,
    (select max(updated_at) from {{ ref('fact_orders') }}) as max_order_updated_at,
    (select count(*) from {{ ref('fact_orders') }})        as orders
