{{
    config(
        enabled=true,
        severity='error'
    )
}}

with orders as (
    select * from {{ ref('stg_tpch__orders') }}
)

select *
from orders
where total_price < 0

