{{
    config(
        enabled=true,
        severity='error',
        error_if = '>50',
        warn_if = '>10',
        tags = ['finance']
    )
}}

with orders as (
    select * from {{ ref('stg_tpch__orders') }}
)

select *
from orders
where total_price < 0
