{{
    config(
        enabled=true,
        severity='error',
        tags = ['finance']
    )
}}

with orders as ( select * from {{ ref('stg_tpch_orders') }} )

select *
from   orders 
where  total_price < 0