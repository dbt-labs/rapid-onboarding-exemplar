with 

some_orders as (
    select * from {{ ref('agg_customer_orders') }}
)



select * from some_orders
