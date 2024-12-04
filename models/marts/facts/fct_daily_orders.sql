with

int_daily_orders_fact as (

    select * from {{ ref('int_daily_orders_fact') }}

),

agg_order_quantity as (

    select
    order_sk,
    order_date_sk,
    order_country_origin_sk,
    sum(order_quantity) as total_order_quantity
    from int_daily_orders_fact
    group by
        order_sk,
        order_date_sk,
        order_country_origin_sk

)

select * from agg_order_quantity

