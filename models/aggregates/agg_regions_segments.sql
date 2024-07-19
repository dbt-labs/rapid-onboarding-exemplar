with

customers as (
    select * from {{ ref('dim_customers') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

agg_orders as (
    select
        customer_id,
        sum(net_item_sales_amount) as total_sales,
        sum(net_item_sales_amount) + 1 as sales_plus_one
    from orders
    group by 1
),

final as (
    select
        customers.region,
        customers.market_segment,
        round(sum(agg_orders.total_sales),2) as total_sales,
        sales_plus_one
    from customers
    left join agg_orders on customers.customer_id = agg_orders.customer_id
    group by 1, 2, 4
)

select * from final