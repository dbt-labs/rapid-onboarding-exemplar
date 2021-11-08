with

customers as (
    select * from {{ ref('dim_customers') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

aggregated_orders as (
    select
        customer_id,
        sum(net_item_sales_amount) as total_sales
    from orders
    group by 1
),

final as (
    select
        customers.region,
        customers.market_segment,
        sum(aggregated_orders.total_sales) as total_sales
    from customers
    left join aggregated_orders on customers.customer_id = aggregated_orders.customer_id
    group by 1, 2
)

select * from final