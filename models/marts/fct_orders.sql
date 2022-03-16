{{
    config(
        tags = ['finance']
    )
}}

with order_item as (
    
    select * from {{ ref('order_items') }}

),
order_item_summary as (

    select 
        order_id,
        sum(gross_item_sales_amount) as gross_item_sales_amount,
        sum(item_discount_amount) as item_discount_amount,
        sum(item_tax_amount) as item_tax_amount,
        sum(net_item_sales_amount) as net_item_sales_amount
    from order_item
    group by
        1
),
final as (

    select 

        order_item.order_id, 
        order_item.order_date,
        order_item.customer_id,
        order_item.order_status_code,
        order_item.priority_code,
        order_item.clerk_name,
        order_item.ship_priority,
                
        1 as order_count,                
        order_item_summary.gross_item_sales_amount,
        order_item_summary.item_discount_amount,
        order_item_summary.item_tax_amount,
        order_item_summary.net_item_sales_amount
    from
        order_item
    inner join order_item_summary
            on order_item.order_id = order_item_summary.order_id
)
select 
    *
from
    final

order by
    order_date