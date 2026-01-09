{{
    config(
        tags = ['finance']
    )
}}

with order_item as (
    
    select * from {{ ref('int_order_items_joined') }}

),

final as (

    select 
        1 as static_field_1,
        order_id, 
        order_date,
        customer_id,
        order_status_code,
        priority_code,
        clerk_name,
        ship_priority,
                
        1 as order_count,                
        sum(gross_item_sales_amount) as gross_item_sales_amount,
        sum(item_discount_amount) as item_discount_amount,
        sum(item_tax_amount) as item_tax_amount,
        sum(net_item_sales_amount) as net_item_sales_amount

    from order_item
    {{ dbt_utils.group_by(n = 8) }}

)

select * from final
order by order_date