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

        order_id, 
        order_date,
        customer_id,
        order_status_code,
        priority_code,
        clerk_name,
        ship_priority

    from order_item
    {{ dbt_utils.group_by(n = 8) }}

)

select * from final
order by order_date