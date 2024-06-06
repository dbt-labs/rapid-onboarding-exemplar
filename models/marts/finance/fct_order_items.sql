{{
    config(
        tags = ['finance']
    )
}}

with order_item as (
    
    select * from {{ ref('int_order_items_joined') }}

),
part_supplier as (
    
    select * from {{ ref('int_part_suppliers_joined') }}

),
final as (
    select 
        order_item.order_item_id,
        order_item.order_id,
        order_item.order_date,
        order_item.customer_id,
        order_item.part_id,
        order_item.supplier_id,
        order_item.order_item_status_code,
        order_item.return_flag,
        order_item.line_number,
        order_item.ship_date,
        order_item.commit_date,
        order_item.receipt_date,
        order_item.ship_mode,
        part_supplier.cost as supplier_cost,
        {# ps.retail_price, #}
        order_item.base_price,
        order_item.discount_percentage,
        order_item.discounted_price,
        order_item.tax_rate,


        
        1 as order_item_count,
        order_item.quantity,
        order_item.gross_item_sales_amount,
        order_item.discounted_item_sales_amount,
        order_item.item_discount_amount,
        order_item.item_tax_amount,
        order_item.net_item_sales_amount

    from
        order_item
        inner join part_supplier
            on order_item.part_id = part_supplier.part_id and
                order_item.supplier_id = part_supplier.supplier_id
)
select 
    *
from
    final
order by
    order_date
    