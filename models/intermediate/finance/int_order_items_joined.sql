with orders as (

    select * from {{ ref('stg_tpch__orders') }}

),

line_item as (

    select * from {{ ref('int_line_items_amounts_calculated') }}

)
select

    line_item.order_item_id,
    orders.order_id,
    orders.customer_id,
    line_item.part_id,
    line_item.supplier_id,

    orders.order_date,
    orders.order_status_code,
    orders.priority_code,
    orders.clerk_name,
    orders.ship_priority,

    line_item.return_flag,
    line_item.line_number,
    line_item.order_item_status_code,
    line_item.ship_date,
    line_item.commit_date,
    line_item.receipt_date,
    line_item.ship_mode,
    line_item.extended_price,
    line_item.quantity,

    line_item.base_price,
    line_item.discount_percentage,
    line_item.discounted_price,
    line_item.gross_item_sales_amount,
    line_item.discounted_item_sales_amount,
    line_item.item_discount_amount,
    line_item.tax_rate,
    line_item.item_tax_amount,
    line_item.net_item_sales_amount

from
    orders
inner join line_item
        on orders.order_id = line_item.order_id
order by
    orders.order_date
