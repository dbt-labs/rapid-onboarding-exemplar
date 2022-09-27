with line_item as (

    select * from {{ ref('stg_tpch__line_items') }}

)
select

    order_item_id,
    order_id,
    part_id,
    supplier_id,

    return_flag,
    line_number,
    order_item_status_code,
    ship_date,
    commit_date,
    receipt_date,
    ship_mode,
    extended_price,
    quantity,

    -- extended_price is actually the line item total,
    -- so we back out the extended price per item
    (extended_price/nullif(quantity, 0)){{ money() }} as base_price,
    discount_percentage,
    (base_price * (1 - discount_percentage)){{ money() }} as discounted_price,

    extended_price as gross_item_sales_amount,
    (extended_price * (1 - discount_percentage)){{ money() }} as discounted_item_sales_amount,
    -- We model discounts as negative amounts
    (-1 * extended_price * discount_percentage){{ money() }} as item_discount_amount,
    tax_rate,
    ((gross_item_sales_amount + item_discount_amount) * tax_rate){{ money() }} as item_tax_amount,
    (
        gross_item_sales_amount +
        item_discount_amount +
        item_tax_amount
    ){{ money() }} as net_item_sales_amount

from
    line_item
