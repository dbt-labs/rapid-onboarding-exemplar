select 
    order_id,
    order_date as order_time,
    customer_id,
    order_status_code,
    priority_code,
    clerk_name,
    ship_priority,
    order_count,
    gross_item_sales_amount,
    item_discount_amount,
    item_tax_amount,
    net_item_sales_amount
from {{ ref('fct_orders') }}
