select
  customer_id,
  sum(gross_item_sales_amount) as total_sales
from {{ ref('fct_orders') }}
group by 1 
order by 2 desc