select 
    * exclude priority_code,
    order_count + 1 as order_count_plus_one
from {{ ref('fct_orders') }}
