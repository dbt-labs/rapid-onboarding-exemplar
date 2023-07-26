select 
    * exclude priority_code
from {{ ref('fct_orders') }}
