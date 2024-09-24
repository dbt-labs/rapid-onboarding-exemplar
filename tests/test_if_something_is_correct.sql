select
* 
from {{ ref('stg_customers') }}
where customer_id is null