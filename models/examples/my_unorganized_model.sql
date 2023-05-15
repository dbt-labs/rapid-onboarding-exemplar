
select *
from {{ source('jaffle_shop', 'customers') }} customers 
join {{ source('jaffle_shop', 'orders') }} orders 
on customers.id = orders.user_id
