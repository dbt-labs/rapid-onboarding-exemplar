
select
    customers.id as customer_id,
    orders.id as order_id,
    orders.status as order_status
from {{ source('jaffle_shop', 'customers') }} customers 
join {{ source('jaffle_shop', 'orders') }} orders 
on customers.id = orders.user_id
