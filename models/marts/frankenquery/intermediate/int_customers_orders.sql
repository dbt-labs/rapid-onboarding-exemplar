
with orders as (
  select * from {{ ref('stg_jaffle_shop_orders')}}
),

customers as (
  select * from {{ ref('stg_jaffle_shop_customers') }}
),

joined as (
    select
        customers.customer_id,  
        customers.name, 
        customers.first_name,
        customers.last_name,
        orders.order_id,
        orders.user_id,
        orders.order_date,
        orders.order_status,
        row_number() over (partition by orders.user_id order by orders.order_date, orders.order_id) as user_order_seq
    from orders
    left join customers 
        on orders.user_id = customers.customer_id
)

select * from joined
