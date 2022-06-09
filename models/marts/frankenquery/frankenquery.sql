with

customers_orders as (
  select * from {{ ref('int_customers_orders') }}
),

payments as (
  select * from {{ ref('stg_stripe_payments') }}
),

joined as (

  select 
        customer_id,
        name as full_name,
        last_name as surname,
        first_name as givenname,
        min(order_date) as first_order_date,
        min(case when order_status NOT IN ('returned','return_pending') then order_date end) as first_non_returned_order_date,
        max(case when order_status NOT IN ('returned','return_pending') then order_date end) as most_recent_non_returned_order_date,
        COALESCE(max(user_order_seq),0) as order_count,
        COALESCE(count(case when order_status != 'returned' then 1 end),0) as non_returned_order_count,
        sum(case when order_status NOT IN ('returned','return_pending') then ROUND(payments.amount/100.0,2) else 0 end) as total_lifetime_value,
        sum(case when order_status NOT IN ('returned','return_pending') then ROUND(payments.amount/100.0,2) else 0 end)/NULLIF(count(case when order_status NOT IN ('returned','return_pending') then 1 end),0) as avg_non_returned_order_value,
        array_agg(distinct order_id) as order_ids
    
  from customers_orders
    left outer join payments
      on customers_orders.order_id = payments.orderid
      where customers_orders.order_status NOT IN ('pending') and payments.status != 'fail'
      group by customers_orders.customer_id, customers_orders.name, customers_orders.last_name, customers_orders.first_name
)

select * from joined