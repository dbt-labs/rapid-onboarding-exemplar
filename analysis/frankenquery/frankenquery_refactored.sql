WITH

orders_transformed as (

    -- staging model:  jaffle_shop/stg_orders
    select 
        id as order_id,
        user_id as customer_id,
        order_date,
        status,
        row_number() over (partition by user_id order by order_date, id) as user_order_seq,
        status not in ('returned','return_pending') as is_not_return
    from raw.jaffle_shop.orders
  
    where orders.status NOT IN ('pending') 
),

customers_transformed as ( 

    -- staging model:  jaffle_shop/stg_customers
    select 
        id as customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name 
    from raw.jaffle_shop.customers
),

payments_transformed as (

    -- staging model:  stripe/base/base_payments
    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as method,
        status,
        round(amount/100.0,2) as amount,
        created as payment_date
    from raw.stripe.payment
  
),

successful_payments as (
 
    -- staging model:  stripe/stg_successful_payments
    select *
    from payments_transformed 
    where status not in ('fail')
  
),

customer_facts as (
  
    -- mart:  core/dim_customers
  
    select 
        customers.customer_id,
        customers.full_name,
        customers.last_name,
        customers.first_name,

        min(orders.order_date) 
            as first_order_date,

        min(case when orders.is_not_return then order_date end) 
            as first_non_returned_order_date,

        max(case when orders.is_not_return then order_date end)
            as most_recent_non_returned_order_date,

        coalesce(max(user_order_seq),0) 
            as order_count,
  
        coalesce(count(case when orders.is_not_return then 1 end),0) 
            as non_returned_order_count,
  
        sum(case when orders.is_not_return then payments.amount else 0 end) 
            as total_lifetime_value,
  
        sum(case when orders.is_not_return then payments.amount else 0 end)
        /
        nullif(count(case when orders.is_not_return then 1 end),0) 
            as avg_non_returned_order_value,

        array_agg(distinct orders.order_id) as order_ids

    from orders_transformed as orders

    inner join customers_transformed as customers
        on orders.customer_id = customers.customer_id

    left outer join successful_payments as payments
        on payments.order_id = payments.order_id

    group by 1, 2, 3, 4
  
),

orders_with_detail as (
  
    -- mart:  sales/fct_orders
    
    select 
        orders.order_id,
        orders.customer_id,
        orders.status as order_status,

        customer_facts.last_name,
        customer_facts.first_name,
        customer_facts.first_order_date,
        customer_facts.order_count,
        customer_facts.total_lifetime_value,

        payments.amount as order_value_dollars

    from orders_transformed as orders
  
    inner join customer_facts
        on orders.customer_id = customer_facts.customer_id

    left outer join successful_payments as payments
        on orders.order_id = payments.order_id

)

select * from orders_with_detail