
select * from {{ ref('stg_jaffle_shop_orders')}} as orders

join (select 
        customer_id as cust_id,  
        name, 
        first_name,
        last_name 
      from {{ ref('stg_jaffle_shop_customers') }}
        ) as customers
on orders.user_id = customers.cust_id

join (

    select 
        b.customer_id,
        b.name as full_name,
        b.last_name as surname,
        b.first_name as givenname,
        min(order_date) as first_order_date,
        min(case when a.order_status NOT IN ('returned','return_pending') then order_date end) as first_non_returned_order_date,
        max(case when a.order_status NOT IN ('returned','return_pending') then order_date end) as most_recent_non_returned_order_date,
        COALESCE(max(user_order_seq),0) as order_count,
        COALESCE(count(case when a.order_status != 'returned' then 1 end),0) as non_returned_order_count,
        sum(case when a.order_status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end) as total_lifetime_value,
        sum(case when a.order_status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end)/NULLIF(count(case when a.order_status NOT IN ('returned','return_pending') then 1 end),0) as avg_non_returned_order_value,
        array_agg(distinct a.order_id) as order_ids

    from (
      select 
        row_number() over (partition by user_id order by order_date, order_id) as user_order_seq,
        *
      from {{ ref('stg_jaffle_shop_orders') }}
    ) a

    join ( 
      select 
        *
      from {{ ref('stg_jaffle_shop_customers') }}
    ) b
    on a.user_id = b.customer_id

    left outer join raw.stripe.payment c
    on a.user_id = c.orderid

    where a.order_status NOT IN ('pending') and c.status != 'fail'

    group by b.customer_id, b.name, b.last_name, b.first_name

) customer_order_history
on orders.user_id = customer_order_history.customer_id

left outer join raw.stripe.payment payments
on orders.order_id = payments.orderid

where payments.status != 'fail'