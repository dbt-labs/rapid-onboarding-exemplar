{{
    config(
        materialized='incremental',
        post_hook = ['delete from orders where order_date < current_date - interval 1 year']
    )
}}
   select
       r.order_id,
       r.customer_id,
       r.order_date
   from
       {{ ref('stg_jaffle_shop__orders') }} r
   join
       {{ ref('stg_jaffle_shop__customers') }} c
   on
       r.customer_id = c.customer_id
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where r.order_date > (select date_add('day', -7, max(order_date)) from {{ this }}) 
    {% endif %}


   -- step 2: update existing rows in the main table
   update orders
   set
       customer_status = s.customer_status
   from
       stg_new_orders s
   where
       orders.order_id = s.order_id;
