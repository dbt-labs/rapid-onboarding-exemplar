
{%- set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card' ] -%}

with payments as (

    select * from {{ ref('stg_stripe__payments') }}
),

-- strategy, make and iterate over a list of all the valid payment_methods
-- to eliminate all the copy and paste actions

pivoted as (
    select 
        order_id,

        

        -- Strategy, when not loop.last, add a comma | when loop.last is true, do not add a comma

        {% for payment_method in payment_methods -%}
          sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount
          {%- if not loop.last -%}
            ,
          {%- endif %}   
        {% endfor -%}      
    
    from payments
    where status = 'success'
    group by 1

)

select * from pivoted
