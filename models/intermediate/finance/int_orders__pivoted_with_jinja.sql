-- place at the top for maintainability
{%- set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card' ] -%}

with payments as (

    select * from {{ ref('stg_stripe__payments') }}
),

-- strategy, make and iterate over a list of all the valid payment_methods
-- to eliminate all the copy and paste actions

pivoted as (
    select 
        order_id,

        

        -- First pass, but produces an errror due to the comma on the last run through the loop
        -- Trouble getting Jinja to write the SQL I want

        --{% for payment_method in payment_methods %}
        --    sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        --{% endfor %}

        -- Solution identify when we are at the last iteration of a loop
        -- Jinja docs https://jinja.palletsprojects.com/en/3.1.x/templates/#for 

        -- New strategy, when not loop.last, add a comma | when loop.last is true, do not add a comma

        {% for payment_method in payment_methods -%}
          sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount
          {%- if not loop.last -%}
            ,
          {%- endif %}   
        {% endfor -%}      

        -- sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
        -- sum(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount,
        -- sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
        -- sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount
    
    from payments
    where status = 'success'
    group by 1

)

select * from pivoted
