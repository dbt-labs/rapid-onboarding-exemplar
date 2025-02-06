with payments as (

    select * from {{ ref('stg_stripe__payments') }}
),

aggregated as (

    select
    order_id,
    {% for payment_method in ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] %}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount
        {%- if not loop.last -%},{% endif %}
    {% endfor %}
    from payments
    group by 1

)

select * from aggregated

