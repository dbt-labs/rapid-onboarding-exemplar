{# parameterize the following variables #}
{% set segment = 'BUILDING' %}
{% set date = '1995-03-15' %}

with

customers as (

    select * from {{ ref('stg_tpch_customers') }}

),

orders as (

    select * from {{ ref('stg_tpch_orders')}}

),

line_items as (

    select * from {{ ref('stg_tpch_line_items') }}

),

final as (

select
    line_items.order_id,
    sum(line_items.extended_price*(1-line_items.discount)) as revenue,
    orders.order_date,
    orders.ship_priority
from
    customers,
    orders,
    line_items
where
    customers.market_segment = '{{ segment }}'
    and customers.customer_id = orders.customer_id
    and line_items.order_id = orders.order_id
    and orders.order_date < to_date('{{ date }}')
    and line_items.ship_date > to_date('{{ date }}')
group by
    line_items.order_id,
    orders.order_date,
    orders.ship_priority
order by
    revenue desc,
    orders.order_date
)

select * from final
