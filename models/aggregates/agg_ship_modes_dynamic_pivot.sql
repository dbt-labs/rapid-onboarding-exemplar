/* Create a pivot table with dynamic columns based on the ship modes that are in the system */

{%- call statement('result', fetch_result=True) -%}

    {# this pulls the unique ship modes from the fct_order_items table #}
    select ship_mode from {{ ref('fct_order_items') }} group by 1 

{%- endcall %}

{% set ship_modes = load_result('result').table.columns[0].values() %}

select
    date_part('year', order_date) as order_year,

    {# Loop over ship_modes array from above, and sum based on whether the record matches the ship mode #}
    {%- for ship_mode in ship_modes -%}
        sum(case when ship_mode = '{{ship_mode}}' then gross_item_sales_amount end) as "{{ship_mode|replace(' ', '_')}}_amount"
        {%- if not loop.last -%},{% endif %}
    {% endfor %}

from {{ ref('fct_order_items') }}
group by 1

-- here is a comment