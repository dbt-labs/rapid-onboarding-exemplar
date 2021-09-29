{# parameterize the following variables #}
{% set segment = 'BUILDING' %}
{% set date = '1995-03-15' %}

select
    l_orderkey,
    sum(l_extendedprice*(1-l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    snowflake_sample_data.tpch_sf1.customer,
    snowflake_sample_data.tpch_sf1.orders,
    snowflake_sample_data.tpch_sf1.lineitem
where
    c_mktsegment = '{{ segment }}'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < to_date('{{ date }}')
    and l_shipdate > to_date('{{ date }}')
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
