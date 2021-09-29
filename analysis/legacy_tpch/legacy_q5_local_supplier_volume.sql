{# parameterize the following variables #}
{% set region = 'ASIA' %}
{% set date = '1994-01-01' %}


select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    snowflake_sample_data.tpch_sf1.customer,
    snowflake_sample_data.tpch_sf1.orders,
    snowflake_sample_data.tpch_sf1.lineitem,
    snowflake_sample_data.tpch_sf1.supplier,
    snowflake_sample_data.tpch_sf1.nation,
    snowflake_sample_data.tpch_sf1.region
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = '{{ region }}'
    and o_orderdate >= to_date('{{ date }}')
    and o_orderdate < dateadd(year, 1, to_date('{{ date }}')) 
group by
    n_name
order by
    revenue desc