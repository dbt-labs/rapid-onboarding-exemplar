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
    -- and r_name = '[REGION]'
    -- and o_orderdate >= date '[DATE]'
    -- and o_orderdate < date '[DATE]' + interval '1' year
group by
    n_name
order by
    revenue desc