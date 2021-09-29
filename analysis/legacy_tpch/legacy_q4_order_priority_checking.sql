select
    o_orderpriority,
    count(*) as order_count
from
    snowflake_sample_data.tpch_sf1.orders
where
    -- o_orderdate >= date '[DATE]'
    -- and o_orderdate < date '[DATE]' + interval '3' month
    exists (
        select
            *
        from
            snowflake_sample_data.tpch_sf1.lineitem
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority