{# parameterize the following variables #}
{% set date = '1993-07-01' %}

select
    o_orderpriority,
    count(*) as order_count
from
    snowflake_sample_data.tpch_sf1.orders
where
    o_orderdate >= date '{{ date }}'
    and o_orderdate < dateadd(month, 3, to_date('{{ date }}')) 
    and exists (
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