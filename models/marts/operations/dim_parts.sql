with part as (

    select * from {{ref('stg_tpch__parts')}}

),

final as (
    select
        part_id,
        manufacturer,
        name,
        brand,
        type,
        size,
        container,
        retail_price,
        retail_price + 30 as inflation_price
    from
        part
)
select *
from final
order by part_id
