with part as (

    select * from {{ref('stg_tpch__parts')}}

),

final as (
    select
        part_id as part_id_number,
        manufacturer,
        name,
        brand,
        type,
        size,
        container,
        retail_price
    from
        part
)
select *
from final
order by part_id_number
