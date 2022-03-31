with part as (

    select * from {{ref('stg_tpch_parts')}}
    where 1=1

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
        retail_price
    from
        part
)
select *
from final  
order by part_id