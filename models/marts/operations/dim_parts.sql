with part as (

<<<<<<< HEAD:models/marts/core/dim_parts.sql
    select * from {{ref('stg_tpch_parts')}}
    where 1=1
=======
    select * from {{ref('stg_tpch__parts')}}
>>>>>>> e0f87f3617f1b3d303cacd3229145f095fa65600:models/marts/operations/dim_parts.sql

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
