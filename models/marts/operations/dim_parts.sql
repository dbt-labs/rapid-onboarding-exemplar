{{
    config(
        materialized='view'
    )
}}

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
        retail_price
    from
        part
)
select *
from final
order by part_id
