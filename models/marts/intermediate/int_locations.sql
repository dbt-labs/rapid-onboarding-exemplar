with

nations as (

    select * from {{ ref('stg_tpch_nations') }}

),

regions as (

    select * from {{ ref('stg_tpch_regions') }}

),

joined as (

    select 

        nations.nation_id,
        nations.region_id,
        nations.name as nation_name,
        nations.comment as nation_comment,

        -- descriptions
        regions.name as region_name,
        regions.comment as region_comment

    from nations 
    left join regions on nations.region_id = regions.region_id

)

select * from joined