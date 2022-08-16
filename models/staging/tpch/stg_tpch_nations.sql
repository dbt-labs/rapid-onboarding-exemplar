with source as (

    select * from {{ source('tpch', 'nation') }}

),

renamed as (

    select

        -- ids
        n_nationkey as nation_id,
        n_regionkey as region_id,
        'this is a nation' as new_col,

        -- descriptions
        n_name as name,
        n_comment as comment

    from source

)

select * from renamed