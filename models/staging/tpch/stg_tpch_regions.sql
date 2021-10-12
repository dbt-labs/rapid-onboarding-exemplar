with source as (

    select * from {{ source('tpch', 'region') }}

),

renamed as (

    select

        -- ids
        r_regionkey as region_id,

        -- descriptions
        r_name as region_name,
        r_comment as comment

    from source

)

select * from renamed