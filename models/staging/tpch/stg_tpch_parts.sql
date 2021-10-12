with source as (

    select * from {{ source('tpch', 'part') }}

),

renamed as (

    select

        -- ids
        p_partkey as part_id,
        
        -- descriptions
        p_name as name,
        p_type as part_type,
        p_size as part_size,
        p_mfgr as manufacturer,
        p_brand as brand,
        p_comment as comment,
        p_container as container,

        -- amounts
        p_retailprice as price
        

    from source

)

select * from renamed