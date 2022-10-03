with source as (

    select * from {{ source('tpch', 'part') }}

),

renamed as (

    select

        -- ids
        p_partkey as part_id,
        
        -- descriptions
        p_name as name,
        p_type as type,
        p_size as size,
        p_mfgr as manufacturer,
        p_brand as brand,
        p_comment as comment,
        p_container as container,
        true as new_col,

        -- amounts
        p_retailprice as retail_price
        

    from source

)

select * from renamed
