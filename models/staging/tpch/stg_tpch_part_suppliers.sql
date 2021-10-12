with source as (

    select * from {{ source('tpch', 'partsupp') }}

),

renamed as (

    select

        -- ids
        ps_partkey as part_id,
        ps_suppkey as supplier_id,

        -- descriptions
        ps_comment as comment,

        -- amounts
        ps_availqty as available_quantity,
        ps_supplycost as supply_cost
        
    from source

)

select * from renamed