with source as (

    select * from {{ source('tpch', 'lineitem') }}

),

renamed as (

    select

        -- ids
        l_orderkey as order_id,
        l_partkey as part_id,
        l_suppkey as part_supplies_id,

        -- descriptions
        l_linenumber as line_numbers,
        l_comment as comment,
        l_shipmode as ship_mode,
        l_shipinstruct as ship_instructions,
        
        -- numbers
        l_quantity as quantity,
        l_extendedprice as price,
        l_discount as discount,
        l_tax as tax,
        
        -- status
        l_linestatus as line_status,
        l_returnflag as return_flag,
        
        -- dates
        l_shipdate as ship_date,
        l_commitdate as commit_date,
        l_receiptdate as receipt_date

    from source

)

select * from renamed