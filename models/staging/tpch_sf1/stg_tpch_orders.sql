with source as (

    select * from {{ source('tpch', 'orders') }}

),

renamed as (

    select

        -- ids
        o_orderkey as order_id,
        o_custkey as customer_id,
        
        -- descriptions
        o_comment,
        o_clerk,

        -- numbers
        o_totalprice,

        -- statuses
        o_orderstatus,
        o_orderpriority,
        o_shippriority,

        -- dates
        o_orderdate

    from source

)

select * from renamed