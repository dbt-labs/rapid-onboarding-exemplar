with source as (

    select * from {{ source('tpch', 'orders') }}

),

renamed as (

    select

        -- ids
        o_orderkey as order_id,
        o_custkey as customer_id,
        
        -- descriptions
        o_comment as comment,
        o_clerk as clerk,

        -- numbers
        o_totalprice as total_price,

        -- statuses
        o_orderstatus as order_status,
        o_orderpriority as order_priority,
        o_shippriority as ship_priority,

        -- dates
        o_orderdate as order_date

    from source

)

select * from renamed