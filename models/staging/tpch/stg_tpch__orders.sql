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
        o_clerk as clerk_name,

        -- numbers
        o_totalprice as total_price,

        -- statuses
        o_orderstatus as order_status_code,
        o_orderpriority as priority_code,
        o_shippriority as ship_priority,

        -- dates
        o_orderdate as order_date

    from source

)

select * from renamed

{{ limit_data_in_dev('order_date', 10)}}

