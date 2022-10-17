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
        o_orderdate as order_date,

        sum(total_price) over (partition by customer_id order by order_date rows between unbounded preceding and current row) as running_customer_lifetime_value,
        count(order_id) over (partition by customer_id) as total_customer_order_count,
        row_number() over (partition by customer_id order by order_date) as customer_order_sequence

    from source

)

select * from renamed