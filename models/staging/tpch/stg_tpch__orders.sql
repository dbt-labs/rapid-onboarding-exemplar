{{
    config(
        materialized='incremental',
        unique_key='ORDER_ID',
        merge_update_columns = ['CUSTOMER_ID', 'COMMENT','CLERK_NAME','TOTAL_PRICE','ORDER_STATUS_CODE','PRIORITY_CODE']
    )
}}

with source as (

    select * from {{ source('tpch', 'orders') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where o_orderdate > (select max(order_date) from {{ this }})
        
    {% endif %}

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

        current_timestamp() as created_at,
        current_timestamp() as updated_at

    from source

)

select * from renamed