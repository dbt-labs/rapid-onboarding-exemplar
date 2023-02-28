with source as (

    select * from {{ source('tpch', 'customer') }}

),

renamed as (

    select
        -- ids
        c_custkey as customer_id,
        c_nationkey as nation,

        -- descriptions
        c_name as name_update,
        c_address as address,
        c_phone as phone_number,
        c_acctbal as account_balance,
        c_mktsegment as market_segment,
        c_comment as comment

    from source

)

select * from renamed