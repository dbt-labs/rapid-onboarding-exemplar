with source as (

    select * from {{ source('tpch', 'customer') }}

),

renamed as (

    select
        
        -- dan testing surrogate key
        {{ dbt_utils.surrogate_key(['c_custkey', 'c_nationkey' ])}} as combo_key,

        -- ids
        c_custkey as customer_id,
        c_nationkey as nation_id,

        -- descriptions
        c_name as name,
        c_address as address,
        c_phone as phone_number,
        c_acctbal as account_balance,
        c_mktsegment as market_segment,
        c_comment as comment

    from source

)

select * from renamed