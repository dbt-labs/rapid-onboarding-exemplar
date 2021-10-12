with source as (

    select * from {{ source('tpch', 'supplier') }}

),

renamed as (

    select

        -- ids
        s_suppkey as supplier_id,
        s_nationkey as nation_id,

        -- descriptions
        s_name as name,
        s_address as address,
        s_phone as phone,
        s_comment as comment,

        -- amounts
        s_acctbal as account_balance

    from source

)

select * from renamed