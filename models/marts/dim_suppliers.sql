{{
    config(
        materialized = 'table'
    )
}}

with supplier as (

    select * from {{ ref('stg_tpch_suppliers') }}

),
nation as (

    select * from {{ ref('stg_tpch_nations') }}
),
region as (

    select * from {{ ref('stg_tpch_regions') }}

),
final as (

    select 
        supplier.supplier_id,
        supplier.supplier_name,
        supplier.supplier_address,
        nation.name as nation,
        region.name as region,
        supplier.phone_number,
        supplier.account_balance
    from
        supplier
    inner join nation
            on supplier.nation_id = nation.nation_id
    inner join region 
            on nation.region_id = region.region_id
)

select * from final