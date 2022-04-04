{{
    config(
        transient = false
    )
}}

with 

customer as (

    select * from {{ ref('stg_tpch_customers') }}

),

locations as (

    select * from {{ ref('int_locations') }}

),

final as (

    select 
        
        customer.customer_id,
        customer.name || ' test' as name,
        customer.address,
        locations.nation_name as nation,
        locations.region_name as region,
        customer.phone_number,
        customer.account_balance,
        customer.market_segment
    
    from customer    
    inner join locations on customer.nation_id = locations.nation_id

)

select * from final 
order by customer_id