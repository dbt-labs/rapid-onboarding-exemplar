with 
customers as (
    select * from {{ source('jaffle_shop', 'customers')}}
),

renamed as (

    select 
        first_name || ' ' || last_name as name, 
        id as customer_id,
        first_name,
        last_name
    from customers
)

select * from renamed
      