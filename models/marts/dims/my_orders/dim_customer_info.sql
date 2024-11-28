with

dim_orders as (

    select * from {{ ref('dim_orders') }}

),

final as (

    select 
        cust_lname || ', ' || cust_fname as cust_fullname,
    from dim_orders
)

select * from final
