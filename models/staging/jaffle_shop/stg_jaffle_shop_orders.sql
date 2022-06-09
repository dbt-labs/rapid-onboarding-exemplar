with orders as (
    select * from {{ source('jaffle_shop','orders') }}
),

renamed as (
    select 
        orders.id as order_id,
        orders.user_id,
        order_date,
        orders.status as order_status
    from orders
)

select * from renamed