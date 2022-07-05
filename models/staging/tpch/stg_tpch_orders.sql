with source as (

    select * from {{ source('tpch', 'orders') }}

),

renamed as (

    select

    from source

)

select * from renamed