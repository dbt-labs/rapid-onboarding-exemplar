with

dates as (

    select * from {{ ref('dim_dates') }}

),

orders as (

    select * from {{ ref('dim_orders') }}

),

countries as (

    select * from {{ ref('dim_countries') }}

),

join_sources as (

    select
        ordr.order_sk,
        country.country_sk as order_country_origin_sk,
        date.date_sk as order_date_sk,
        ordr.order_quantity
    from orders ordr
    left join dates date
        on ordr.order_date = date.calendar_date
    left join countries country
        on ordr.order_country_origin = country.country_name

)

select * from join_sources
