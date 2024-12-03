{{
    config(
        materialized='table'
    )
}}


with

orders as (

    select '1'::numeric(10,2) as order_sk,'1234-567-00'::varchar(100) as credit_card_number ,'order-001' as order_id, '2024-01-01'::date as order_date, 'United States'::varchar(100) as order_country_origin,'John'::varchar(10) as cust_fname, 'Lennon'::varchar(10) as cust_lname union all
    select '2'::numeric(10,2) as order_sk,'1234-567-00'::varchar(100) as credit_card_number ,'order-002' as order_id, '2024-01-03'::date as order_date, 'France'::varchar(100) as order_country_origin,'Ringo'::varchar(10) as cust_fname, 'Starr'::varchar(10) as cust_lname union all
    select '3'::numeric(10,2) as order_sk,'1234-567-00'::varchar(100) as credit_card_number ,'order-003' as order_id, '2024-01-03'::date as order_date, 'Philippines'::varchar(100) as order_country_origin,'Paul'::varchar(10) as cust_fname, 'McCartney'::varchar(10) as cust_lname

)
select * from orders


