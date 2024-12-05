{{
    config(
        materialized='table',
        tags=["orders"]
    )
}}



with

orders as (

    select '1'::numeric(10,2) as order_sk, 'order-001' as order_id, '2024-01-01'::date as order_date, 'United States'::varchar(100) as order_country_origin,'John'::varchar(10) as cust_fname, 'Lennon'::varchar(10) as cust_lname, 1::decimal(17,3) as order_quantity union all
    select '2'::numeric(10,2) as order_sk, 'order-002' as order_id, '2024-01-03'::date as order_date, 'France'::varchar(100) as order_country_origin,'Ringo'::varchar(10) as cust_fname, 'Starr'::varchar(10) as cust_lname,  2::decimal(17,2)  as order_quantity union all
    select '3'::numeric(10,2) as order_sk, 'order-003' as order_id, '2024-01-03'::date as order_date, 'Philippines'::varchar(100) as order_country_origin,'Paul'::varchar(10) as cust_fname, 'McCartney'::varchar(10) as cust_lname,  3::decimal(17,3)  as order_quantity

-- model contract example: missing column order_quantity
    -- select '1'::numeric(10,2) as order_sk, 'order-001' as order_id, '2024-01-01'::date as order_date, 'United States'::varchar(100) as order_country_origin,'John'::varchar(10) as cust_fname, 'Lennon'::varchar(10) as cust_lname union all
    -- select '2'::numeric(10,2) as order_sk, 'order-002' as order_id, '2024-01-03'::date as order_date, 'France'::varchar(100) as order_country_origin,'Ringo'::varchar(10) as cust_fname, 'Starr'::varchar(10) as cust_lname union all
    -- select '3'::numeric(10,2) as order_sk, 'order-003' as order_id, '2024-01-03'::date as order_date, 'Philippines'::varchar(100) as order_country_origin,'Paul'::varchar(10) as cust_fname, 'McCartney'::varchar(10) as cust_lname
    -- union all
    -- select NULL::decimal(18,2) as order_sk, 'order-003' as order_id, '2024-01-03'::date as order_date, 'Philippines'::varchar(100) as order_country_origin,'Johnny'::varchar(10) as cust_fname, 'English'::varchar(10) as cust_lname,  3 as order_quantity union all
    -- select '3'::decimal(18,2) as order_sk, 'order-003' as order_id, '2024-01-03'::date as order_date, 'Philippines'::varchar(100) as order_country_origin,'Mister'::varchar(10) as cust_fname, 'Bean'::varchar(10) as cust_lname,  3 as order_quantity

)

select * from orders


