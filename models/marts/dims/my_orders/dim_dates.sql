{{
    config(
        materialized='table',
        tags=["orders"]
    )
}}



with

dates as (

    select    '20240101'::decimal(18,2) as date_sk, '2024-01-01'::date as calendar_date union all
    select    '20240102'::decimal(18,2) as date_sk, '2024-01-02'::date as calendar_date union all
    select    '20240103'::decimal(18,2) as date_sk, '2024-01-03'::date as calendar_date union all
    select    '20240104'::decimal(18,2) as date_sk, '2024-01-04'::date as calendar_date union all
    select    '20240105'::decimal(18,2) as date_sk, '2024-01-05'::date as calendar_date

)

select * from dates