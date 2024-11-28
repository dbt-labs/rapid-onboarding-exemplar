{{
    config(
        materialized='table'
    )
}}

with

countries as (

    select 1 as country_sk, 'United States'::varchar(100) as country_name union all
    select 2 as country_sk, 'France'::varchar(100) as country_name union all
    select 3 as country_sk, 'Italy'::varchar(100) as country_name union all
    select 4 as country_sk, 'Philippines'::varchar(100) as country_name
    -- union all
    -- select 5 as country_sk, 'Italia'::varchar(100) as country_name

)

select * from countries