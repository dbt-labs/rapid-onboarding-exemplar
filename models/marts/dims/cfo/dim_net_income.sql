{{ config(
    materialized='table',
    tags=["cfo"]
) }}


with

source as (

    select '1'::numeric(10, 3) as account_id, '10000.00'::numeric(10, 3) net_income_usd,'Major'::varchar as account_type, '2024-01-01'::date as record_updated_ts union all
    select '2'::numeric(10, 3) as account_id, '10000.00'::numeric(10, 3) net_income_usd,'Minor'::varchar as account_type, '2024-01-01'::date as record_updated_ts

)

select * from source
