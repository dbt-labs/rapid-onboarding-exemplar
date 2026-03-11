{{
    config(
        materialized=env_var('DBT_MATERIALIZATION')
    )
}}
select *
    *

from {{ source('stripe','payment') }} 
-- pull only the most recent update for each unique record
where orderid = 1
order by 1, dbt_valid_from



