-- solution #1 (Snowflake variables)
-- leverage snowflake variables
-- requires that this be built as a table rather than view, otherwise downstream models 
-- won't be able to access session variable
-- unable to preview in dbt cloud because config not resolved in compile/preview command, only
-- at dbt run / dbt build time
-- needs to reference the exact version of the table in snowflake

{{
    config(
        materialized = 'table',
        sql_header = "
            set postdate = (select to_date(max(date_day)) from dbt_coap.date_spine);
            set thrudate = (select last_day(add_months($postdate,-1)));
        "
    )
}}

select 
    $postdate as original_date,
    $thrudate as previous_date

