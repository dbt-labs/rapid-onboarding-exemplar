{{
    config(
        materialized='table',
        transient=false
    )
}}

select 1 as id, 'yahoo.com' as tld, 'yahoo' as email_provider union all
select 2 as id, 'gmail.com' as tld, 'google' as email_provider 