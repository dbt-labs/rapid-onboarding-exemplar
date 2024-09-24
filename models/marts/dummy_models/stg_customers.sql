{{
    config(
        materialized='table',
        transient=false
    )
}}


select 1 as customer_id, 'name1' as first_name, 'lname1' as last_name,  'cool@example.com' as email, 'example.com' as email_top_level_domain union all
select 2 as customer_id, 'name2' as first_name, 'lname2' as last_name,  'cool@unknown.com' as email, 'unknown.com' as email_top_level_domain union all
select 3 as customer_id, 'name3' as first_name, 'lname3' as last_name,  'badgmail.com' as email, 'gmail.com' as email_top_level_domain union all
select 4 as customer_id, 'name4' as first_name, 'lname4' as last_name,  'missingdot@gmailcom' as email, 'gmail.com' as email_top_level_domain

