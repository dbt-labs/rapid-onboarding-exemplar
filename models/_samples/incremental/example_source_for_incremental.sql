
{{
    config(
        materialized='view'
    )
}}

{% set country_options=['US', 'Mexico', 'Canada', 'Germany', 'England', 'France', 'Ireland'] %}
{% set page_url_options=['https://www.getdbt.com/', 'https://docs.getdbt.com/', 'https://docs.getdbt.com/quickstarts'] %}
{% set page_url_title_options=['dbt Labs | Transform Data in Your Warehouse', 'What is dbt? | dbt Developer Hub', 'Quickstarts | dbt Developer Hub'] %}

with series as (

{{ dbt_utils.generate_series(3000000) }}

),

seconds_this_month as (

select 
    generated_number as event_id,

    -- events occur every second in the month
    {{ dbt.dateadd("second", "event_id", dbt.date_trunc('month', 'current_timestamp')) }} as event_timestamp


from series

where event_timestamp <= current_timestamp

),

final as (

    select 
        event_id,
        round(1 + date_part('second', event_timestamp)::int / 10.0) as user_id,

        case 
            when date_part('second', event_timestamp)::int % 3 < 2
                then 'page_view'
            else 'page_ping'
        end as event,

        event_timestamp,

        case 
            when date_part('second', event_timestamp)::int % 3 = 0
                then '{{ page_url_options[0] }}'
            when date_part('second', event_timestamp)::int % 3 = 1
                then '{{ page_url_options[1] }}'
            else '{{ page_url_options[2] }}'
        end as page_url,

        case 
            when date_part('second', event_timestamp)::int % 3 = 0
                then '{{ page_url_title_options[0] }}'
            when date_part('second', event_timestamp)::int % 3 = 1
                then '{{ page_url_title_options[1] }}'
            else '{{ page_url_title_options[2] }}'
        end as page_title,

        case 
            when round(1 + date_part('second', event_timestamp)::int / 10.0) % 7 < 4
                then 'computer'
            else 'mobile'
        end as device_type,

        case 
            when round(1 + date_part('second', event_timestamp)::int / 10.0)::int = 1
                then '{{ country_options[0] }}'
            when round(1 + date_part('second', event_timestamp)::int / 10.0)::int = 2
                then '{{ country_options[1] }}'
            when round(1 + date_part('second', event_timestamp)::int / 10.0)::int = 3
                then '{{ country_options[2] }}'
            when round(1 + date_part('second', event_timestamp)::int / 10.0)::int = 4
                then '{{ country_options[3] }}'
            when round(1 + date_part('second', event_timestamp)::int / 10.0)::int = 5
                then '{{ country_options[4] }}'
            when round(1 + date_part('second', event_timestamp)::int / 10.0)::int = 6
                then '{{ country_options[5] }}'
            else '{{ country_options[6] }}'
        end as geo,

        -- rounded event timestamp to mimic data being loaded every 5 seconds
        {{ dbt.dateadd("second", "- ((event_id % 60) % 5)", "event_timestamp" ) }} as _etl_loaded_at

        

    from seconds_this_month

)

select *
from final
