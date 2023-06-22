{{
    config(
        materialized='view'
    )
}}

{% set status_options=['shipped', 'ordered', 'placed', 'returned'] %}
{% set country_options=['US', 'Mexico', 'Canada', 'Germany', 'England', 'France', 'Ireland'] %}

WITH RECURSIVE seconds_this_month AS (
    -- start date
    SELECT date_trunc('month', current_timestamp) as _etl_loaded_at
    UNION ALL
    SELECT DATEADD('second',1,_etl_loaded_at) as _etl_loaded_at
    FROM seconds_this_month
    
    WHERE _etl_loaded_at < current_timestamp
),

final as (

    select 
        ROW_NUMBER() over (order by _etl_loaded_at) as id,
        case 
            when date_part('second', _etl_loaded_at)::int between 0 and 14
                then '{{ status_options[0] }}'
            when date_part('second', _etl_loaded_at)::int between 15 and 29
                then '{{ status_options[1] }}'
            when date_part('second', _etl_loaded_at)::int between 30 and 44
                then '{{ status_options[2] }}'
            else '{{ status_options[3] }}'
        end as status,
        case 
            when date_part('second', _etl_loaded_at)::int between 0 and 8
                then '{{ country_options[0] }}'
            when date_part('second', _etl_loaded_at)::int between 9 and 17
                then '{{ country_options[1] }}'
            when date_part('second', _etl_loaded_at)::int between 18 and 26
                then '{{ country_options[2] }}'
            when date_part('second', _etl_loaded_at)::int between 27 and 35
                then '{{ country_options[3] }}'
            when date_part('second', _etl_loaded_at)::int between 36 and 44
                then '{{ country_options[4] }}'
            when date_part('second', _etl_loaded_at)::int between 45 and 53
                then '{{ country_options[5] }}'
            else '{{ country_options[6] }}'
        end as country,
        _etl_loaded_at

    from seconds_this_month

)

select *
from final