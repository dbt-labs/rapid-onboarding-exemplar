{# https://github.com/dbt-labs/dbt-utils/tree/1.1.0/#date_spine-source #}

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2023-01-01' as date)",
    end_date="current_date"
   )
}}