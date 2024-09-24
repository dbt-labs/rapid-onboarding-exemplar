{{
    config(
        materialized='incremental',
        incremental_strategy='trunc_reload',
        transient=true,
        on_schema_change="sync_all_columns"
    )
}}

select 1 as col_val, current_timestamp as etl_recorded_ts, 1::varchar as new_col union all
select 2 as col_val, current_timestamp as etl_recorded_ts, 1::varchar as new_col union all
select 3 as col_val, current_timestamp as etl_recorded_ts, 1::varchar as new_col
