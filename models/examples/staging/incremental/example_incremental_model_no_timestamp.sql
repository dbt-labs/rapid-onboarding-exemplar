{{
    config(
        materialized='incremental',
        on_schema_change='sync_all_columns'
    )
}}

with source as (
    select * from {{ ref('example_source_for_incremental') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where id not in (select id from {{ this }}) 
    {% endif %}
)

select *
from source