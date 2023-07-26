{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='sync_all_columns'
    )
}}

with source as (
    select * from {{ ref('example_source_for_incremental') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where _etl_loaded_at > (select {{ dbt.dateadd("hour", -3, "max(_etl_loaded_at)") }} from {{ this }}) 

        
    {% endif %}
)

select *
from source