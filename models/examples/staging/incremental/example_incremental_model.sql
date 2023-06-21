{{
    config(
        materialized='incremental'
    )
}}

with source as (
    select * from {{ ref('example_incremental_source') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where etl_loaded_at > (select max(etl_loaded_at) from {{ this }}) 
    {% endif %}
)

select *
from source