{{ config(
    materialized='table',
    tags=["cfo"]
) }}

with

sources as (

    select * from {{ ref('int_revenue_fct') }}

)

select * from sources
