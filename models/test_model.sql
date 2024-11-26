{{
    config(
        materialized='table'
    )
}}


with

src as (

    select 1 as col_id

)

select * from src

{{ log("This is a log inside the model", info=True) }}