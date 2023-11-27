with

base as (

    {{ ref('stg_stripe__payments') }}

)

select * from base