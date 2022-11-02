with o as (

    select * from {{ ref('stg_test_RO__orders') }}
)

select * from o 
