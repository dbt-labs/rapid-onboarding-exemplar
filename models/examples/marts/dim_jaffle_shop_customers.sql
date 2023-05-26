with stg_customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

alphabet_grouping as (
    select * from {{ ref('alphabet_grouping') }}
),

stg_customers_tansformed as (
    select 
        customer_id,
        first_name,
        last_name,
        lower(left(last_name, 1)) as first_character_last_name

    from stg_customers
),

final as (

select 
    stg_customers_tansformed.customer_id,
    stg_customers_tansformed.first_name,
    stg_customers_tansformed.last_name,
    alphabet_grouping.letter_grouping

from stg_customers_tansformed
join alphabet_grouping
on stg_customers_tansformed.first_character_last_name = alphabet_grouping.letter

)

select * from final