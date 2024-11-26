with


source as (

    select * from {{ ref('fct_daily_orders') }}

),

validations as (

    select
        order_sk,
        count(*) as rec_cnt
    from {{ ref('fct_daily_orders') }}
    group by order_sk
    having count(*) > 1

)

select * from validations