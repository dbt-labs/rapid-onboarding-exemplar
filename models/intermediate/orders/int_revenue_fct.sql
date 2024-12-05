{{ config(
    materialized='table',
    tags=["cfo"]
) }}

with

depreciations as (

    select * from {{ ref('dim_depreciation_accounts') }}
    where account_type = 'Major'

),

interests as (

    select * from {{ ref('dim_interests_accounts')  }}
    where account_type = 'Major'

),

net_income as (

    select * from {{ ref('dim_net_income') }}
    where account_type = 'Major'

),


taxes as (

    select * from {{ ref('dim_tax_accounts') }}
    where account_type = 'Major'

),

join_sources as (

    select
        net_inc.account_id,
        net_inc.net_income_usd,
        deprec.depreciation_cost_usd,
        ints.interests_amount_usd,
        tax.tax_usd,
        net_inc.account_type
    from net_income net_inc
    left join depreciations deprec
        on net_inc.account_id = deprec.account_id
    left join interests ints
        on net_inc.account_id = ints.account_id
    left join taxes tax
        on net_inc.account_id = tax.account_id

),

final as (

    select
        account_id,
        account_type,
        (net_income_usd + interests_amount_usd + tax_usd + depreciation_cost_usd)::numeric(10, 3) as ebitda
    from join_sources

)

select * from final

