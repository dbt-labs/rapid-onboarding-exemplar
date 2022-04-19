with

source as (
    
    select * from {{ source('cmp_phx', 'policy_premium_monthly_fact_view') }}
    
),

cleaned as (
    
    select
    
        'CMP PHX' as lob
        ,cmp_policy_trx_sk as policy_trx_sk
        ,sales_office_sk
        ,trim(sales_office_code) as sales_office_code
        ,month_end_date
        ,module_effective_date
        ,transaction_effective_date
        ,last_update_ts
        ,f_policy_transaction_key as policy_transaction_key
        
    from source
    
)

select * from cleaned