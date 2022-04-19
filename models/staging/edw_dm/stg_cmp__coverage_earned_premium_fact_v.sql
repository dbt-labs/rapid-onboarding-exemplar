with

source as (
    
    select * from {{ source('cmp', 'coverage_earned_premium_fact_v') }}
    
),

cleaned as (
    
    select
    
        'CMP' as lob
        ,cmp_policy_trx_sk as policy_trx_sk
        ,sales_office_sk
        ,trim(sales_office_code) as sales_office_code
        ,month_end_date

    from source
    
)

select * from cleaned