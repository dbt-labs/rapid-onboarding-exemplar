with

source as (
    
    select * from {{ source('ll', 'coverage_earned_premium_fact_v') }}
    
),

cleaned as (
    
    select
    
        'LL' as lob
        ,ll_policy_trx_sk as policy_trx_sk
        ,sales_office_sk
        ,trim(sales_office_code) as sales_office_code
        ,month_end_date
        
    from source
    
)

select * from cleaned