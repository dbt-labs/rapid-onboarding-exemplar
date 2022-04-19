with

source as (
    
    select * from {{ source('ppa', 'coverage_earned_premium_fact_v') }}
    
),

cleaned as (
    
    select
    
        'PPA' as lob
        ,ppa_policy_trx_sk as policy_trx_sk
        ,sales_office_sk
        ,trim(sales_office_code) as sales_office_code
        ,month_end_date
            
    from source
    
)

select * from cleaned