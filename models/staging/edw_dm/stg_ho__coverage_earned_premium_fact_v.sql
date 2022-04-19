with

source as (
    
    select * from {{ source('ho', 'coverage_earned_premium_fact_v') }}
    
),

cleaned as (
    
    select
    
        'HO' as lob
        ,ho_policy_trx_sk as policy_trx_sk
        ,sales_office_sk
        ,trim(sales_office_code) as sales_office_code
        ,month_end_date
        
    from source
    
)

select * from cleaned