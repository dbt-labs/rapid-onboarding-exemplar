with

coverage_earned_premium as (
    
    select * from {{ ref('stg_cmp_phx__coverage_earned_premium_fact_v') }}
    where month_end_date BETWEEN {{ var('start_date')}} and {{ var('end_date')}}
    
)

, policy as (
    
    select * from {{ ref('stg_cmp_phx__policy_trx_v') }}
    where underwriting_state_code in (
        'AZ','CA','GA','IL','NJ','NV','NY','OK','TX','VA'
    )   
    
)

, final as (
    
    select
    
        coverage_earned_premium.lob
        ,policy.underwriting_state_code
        ,policy.policy_num
        ,policy.policy_module_inception_date
        ,policy.policy_original_inception_date
        ,coverage_earned_premium.month_end_date
        ,policy.sales_office_sk
        ,policy.sales_office_code
    
        ,row_number() over( 
            partition by 
                policy.underwriting_state_code
                ,policy.policy_number
            order by 
                policy.module_effective_date desc
                ,coverage_earned_premium.month_end_date desc
                ,policy.transaction_effective_date desc
                ,policy.last_update_ts desc
                ,coverage_earned_premium.module_effective_date desc
                ,coverage_earned_premium.transaction_effective_date desc
                ,coverage_earned_premium.last_update_ts desc
                ,policy.f_sales_office_key desc
                ,policy.sales_office_code desc
        ) as rno 

    
    from coverage_earned_premium
    inner join policy
        on coverage_earned_premium.policy_transaction_key = policy.policy_transaction_key
    
)

select * from final 