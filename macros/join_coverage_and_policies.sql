{% macro join_coverage_and_policies(coverage_table, policy_table, states) %}
{# This assumes the join key is named 'policy_trx_sk' in both tables. #}

with

coverage_earned_premium as (
    
    select * from {{ coverage_table }}
    where month_end_date between {{ var('start_date') }} and {{ var('end_date') }}
    
),

policy as (
    
    select * from {{ policy_table }}
    where underwriting_state_code in (
        {%- for state in states %}
        '{{ state }}'{%- if not loop.last %},{% endif %}
        {%- endfor %}
    )
    
),

final as (
    
    select
    
        coverage_earned_premium.lob
        ,coverage_earned_premium.month_end_date
        ,coverage_earned_premium.sales_office_sk
        ,coverage_earned_premium.sales_office_code
        ,policy.underwriting_state_code
        ,policy.policy_num
        ,policy.policy_module_inception_date
        ,policy.policy_original_inception_date
        
        ,row_number() over(
            partition by
                policy.underwriting_state_code
                ,policy.policy_num
            order by
                policy.policy_module_inception_date desc
                ,coverage_earned_premium.month_end_date desc
                ,coverage_earned_premium.sales_office_sk desc
                ,policy.sales_office_code desc
        ) as rno
    
    from coverage_earned_premium
    inner join policy
        on coverage_earned_premium.policy_trx_sk = policy.policy_trx_sk
    
    
)

select * from final

{% endmacro %}