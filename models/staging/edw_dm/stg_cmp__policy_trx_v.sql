with

source as (
    
    select * from {{ source('cmp', 'policy_trx_v') }}
    
),

cleaned as (
    
    select
    
        cmp_policy_trx_sk as policy_trx_sk
        ,underwriting_state_code
        ,policy_num
        ,policy_module_inception_date
        ,policy_original_inception_date
        ,trim(sales_office_code) as sales_office_code
        
    from source 
    
)

select * from cleaned