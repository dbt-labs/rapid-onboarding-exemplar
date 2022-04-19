with

source as (
    
    select * from {{ source('cmp_phx', 'policy_transaction_view') }}
    
),

cleaned as (
    
    select
    
        cmp_policy_trx_sk as policy_trx_sk
        ,f_sales_office_key as sales_office_sk
        ,underwriting_state_code
        ,policy_number as policy_num
        ,module_effective_date as policy_module_inception_date
        ,policy_original_inception_date
        ,transaction_effective_date
        ,last_update_ts
        ,trim(sales_office_code) as sales_office_code
        
    from source 
    
)

select * from cleaned
