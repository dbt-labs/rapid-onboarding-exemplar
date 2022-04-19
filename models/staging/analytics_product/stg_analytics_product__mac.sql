-- Filtering for last 5 years moved to later model

with

source as (
    
    select * from {{ source('analytics_product', 'master_agency_code') }}
    
),

cleaned as (
    
    -- Is this data still useful without distinct?
    select distinct 
    
        trim(master_agency_code) as master_agency_code
        ,trim(child_producer) as child_producer
        ,cast(accounting_month as bigint) as accounting_month
    
    from source
    
)

select * from cleaned