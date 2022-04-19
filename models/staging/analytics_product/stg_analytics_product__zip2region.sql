-- Ordering removed since it doesn't do anything except arrange the rows for 
-- visualizing

with

source as (
    
    select * from {{ source('analytics_product', 'zip_to_region_mappings_credibility') }}
    
),

cleaned as (
    
    select
    
        state
        ,lpad(zip, 5, '0') as zip
        ,region_xs
        ,region_s
        ,region_m
        ,region_l
        ,region_xl
        ,cast(replace(ee, ',', '') as bigint) as ee
        ,cast(replace(claim_counts_w_nopay_xcat, ',', '') as bigint) as claim_counts_w_nopay_xcat
        ,cast(replace(claim_counts_w_cat_wo_npay, ',', '') as bigint) as claim_counts_w_cat_wo_npay
    
    from source
    
)

select * from cleaned