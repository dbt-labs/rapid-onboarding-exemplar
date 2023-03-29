-- example showing staging model after snapshot

select
    -- ids
    id as payment_id,
    orderid as order_id,
    
    -- descriptions
    paymentmethod as payment_method,
    status,
    
    -- datetimes
    created as created_time_at

from {{ ref('snapshot_stg_payments') }} 
-- pull only the most recent update for each unique record
where dbt_valid_to is null
