-- example showing staging model after snapshot

select
    -- ids
    id as payment_id,
    orderid as order_id,
    "1" as new_id,
    -- descriptions
    paymentmethod as payment_method,
    status,
    {{ money('amount') }} as amount, -- amount is stored in cents, convert it to dollars
    
    -- datetimes
    created as created_at

from {{ ref('snapshot_stg_payments') }} 
-- pull only the most recent update for each unique record
where dbt_valid_to is null



