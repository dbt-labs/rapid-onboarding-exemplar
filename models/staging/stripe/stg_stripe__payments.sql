-- example showing staging model after snapshot

select
    -- ids
    id as payment_id,
    orderid,
    1 as order_number,
    
    -- descriptions
    paymentmethod as payment_method,
    {{ money('amount') }} as amount, -- amount is stored in cents, convert it to dollars
    
    -- datetimes
    created as created_at

from {{ ref('snapshot_stg_payments') }} 
-- pull only the most recent update for each unique record
where dbt_valid_to is null
