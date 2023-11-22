-- example showing staging model after snapshot

select
    -- ids
    id as payment_id,
    orderid as order_id,
    
    -- descriptions
    paymentmethod as payment_method,
    status,
    {{ money('amount') }} as amount, -- amount is stored in cents, convert it to dollars
    
    -- datetimes
    created as created_at

from {{ ref('snapshot_stg_payments') }} 

-- for CI builds, only pull the records from the prior day

{% if target.name == 'CI'%}
where created_at >= dateadd('day',-1,current_date())
{% endif %}
