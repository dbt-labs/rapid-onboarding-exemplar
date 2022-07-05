{{
    config(
        schema=var('customer')
    )
}}

select
    -- your logic
    *
from
    {{ ref('customer_data') }}
where
    customer_id = '{{ var('customer') }}'