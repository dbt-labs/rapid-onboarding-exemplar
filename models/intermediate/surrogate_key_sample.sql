select
    {{ surrogate_key(['order_id', 'customer_id']) }} as order_customer_sk,
    *
from {{ ref('stg_tpch__orders') }}