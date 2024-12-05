{{
    config(
        tags=['semantic_layer_demo']
    )
}}

  select
   id as customer_id,
   first_name,
   last_name
from {{ source('jaffle_shop', 'customers') }}