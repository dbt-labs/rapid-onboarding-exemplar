{% snapshot snapshot_int_line_items %}

-- Example showing check columns strategy and using custom schemas (this is rare)

-- if you don't want to write to the same schema in development and prod, 
-- use environment-aware variable from dbt_project.yml

-- if you do not have a reliable updated at timestamp, check columns for changes.

{{
    config(
      target_schema=var('example_target_snapshot_schema'),
      unique_key='sk',
      strategy='check',
      check_cols=['check_column']
    )
}}

select 
    * ,
    {{ dbt_utils.generate_surrogate_key(['gross_item_sales_amount', 'net_item_sales_amount']) }} as check_column,
    {{ dbt_utils.generate_surrogate_key(['order_item_id', 'order_id']) }} as sk
    
from {{ ref('int_line_items_amounts_calculated') }}

{% endsnapshot %}