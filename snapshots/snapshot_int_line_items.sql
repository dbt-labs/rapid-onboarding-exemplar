{% snapshot snapshot_int_line_items %}

-- Example showing check columns strategy and using custom schemas (this is rare)

-- if you don't want to write to the same schema in development and prod, 
-- use environment-aware variable from dbt_project.yml

-- if you do not have a reliable updated at timestamp, checek columns for changes.

{{
    config(
      target_schema=var('example_target_snapshot_schema'),
      unique_key='order_item_id',
      strategy='check',
      check_cols=['gross_item_sales_amount', 'net_item_sales_amount'],
    )
}}

select * from {{ ref('int_line_items_amounts_calculated') }}

{% endsnapshot %}