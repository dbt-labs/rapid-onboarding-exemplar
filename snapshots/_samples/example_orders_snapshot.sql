{% snapshot example_orders_snapshot %}
    {{
        config(
            target_database='analytics',
            target_schema='snapshots',
            unique_key='order_id',
            strategy='timestamp',
            updated_at='order_updated_at',
            invalidate_hard_deletes=True
        )
    }}

    select * from {{ ref('example_orders_source_for_snapshot') }}
 {% endsnapshot %}