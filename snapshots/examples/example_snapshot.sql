{% snapshot example_snapshot %}
    {{
        config(
            target_database='analytics',
            target_schema='snapshots',
            unique_key='id',
            strategy='timestamp',
            updated_at='order_updated_at',
            invalidate_hard_deletes=True
        )
    }}

    select * from {{ ref('source_for_example_snapshot') }}
 {% endsnapshot %}