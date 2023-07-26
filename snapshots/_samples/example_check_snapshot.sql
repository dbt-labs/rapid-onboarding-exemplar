{% snapshot example_check_snapshot %}
    {{
        config(
            target_database='analytics',
            target_schema='snapshots',
            unique_key='id',
            strategy='check',
            check_cols=['check_cols'],
            invalidate_hard_deletes=True
        )
    }}

    {% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('example_orders_line_items_source_for_snapshot'), except=["id"]) %}


    select 
        *,
        {{ dbt_utils.generate_surrogate_key(column_names) }} as check_cols
    from {{ ref('example_orders_line_items_source_for_snapshot') }}
 {% endsnapshot %}