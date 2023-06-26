{% snapshot example_snapshot_check %}
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

    {% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('source_for_example_snapshot'), except=["id"]) %}


    select 
        *,
        {{ dbt_utils.generate_surrogate_key(column_names) }} as check_cols
    from {{ ref('source_for_example_snapshot') }}
 {% endsnapshot %}