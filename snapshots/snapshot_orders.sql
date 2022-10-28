{% snapshot orders_snapshot %}

{{
    config(
      target_database='analytics',
      target_schema='dbt_dave_snapshots',
      unique_key='o_orderkey',
      strategy='check',
      check_cols='all'
    )
}}

select * from {{ source('tpch', 'orders') }}

{% endsnapshot %}