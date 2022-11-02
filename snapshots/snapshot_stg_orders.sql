{% snapshot snapshot_dim_customers %}
{#
environment aware target_schema
  -- either use this config: 
        target_schema=target.schema if target.name == 'dev' else 'prod_snapshot',
  -- or
        set a variable beforehand and call the variable
    -- local in the sql file
      -- ex: 
        {% set target_snapshot_schema=target.schema if target.name == 'dev' else 'prod_snapshot' %}
        config: target_schema=target_snapshot_schema
    -- globally in dbt_project.yml
      -- ex: 
          vars:
            target_snapshot_schema: "{{ target.schema if target.name == 'dev' else 'prod_snapshot' }}"
          config: target_schema=var('target_snapshot_schema')

#}

{% set target_snapshot_schema=target.schema if target.name == 'dev' else 'prod_snapshot' %}

{{
    config(
      target_schema=var('target_snapshot_schema'),
      unique_key='order_id',

      strategy='timestamp',
      updated_at='order_date',
    )
}}

select * from {{ ref('stg_tpch__orders') }}

{% endsnapshot %}