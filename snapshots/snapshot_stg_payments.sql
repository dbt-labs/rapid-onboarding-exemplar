{% snapshot snapshot_stg_payments %}

{{
    config(
      target_database='analytics',
      target_schema=var('example_target_snapshot_schema'),
      unique_key='sk',

      strategy='timestamp',
      updated_at='_BATCHED_AT',
    )
}}

select 
    *,
    {{ dbt_utils.generate_surrogate_key(['ID', 'ORDERID']) }} as sk


 from {{ source('stripe', 'payment') }}

{% endsnapshot %}