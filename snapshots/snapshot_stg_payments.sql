{% snapshot snapshot_stg_payments %}

{{
    config(
      target_database='analytics',
      target_schema='snapshots',
      unique_key='ID',

      strategy='timestamp',
      updated_at='_BATCHED_AT',
    )
}}




select * from {{ source('stripe', 'payment') }}

{% endsnapshot %}