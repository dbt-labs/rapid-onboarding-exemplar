{{
    config(
        materialized = 'incremental'
        , begin = '2024-01-01'
        , strategy = 'microbatch'
        , event_time = 'created_at'
        , batch_size = 'day'
    )
}}

select * from {{ ref('transactions') }}