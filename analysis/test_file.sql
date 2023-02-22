select * from {{ ref('stg_stripe__payments') }}
where id = 5