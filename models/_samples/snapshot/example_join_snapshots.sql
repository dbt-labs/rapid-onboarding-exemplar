with order_snapshot as (
    select 
        * exclude dbt_valid_to,
        coalesce(dbt_valid_to, cast('{{ var("future_proof_date") }}' as timestamp)) as dbt_valid_to
    from {{ ref('example_orders_snapshot') }}
),

orders_line_items_snapshot as (
    select 
        * exclude dbt_valid_to,
        coalesce(dbt_valid_to, cast('{{ var("future_proof_date") }}' as timestamp)) as dbt_valid_to
    from {{ ref('example_orders_line_items_snapshot') }}
),

joined as (

select
    orders_line_items_snapshot.id as line_item_id,
    order_snapshot.order_id,
    order_snapshot.payment_method,
    order_snapshot.status,
    orders_line_items_snapshot.amount,
    greatest(order_snapshot.dbt_valid_from,
        orders_line_items_snapshot.dbt_valid_from) as valid_from,
    least(order_snapshot.dbt_valid_to,
        orders_line_items_snapshot.dbt_valid_to) as valid_to,
    min(least(order_snapshot.order_created_at, orders_line_items_snapshot.order_created_at)) as order_created_at,
    max(least(order_snapshot.order_updated_at, orders_line_items_snapshot.order_updated_at)) as order_updated_at


from order_snapshot

left join orders_line_items_snapshot
on order_snapshot.order_id = orders_line_items_snapshot.order_id
and order_snapshot.dbt_valid_from <= orders_line_items_snapshot.dbt_valid_to
and order_snapshot.dbt_valid_to >= orders_line_items_snapshot.dbt_valid_from

group by 1,2,3,4,5,6,7

),

final as (
    select 
        {{ dbt_utils.generate_surrogate_key(['line_item_id', 'valid_from', 'valid_to']) }} as surrogate_key,
        *
    from joined
)

select * from final
