{{
    config(
        materialized='view'
    )
}}


with id_series as (

{{ dbt_utils.generate_series(2000) }}

),

adding_order_id as (


select 
    generated_number as id,
    
    -- wacky way to get some semblance of randomness for which ids are tied to which orders
    dense_rank() over (
        order by 
            case 
                when id % 7 = 0
                    then id - 1
                when id % 7 = 1 
                    then id - 2
                when id % 9 = 0
                    then id - 1
                else id
            end
    )
     as order_id
        

from id_series

),

distinct_order_ids as (
    select 
        distinct order_id
    
    from adding_order_id
),

adding_order_details as (
    select 
        order_id,
        -- 60% of records get an update
        uniform(1, 10, random()) <= 6 as needs_update,
        
        case 
            -- 60% chance of shipped
            when uniform(1, 10, random()) <= 6
                then 'shipped'
            
            -- 75% of remaining time placed
            when uniform(1, 4, random()) <= 3
                then 'pending'
            
            -- 60% of remaining time in delivery
            when uniform(1, 4, random()) <= 3
                then 'in delivery'
            
            else 'returned'
        
        end as status,

        {{ dbt.dateadd("hour", "order_id", dbt.date_trunc('year', 'current_timestamp')) }}::date as order_created_date

    from  distinct_order_ids
),

final as (

    select 
        adding_order_id.id, 
        adding_order_id.order_id,
        
        case 
            when adding_order_id.order_id % 10 <= 6
                then 'credit'
            when adding_order_id.order_id % 10 <= 8
                then 'debit'
            else 'cash'
        end as payment_method,

        case 
            when id % 3 = 0
                then 9.99
            when id % 3 = 1
                then 5.99
            else 19.99
        end as amount,

        adding_order_details.status,

        adding_order_details.order_created_date,

        case 
            when adding_order_details.needs_update
                then current_timestamp
            else order_created_date
        end as order_updated_at


    from adding_order_id

    join adding_order_details
    using (order_id)

)

select *
from final