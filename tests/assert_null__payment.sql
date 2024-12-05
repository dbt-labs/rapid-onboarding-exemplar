with

source as (

    select * from {{ source('stripe', 'payment') }}

)

select
id
from source
where id is null