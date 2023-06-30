
select count(*)
from {{ ref('stg_jaffle_shop__customers') }}
join {{ ref('stg_jaffle_shop__orders') }}
using (customer_id)

having count(*) = 0