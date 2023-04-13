{{
    config(
        enabled=false
    )
}}

select 
    *
from {{
    metrics.calculate(
        metric('total_revenue'),
        grain='all_time',
        dimensions=['customer_id']
    )
}}
order by 2 desc