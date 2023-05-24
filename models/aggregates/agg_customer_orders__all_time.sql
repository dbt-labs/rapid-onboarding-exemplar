select 
    *
from {{
    metrics.calculate(
        metric('total_revenue'),
        dimensions=['customer_id']
    )
}}
order by 2 desc