select 
    *
from {{
    metrics.calculate(
        metric('total_revenue'),
        dimensions=['customer_id']
    )
}}
