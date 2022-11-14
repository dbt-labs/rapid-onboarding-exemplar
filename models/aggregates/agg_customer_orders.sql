select 
    *
from {{
    metrics.calculate(
        metric('total_revenue'),
        grain='all_time'
    )
}}
order by 2 desc