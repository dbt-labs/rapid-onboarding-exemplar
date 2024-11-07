select
    *,
    null as test_col,
    10 as constant
from    
    {{ ref('stg_stripe__payments') }}