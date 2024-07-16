
select * 

-- specifying old version of model
from {{ ref('example_private_finance_model', v=1) }}


