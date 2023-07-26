select *
from {{ ref('fct_dbt__model_executions') }}