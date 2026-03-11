
select *
from {{ ref('dbt_developer_pathway','dim_customers') }}