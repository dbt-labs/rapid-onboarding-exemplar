{# 
example calling compare_queries macro
https://github.com/dbt-labs/dbt-audit-helper/tree/0.9.0/#compare_queries-source
#}

{% set old_query %}
  select *
  from analytics.analytics.dim_customers
{% endset %}

{% set new_query %}
  select *
  from {{ ref('dim_customers') }}
{% endset %}

{{ audit_helper.compare_queries(
    a_query=old_query,
    b_query=new_query,
    primary_key="customer_id"
) }}
