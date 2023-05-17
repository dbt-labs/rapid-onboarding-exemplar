{# https://github.com/dbt-labs/dbt-audit-helper#compare_all_columns-source #}

{{
    config(
        severity = 'warn'
    )
}}

{{ 
  audit_helper.compare_all_columns(
    a_relation=ref('dim_customers'),
    b_relation=api.Relation.create(database='analytics', schema='analytics', identifier='dim_customers'),
    primary_key='customer_id'
  ) 
}}
where not perfect_match