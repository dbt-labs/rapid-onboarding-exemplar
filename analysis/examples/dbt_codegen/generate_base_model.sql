{# 
example calling generate_base_model macro
https://github.com/dbt-labs/dbt-codegen/blob/main/macros/generate_base_model.sql
#}

{{- codegen.generate_base_model(
    source_name='tpch',
    table_name='orders',
    leading_commas=False,
    case_sensitive_cols=False,
    materialized='table'
) -}}