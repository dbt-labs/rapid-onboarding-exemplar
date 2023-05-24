{# 
example calling generate_model_yaml macro
https://github.com/dbt-labs/dbt-codegen/blob/main/macros/generate_model_yaml.sql
#}

{{- codegen.generate_model_yaml(
        model_names=['stg_tpch__customers', 'dim_customers'],
        upstream_descriptions=True
    ) 
-}}