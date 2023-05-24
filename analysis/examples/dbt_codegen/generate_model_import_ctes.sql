{# 
example calling generate_model_import_ctes macro
https://github.com/dbt-labs/dbt-codegen#create_base_models-source
#}

{{- codegen.generate_model_import_ctes(
    model_name = 'my_unorganized_model'
) -}}