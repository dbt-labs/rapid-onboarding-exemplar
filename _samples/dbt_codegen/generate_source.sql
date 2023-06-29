{# 
example calling generate_source macro
https://github.com/dbt-labs/dbt-codegen/blob/main/macros/generate_source.sql
#}

{{- codegen.generate_source(
    schema_name = 'jaffle_shop',
    database_name = 'raw',
    generate_columns = True,
    include_descriptions=False,
    table_pattern='%',
    exclude='', 
    name='jaffle_shop', 
    table_names=None
    ) 
-}}