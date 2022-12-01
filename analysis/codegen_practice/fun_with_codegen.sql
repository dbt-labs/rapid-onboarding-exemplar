{# ----- generate_source ------ #}

{# Source yml file stub #}
{{ codegen.generate_source('tpch_sf1') }}

{# Source yml including database name and table names #}
{{ codegen.generate_source('tpch_sf1', 'snowflake_sample_data') }}

{# Source yml including database name, table names and column names #}
{{ codegen.generate_source('tpch_sf1', 'snowflake_sample_data', generate_columns='True') }}

{# ----- generate_base_model ------ #}

{# Generates the staging model for a souce declared in a yaml file #}
{{ codegen.generate_base_model(
    source_name='tpch',
    table_name='customer'
) }}

{# Another example building a staging model for the orders table #}
{{ codegen.generate_base_model(
    source_name='tpch',
    table_name='orders',
) }}

{# This will generate the models yaml file after a model is made #}
{{ codegen.generate_model_yaml(
    model_name='stg_tpch__orders'
) }}


{# Generates Import CTE's #}
{{ codegen.generate_model_import_ctes(
    model_name = 'fct_orders'
) }}