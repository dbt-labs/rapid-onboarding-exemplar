{% set models_to_generate = codegen.get_models(directory='staging/tpch') %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
) }}



{{ codegen.generate_source('tpch_sf1', 'snowflake_sample_data') }}


{{ codegen.generate_base_model(
    source_name='tpch',
    table_name='orders',
    leading_commas='true'
) }}