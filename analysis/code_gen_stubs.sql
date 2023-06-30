{{ codegen.generate_model_yaml(
    model_names=['dim_customers']
) }}

{{ codegen.generate_base_model(
    source_name='tpch',
    table_name='customer'
) }}