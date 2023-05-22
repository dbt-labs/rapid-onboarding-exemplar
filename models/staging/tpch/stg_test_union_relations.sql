{{ dbt_utils.union_relations(
    relations=[ref('stg_tpch__nations'), ref('stg_tpch__orders')],
) }}