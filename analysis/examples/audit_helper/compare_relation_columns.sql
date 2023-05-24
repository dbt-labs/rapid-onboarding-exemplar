{# 
example calling compare_relation_columns macro
https://github.com/dbt-labs/dbt-audit-helper/tree/0.9.0/#compare_relation_columns-source
#}

{% set old_etl_relation=ref('my_old_unorganized_model') %}

{% set dbt_relation=ref('my_unorganized_model') %}

{{ audit_helper.compare_relation_columns(
    a_relation=old_etl_relation,
    b_relation=dbt_relation
) }}