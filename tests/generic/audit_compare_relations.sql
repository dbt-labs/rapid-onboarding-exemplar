{% test audit_compare_relations(model, database=none, schema=none, identifier=none, primary_key=none) %}

{% set dbt_relation= model %}

{% if execute %}
    {%- set old_etl_relation = adapter.get_relation(
        database= database,
        schema= schema,
        identifier= identifier ) -%}
with audit_results as (
    {{ audit_helper.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        primary_key= primary_key ,
        summarize = false
    ) }} )
{% endif %}
--if there are any rows where in_a and in_b are not the same, there are mismatching rows between db objects
select * from audit_results where in_a != in_b

{% endtest %}
