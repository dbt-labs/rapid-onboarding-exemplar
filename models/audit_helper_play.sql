{% set dbt_relation=ref('snapshot_stg_payments') %}

{% if execute %}
    {%- set old_etl_relation = adapter.get_relation(
        database="RAW",
        schema="STRIPE",
        identifier="PAYMENT") -%}
with audit_results as (
    {{ audit_helper.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        primary_key="orderid",
        summarize = false
    ) }} )
{% endif %}
--if there are any rows where in_a and in_b are not the same, there are mismatching rows between db objects
select * from audit_results where in_a != in_b