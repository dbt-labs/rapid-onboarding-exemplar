{# https://github.com/dbt-labs/dbt-audit-helper#advanced-usage---dbt-cloud #}

{% macro audit_dim_customers() %}

    {%- set columns_to_compare=adapter.get_columns_in_relation(ref('dim_customers'))  -%}

    {% set old_etl_relation_query %}
        select * from analytics.analytics.dim_customers
    {% endset %}

    {% set new_etl_relation_query %}
        select * from {{ ref('dim_customers') }}
    {% endset %}

    {% if execute %}
        {% for column in columns_to_compare %}
            {{ log('Comparing column "' ~ column.name ~'"', info=True) }}
            {% set audit_query = audit_helper.compare_column_values(
                    a_query=old_etl_relation_query,
                    b_query=new_etl_relation_query,
                    primary_key="customer_id",
                    column_to_compare=column.name
            ) %}

            {% set audit_results = run_query(audit_query) %}

            {% do log(audit_results.column_names, info=True) %}
                {% for row in audit_results.rows %}
                    {% do log(row.values(), info=True) %}
                {% endfor %}
        {% endfor %}
    {% endif %}

{% endmacro %}