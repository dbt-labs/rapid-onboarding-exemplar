{#{% macro union_tables_by_prefix(database,schema, prefix) %} #}
{% macro union_tables_by_prefix(database, schema_pattern, table_pattern) %} 
    {# {% set tables = dbt_utils.get_relations_by_prefix(database=database, schema=schema, prefix=prefix) %} #}
   {% set tables=dbt_utils.get_relations_by_pattern( schema_pattern=schema_pattern, database=database, table_pattern=table_pattern) %}

    {% for table in tables %}
        {% if not loop.first %}
            union all            
        {% endif %}

        select * from {{ table.database }}.{{ table.schema }}.{{ table.identifier }}   

    {% endfor %}
 {% endmacro %} 