{% macro get_filename() %}

    {% set nodes = graph.nodes.values() %}
    {% for node in nodes %}
        {{ node.name if node.resource_type == 'model' }}
    {% endfor %}

{% endmacro %}

