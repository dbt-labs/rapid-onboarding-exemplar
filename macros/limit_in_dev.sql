
{% macro limit_in_dev(column_name) %}

    {% if target.name == 'dev' %}
        where {{ column_name }} >= dateadd('day', -3, current_date)
    {% endif %}

{% endmacro %}