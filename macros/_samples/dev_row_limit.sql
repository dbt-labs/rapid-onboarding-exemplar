-- macros/dev_row_limit.sql
{% macro dev_row_limit(max_rows=1000) %}
  {% if target.name != 'prod' %}
    limit {{ max_rows }}
  {% endif %}
{% endmacro %}