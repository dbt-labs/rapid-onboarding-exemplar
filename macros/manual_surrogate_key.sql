-- macros/surrogate_key.sql
{% macro surrogate_key(cols) %}
    md5(
        {{ cols
            | map('string')
            | join(" || '|' || ")
        }}
    )
{% endmacro %}