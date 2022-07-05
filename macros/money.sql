{% macro money(col) -%}
    {{col}} /100::decimal(16,4)
{%- endmacro %}

