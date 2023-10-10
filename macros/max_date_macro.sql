{%- macro max_date_macro(model, column) -%}

    {%- set sql_statement -%}
        select to_date(max({{ column }})) from {{ model }}
    {%- endset -%}

    {%- set max_date = dbt_utils.get_single_value(sql_statement) -%}

    {{- max_date -}}

{%- endmacro -%}