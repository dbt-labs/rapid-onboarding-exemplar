{%- macro date_diff_macro(original_date, delta) -%}

    {%- set sql_statement -%}
        select last_day(add_months('{{ original_date }}', {{ delta }}))
    {%- endset -%}

    {%- set new_date = dbt_utils.get_single_value(sql_statement) -%}

    {{- new_date -}}

{%- endmacro -%}