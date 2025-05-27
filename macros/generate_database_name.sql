{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}
    {%- set env = env_var('DBT_ENV_NAME') -%}
    {%- if custom_database_name is not none -%}

        {{ custom_database_name | trim }}_{{ env }}

    {%- else -%}

        {{ default_database }}

    {%- endif -%}

{%- endmacro %}
