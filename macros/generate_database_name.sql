{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}
    {%- set env = env_var('DBT_ENV_NAME') -%}
    {%- if custom_database_name is none and env != 'dev' -%}

        {{ default_database }}

    {%- else -%}

        {{ custom_database_name | trim }}_{{ env }}

    {%- endif -%}

{%- endmacro %}
