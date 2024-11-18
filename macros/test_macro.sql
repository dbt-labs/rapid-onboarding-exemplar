{% macro test_macro() %}

    {{ log("This is a message by log macro", info=True) }}

{% endmacro %}