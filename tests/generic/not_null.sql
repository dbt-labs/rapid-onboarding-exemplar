{% test not_null_or_empty_string(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} is null
    or trim({{ column_name }}) = ''

{% endtest %}