{% test is_positive_and_not_zero(model, column_name) %}

    {{
        config(
            enabled=true,
            error_if = '>50',
            warn_if = '>5',
            tags = ['my_generic_test']
        )
    }}

    with validation as (
        select {{ column_name }} as positive_value
        from {{ model }}
        
    )

    select *
    from validation
    where positive_value <= 0

{% endtest %}