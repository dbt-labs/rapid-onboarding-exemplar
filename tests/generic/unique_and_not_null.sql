{% test unique_and_not_null(model, column_name) %}

    with null_records as (
        select {{ column_name }}
        from {{ model }}
        where {{ column_name }} is null
    ),

    duplicates as (
        select {{ column_name }}
        from {{ model }}
        group by 1 
        having count(*) > 1
    )

    select *
    from null_records 
    union all 
    select * 
    from duplicates

{% endtest %}