{% test not_empty(model) %}

    with num_records as (
        select count(*) as total_rows
        from {{ model }}
    )

    select *
    from num_records
    where total_rows = 0

{% endtest %}