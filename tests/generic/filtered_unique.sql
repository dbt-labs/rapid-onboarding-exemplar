{% test filtered_unique(model, column_name, records_to_check=True) %}

    with all_records as (
        select *
        from {{ model }}
    ),

    new_records as (

        select {{ column_name }}
        from all_records
        where {{ records_to_check }}

    )

    select new_records.{{ column_name }}
    from new_records
    join all_records
    using ({{ column_name }})
    group by 1 having count(*) > 1
    
{% endtest %}
