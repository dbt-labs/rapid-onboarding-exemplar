{% test relationship_generic(model, column_name, key_field, parent_model) %}

with parent as (

    select
        {{ key_field }} as id

    from {{ parent_model }}

),

child as (

    select
        {{ column_name }} as id

    from {{ model }}

)

select *
from child
where id is not null
  and id not in (select id from parent)

{% endtest %}