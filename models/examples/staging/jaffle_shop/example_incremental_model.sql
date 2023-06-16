{% set status_options=['shipped', 'ordered', 'placed', 'returned'] %}
{% set min_id=0 %}
{% set max_id=10 %}

{% for number in range(min_id, max_id) %}
    select 
        {{ number }} as id,
        current_time as etl_loaded_at,
        '{{ status_options|random }}' as status
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}

