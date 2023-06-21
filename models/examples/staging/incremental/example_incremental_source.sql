{{
    config(
        materialized='incremental'
    )
}}

{% set status_options=['shipped', 'ordered', 'placed', 'returned'] %}
{% set country_options=['US', 'Mexico', 'Canada', 'Germany', 'England', 'France', 'Ireland'] %}


{% if is_incremental() %}
    {% set results = dbt_utils.get_query_results_as_dict('select max(id) as max_id from ' ~ this) %}
    {{ log('hi', info=True) }}
    {{ log("results['MAX_ID'][0]", info=True) }}
    {{ log(results['MAX_ID'][0], info=True) }}
    {% set min_id = results['MAX_ID'][0]| int + 1 %}
    {% set max_id = min_id + 10 %}
{% else %}
    {% set min_id = 0 %}
    {% set max_id = 10 %}
{% endif %}

{% for number in range(min_id, max_id) %}
    select 
        {{ number }}::int as id,
        '{{ status_options|random }}' as status,
        '{{ country_options|random }}' as country,
        current_time as etl_loaded_at
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}

