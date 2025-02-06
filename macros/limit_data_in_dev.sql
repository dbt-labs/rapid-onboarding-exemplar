{% macro limit_data_in_dev(column_name, dev_days_of_data = 3) %}

-- if running in dev, only look at data thats 3 days old at the oldest
    {% if target.name == 'dev' %}

        where {{column_name }} >= dateadd('day', -{{ dev_days_of_data }}, current_timestamp)

    {% endif %}
{% endmacro %}
