{% macro cents_to_dollars(amount, source_system_value, decimals=2) %}
    
    {{ log("Running some_macro: " ~  'source_system_value'  , info=true) }}
    {% if source_system_value  == 'apple' %}
        {{ amount }} / 100::decimal(16, {{ decimals }})
    {% elif source_system_value  == 'andriod' %}  
        {{ amount }} / 100::decimal(16, {{ decimals }})*1.6
    {% endif %}

{% endmacro %}