{# Practice with basic variables assignments #}

{# 

{%- set my_cool_string = 'Wow! Cool!' -%}
{%- set my_second_cool_string = 'This is Jinja' -%}
{% set my_cool_number = 50 %}

{{ my_cool_string }} {{ my_second_cool_string }} I want to write Jinja for {{ my_cool_number }} years.

{% set my_national_parks = ['Yellowstone', 'Yosemite', 'Sequoia', 'Grand Canyon', 'Glacier'] %}

{{ my_national_parks[2] }}

{% for park in my_national_parks %}
    My favorite place to hike is {{ park }} National Park!
{% endfor %}    

#}

{% set temperature = 55 %}

{% if temperature < 32 %}
  Water is turning to ice outside.
{% else %}
  Water is not ice outside.
{% endif %}

{% set foods = ['carrot', 'hotdog', 'cucumber', 'bell pepper'] %}

{% for food in foods %}
  {% if food == 'hotdog' %}
    {% set food_type = 'snack' %}
  {% else %}
    {% set food_type = 'vegetable' %}
  {% endif %}

The humble {{ food }} is my favorite {{ food_type }}

{% endfor %}  

{# dictionaries in jinja #}

{% set websters_dict = {
    'word' : 'data',
    'speech_part' : 'noun',
    'definition' : 'if you know you know'
} %}

{{ websters_dict['word'] }}

{{ websters_dict['word'] }} ({{ websters_dict['speech_part'] }}): defined as "{{ websters_dict['definition'] }}"

{# can extend this to a list of dictionaries too #}

