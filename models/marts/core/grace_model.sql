  {%- for i in range(1, 10) %}
      select {{ i }} as my_column
  
      {%- if not loop.last %}
          union all 
      {% endif %}
  
  {%- endfor %}