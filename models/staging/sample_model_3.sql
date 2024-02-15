-- solution #3 (dbt Macro)
-- leverage dbt logic
-- can be built as a table or view
-- a little bit more gnarly to manage the logic with types and casting, more lines of code
-- all handled within dbt

{% set max_date = max_date_macro(ref('date_spine'), 'date_day') %}
{% set my_var_2 = date_diff_macro(max_date, -1) %}

select 
    to_date('{{ max_date }}') as original_date,
    to_date('{{ my_var_2 }}') as previous_date
