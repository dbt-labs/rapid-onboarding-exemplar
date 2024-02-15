-- solution #2 (dbt Logic)
-- leverage dbt logic
-- can be built as a table or view
-- a little bit more gnarly to manage the logic with types and casting, more lines of code
-- all handled within dbt

{% set sql_statement_1 %}
    select to_date(max(date_day)) from {{ ref('date_spine') }}
{% endset %}

{% set postdate = dbt_utils.get_single_value(sql_statement_1) %}

{% set sql_statement_2 %}
    select last_day(add_months('{{ postdate }}', -1))
{% endset %}

{% set thrudate = dbt_utils.get_single_value(sql_statement_2) %}

select 
    to_date('{{ postdate }}') as original_date,
    to_date('{{ thrudate }}') as previous_date
