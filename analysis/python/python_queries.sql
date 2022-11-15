/*
Python Models Demo
====================

Python models similar to documentation examples
  https://docs.getdbt.com/docs/building-a-dbt-project/building-models/python-models#specific-data-warehouses

Run `dbt run -s python` to run your python models (in the models/python folder)
  then query these tables to show the output, as `Preview` does exist for Python models.

Use cases covered:
====================
*/

-- 1. using python builtin functions, like `.describe()`
select * from {{ ref('py01__python_builtins__describe') }} ;

-- 2. using variables from project in config.yml, like `target.name` 
  -- because you cannot declare or retrieve variables in the normal way
select * from {{ ref('py02__use_variables__customers_limit_10') }} ;

-- 3. importing pypi packages, like `holiday`
select * from {{ ref('py03__import_pypi_package__holiday') }} ;

-- 4. define a function within the model, like a function to `x*2`
select * from {{ ref('py04__define_function__payment_glitch') }} ;

-- 5. add a snowpark anonymous udf function to `add random int to x`
select * from {{ ref('py05__add_udf_function__payment_glitch') }} ;
