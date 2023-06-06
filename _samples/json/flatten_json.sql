--snowflake functionality to demo how SQL can be used to parse json in a single column 

with json_example as (
select * from {{ref('json_example')}}
),

--use the snowflake parse_json() function
json_to_parse as (
  select parse_json(column_1) as json_to_parse_column
  from json_example
),

--extract the key value pairs into columns
json_parsed as (
select json_to_parse_column:product::string as product,
json_to_parse_column:order_date::string as order_date,
json_to_parse_column:price::float as price
from json_to_parse
),

--convert data types
final as (
  select product, 
  TO_DATE(order_date,'MM-DD-YYYY') as order_date,
  price
  from json_parsed
)

select * from final