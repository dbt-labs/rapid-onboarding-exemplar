--snowflake functionality to demo how SQL can be used to parse json in a single column 

with json_example as (
select * from raw.tpch_json.customer_json
),

--use the snowflake parse_json() function
json_to_parse as (
  select parse_json(cust_json) as json_to_parse_column
  from json_example
),

--extract the key value pairs into columns
json_parsed as (
select json_to_parse_column:c_name::string as c_name,
json_to_parse_column:c_custkey::string as c_key,
json_to_parse_column:c_acctbal::float as c_accbal,
json_to_parse_column:c_mktsegment::string as c_mktsegment,
json_to_parse_column:c_nationkey::string as c_nationkey,
json_to_parse_column:c_comment::string as c_comment,
json_to_parse_column:c_address::string as c_address,
json_to_parse_column:c_phone::int as c_phone

from json_to_parse
),

--convert data types
final as (
  select c_name as customer_name,
  c_custkey as customer_key,
  c_acctbal as customer_account_balance,
  c_mktsegment as customer_market_segment,
  c_nationkey as customer_nation_key,
  c_comment as customer_comment,
  c_address as customer_address,
  c_phone as customer_phone_number
  from json_parsed
)

select * from final