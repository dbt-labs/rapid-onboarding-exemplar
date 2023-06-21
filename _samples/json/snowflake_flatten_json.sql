--snowflake functionality to demo how SQL can be used to parse json in a single column 

with json_example as (
select parse_json(cust_json::variant) as cust_json from raw.tpch_json.customer_json
),

--use the snowflake parse_json() function
json_to_parse as (
  select parse_json(cust_json) as json_to_parse_column
  from json_example
),

--extract the key value pairs into columns
json_parsed as (
select json_to_parse_column:C_NAME::string as c_name,
json_to_parse_column:C_CUSTKEY::string as c_custkey,
json_to_parse_column:C_ACCTBAL::float as c_acctbal,
json_to_parse_column:C_MKTSEGMENT::string as c_mktsegment,
json_to_parse_column:C_NATIONKEY::string as c_nationkey,
json_to_parse_column:C_COMMENT::string as c_comment,
json_to_parse_column:C_ADDRESS::string as c_address,
json_to_parse_column:C_PHONE::string as c_phone

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