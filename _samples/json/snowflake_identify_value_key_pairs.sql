-- this will provide you with every key value pair using Snowflake functionality, which will then be leverage in the stg_model
-- flatten docs: https://docs.snowflake.com/en/sql-reference/functions/flatten

select distinct path
from raw.tpch_json.customer_json, 
        lateral flatten(cust_json, recursive=>true) f