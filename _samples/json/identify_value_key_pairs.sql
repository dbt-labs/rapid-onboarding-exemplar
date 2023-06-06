-- this will provide you with every key value pair, which will then be leverage in the stg_model

select distinct path
from {{ source('source_name', 'object_name') }}, 
        lateral flatten(my_column, recursive=>true) f