select *
from {{ source('tpch','customers') }}