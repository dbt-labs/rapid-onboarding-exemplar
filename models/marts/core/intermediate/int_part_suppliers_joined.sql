with part as (
    
    select * from {{ ref('stg_tpch_parts') }}

),

supplier as (

    select * from {{ ref('stg_tpch_suppliers') }}

),

part_supplier as (

    select * from {{ ref('stg_tpch_part_suppliers') }}

),

final as (
    select 

    part_supplier.part_supplier_id,
    part.part_id,
    part.name as part_name,
    part.manufacturer,
    part.brand,
    part.type as part_type,
    part.size as part_size,
    part.container,
    part.retail_price,

    supplier.supplier_id,
    supplier.supplier_name,
    supplier.supplier_address,
    supplier.phone_number,
    supplier.account_balance,
    supplier.nation_id,

    part_supplier.available_quantity,
    part_supplier.cost
from
    part 
inner join 
    part_supplier
        on part.part_id = part_supplier.part_id
inner join
    supplier
        on part_supplier.supplier_id = supplier.supplier_id
order by
    part.part_id
)

select * from final