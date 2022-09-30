with

supplier as (

    select * from {{ ref('stg_tpch_suppliers') }}

),

nations_regions as (

    select * from {{ ref('int_nations_regions') }}
),

final as (

    select 

        supplier.supplier_id,
        supplier.supplier_name,
        supplier.supplier_address,
        
        nations_regions.nation,

        nations_regions.region,
        
        supplier.phone_number,
        supplier.account_balance

    from supplier
    inner join nations_regions
            on supplier.nation_id = nations_regions.nation_id
)

select * from final