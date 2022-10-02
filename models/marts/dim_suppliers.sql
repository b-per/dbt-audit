with

suppliers as (

    select * from {{ ref('stg_tpch_suppliers') }}

),

nations_regions as (

    select * from {{ ref('int_nations_regions') }}
),

final as (

    select 

        suppliers.supplier_id,
        suppliers.supplier_name,
        suppliers.supplier_address,
        suppliers.phone_number,
        suppliers.account_balance,
        
        nations_regions.nation,
        nations_regions.region

    from suppliers
    left join nations_regions
            on suppliers.nation_id = nations_regions.nation_id
)

select * from final