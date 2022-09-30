{{
    config(
        materialized = 'incremental',
        unique_id='customer_id',
        transient=false
    )
}}

with

customer as (

    select * from {{ ref('stg_tpch_customers') }}

),

nation as (

    select * from {{ ref('stg_tpch_nations') }}
),

region as (

    select * from {{ ref('stg_tpch_regions') }}

),

final as (

    select 

        customer.customer_id,
        customer.name,
        customer.address,

        nation.nation_id,
        nation.name as nation,
        
        region.region_id as region_id,
        region.name as region,
        region.phone_number,
        region.account_balance,
        region.market_segment

    from
        customer
        inner join nation
            on customer.nation_id = nation.nation_id
        inner join region
            on nation.region_id = region.region_id
    order by 1
)

select * from final