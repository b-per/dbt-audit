{{
    config(
        materialized = 'incremental',
        unique_id='customer_id',
        transient=false
    )
}}

with customer as (

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
        customer_id,
        customer.name,
        address,
        -- nation.nation_id as nation_id,
        nation.name as nation,
        {# region.region_id as region_id, #}
        region.name as region,
        phone_number,
        account_balance,
        market_segment
    from
        customer
        inner join nation
            on customer.nation_id = nation.nation_id
        inner join region on nation.region_id = region.region_id
)
select 
    *
from
    final
order by
    customer_id