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

nations_regions as (

    select * from {{ ref('int_nations_regions') }}
),

final as (

    select 

        customer.customer_id,
        customer.name,
        customer.address,
        customer.phone_number,
        customer.account_balance,
        customer.market_segment,

        nations_regions.nation_id,
        nations_regions.nation,
        
        nations_regions.region_id,
        nations_regions.region    

    from customer
    left join nations_regions
        on customer.nation_id = nations_regions.nation_id
    order by 1
)

select * from final