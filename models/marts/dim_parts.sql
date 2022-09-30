{{
    config(
        materialized = 'incremental'
    )
}}

with

part as (

    select * from {{ref('stg_tpch_parts')}}

),

final as (

    select 

        part_id,
        manufacturer,
        name,
        brand,
        type,
        size,
        container,
        retail_price

    from part
    order by 1
)

select * from final  