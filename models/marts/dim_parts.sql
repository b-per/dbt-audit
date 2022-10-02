{{
    config(
        materialized = 'incremental'
    )
}}

with

parts as (

    select * from {{ref('stg_tpch_parts')}}

),

final as (

    select 

        part_id,
        manufacturer,
        part_name,
        brand,
        part_type,
        part_size,
        container,
        retail_price

    from parts
    order by 1
)

select * from final  