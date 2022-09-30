with

nations as (

    select * from {{ ref('stg_tpch_nations') }}

),

regions as (

    select * from {{ ref('stg_tpch_regions') }}

),

nations_regions as (

    select 

        -- ids
        nations.nation_id,
        nations.region_id,

        -- descriptions
        nations.nation,
        nations.nation_comment,

        regions.region,
        regions.region_comment

    from nations
    left join regions
        on nations.region_id = regions.region_id
            
)

select * from nations_regions