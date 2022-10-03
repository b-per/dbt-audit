with

source as (

    select * from {{ source('tpch', 'nation') }}

),

renamed as (

    select

        -- ids
        n_nationkey as nation_id,
        n_regionkey as region_id,

        -- descriptions
        n_name as nation,
        n_comment as nation_comment

    from source

)

select * from renamed