with

source as (

    select * from {{ source('tpch', 'region') }}

),

renamed as (

    select

        -- ids
        r_regionkey as region_id,

        -- descriptions
        r_name as region,
        r_comment as region_comment

    from source

)

select * from renamed