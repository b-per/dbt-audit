with source as (

    select * from {{ source('tpch', 'region') }}

),

renamed as (

    SELECT
        source.r_regionkey as region_id,
        source.r_name as name,
        source.r_comment as comment

    from source

)

select * from renamed