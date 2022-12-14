with source as (

    select * from {{ source('tpch', 'lineitem') }}

),

renamed as (

    select

        -- numbers
        l_quantity as quantity,
        l_extendedprice as extended_price,
        l_discount as discount_percentage,
        l_tax as tax_rate,

        -- descriptions
        l_linenumber as line_number,
        l_comment as comment,
        l_shipmode as ship_mode,
        l_shipinstruct as ship_instructions,
        
        -- status
        l_linestatus as status_code,
        l_returnflag as return_flag,
        
        -- ids
        {{ dbt_utils.surrogate_key(
            ['l_orderkey', 
            'l_linenumber']) }}
                as order_item_id,
        l_orderkey as order_id,
        l_partkey as part_id,
        l_suppkey as supplier_id,
        
        -- dates
        l_shipdate as ship_date,
        l_commitdate as commit_date,
        l_receiptdate as receipt_date

    from source

)

select * from renamed