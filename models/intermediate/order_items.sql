{{
    config(
        tags=['intermediate'],
        alias='orders_items',
        schema='intermediate'
    )
}}

with orders as (
    
    select * from {{ target.schema }}.stg_tpch_orders

),

line_item as (

    select         
        {{ dbt_utils.surrogate_key(
        ['l_orderkey', 
        'l_linenumber']) }}
            as order_item_id,
        l_orderkey as order_id,
        l_partkey as part_id,
        l_suppkey as supplier_id,
        l_linenumber as line_number,
        l_comment as comment,
        l_shipmode as ship_mode,
        l_shipinstruct as ship_instructions,
        l_linestatus as status_code,
        l_returnflag as return_flag,
        l_shipdate as ship_date,
        l_commitdate as commit_date,
        l_receiptdate as receipt_date,
        l_quantity as quantity,
        l_extendedprice as extended_price,
        l_discount as discount_percentage,
        l_tax as tax_rate
    from {{ source('tpch', 'lineitem') }}

)
select 

    line_item.order_item_id,
    orders.order_id,
    orders.customer_id,
    line_item.part_id,
    line_item.supplier_id,
    orders.order_date,
    orders.status_code as order_status_code,
    
    
    line_item.return_flag,
    
    line_item.line_number,
    line_item.status_code as order_item_status_code,
    line_item.ship_date,
    line_item.commit_date,
    line_item.receipt_date,
    line_item.ship_mode,
    line_item.extended_price,
    line_item.quantity,
    
    -- extended_price is actually the line item total,
    -- so we back out the extended price per item
    (line_item.extended_price/nullif(line_item.quantity, 0)){{ money() }} as base_price,
    line_item.discount_percentage,
    (base_price * (1 - line_item.discount_percentage)){{ money() }} as discounted_price,

    line_item.extended_price as gross_item_sales_amount,
    (line_item.extended_price * (1 - line_item.discount_percentage)){{ money() }} as discounted_item_sales_amount,
    -- We model discounts as negative amounts
    (-1 * line_item.extended_price * line_item.discount_percentage){{ money() }} as item_discount_amount,
    line_item.tax_rate,
    ((gross_item_sales_amount + item_discount_amount) * line_item.tax_rate){{ money() }} as item_tax_amount,
    (
        gross_item_sales_amount + 
        item_discount_amount + 
        item_tax_amount
    ){{ money() }} as net_item_sales_amount

from
    orders
inner join line_item
        on orders.order_id = line_item.order_id
order by
    orders.order_date