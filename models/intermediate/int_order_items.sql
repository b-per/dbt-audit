{{
    config(
        tags=['intermediate'],
        alias='orders_items',
        schema='intermediate'
    )
}}

with

line_items as (

    select * from {{ ref('stg_tpch_line_items') }}

),

final as (

    select 

        -- ids
        line_items.order_item_id,
        line_items.part_id,
        line_items.supplier_id,
        
        -- dates
        line_items.ship_date,
        line_items.commit_date,
        line_items.receipt_date,

        -- status
        line_items.return_flag,
        line_items.order_item_status_code,
        
        -- descriptions
        line_items.line_number,
        line_items.ship_mode,

        -- numbers
        line_items.extended_price,
        line_items.quantity,
        
        -- extended_price is actually the line item total,
        -- so we back out the extended price per item
        line_items.discount_percentage,
        line_items.extended_price as gross_item_sales_amount,
        (line_items.extended_price/nullif(line_items.quantity, 0)){{ money() }} as base_price,
        (base_price * (1 - line_items.discount_percentage)){{ money() }} as discounted_price,
        (line_items.extended_price * (1 - line_items.discount_percentage)){{ money() }} as discounted_item_sales_amount,

        -- We model discounts as negative amounts
        line_items.tax_rate,
        (-1 * line_items.extended_price * line_items.discount_percentage){{ money() }} as item_discount_amount,
        ((gross_item_sales_amount + item_discount_amount) * line_items.tax_rate){{ money() }} as item_tax_amount,
        (
            gross_item_sales_amount + 
            item_discount_amount + 
            item_tax_amount
        ){{ money() }} as net_item_sales_amount

    from line_items
    order by 3
)

select * from final