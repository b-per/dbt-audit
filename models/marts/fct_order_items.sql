{{
    config(
        tags = ['finance']
    )
}}

with

order_items as (
    
    select * from {{ ref('int_order_items') }}

),

part_suppliers as (
    
    select * from {{ ref('stg_tpch_part_suppliers') }}

),

final as (

    select 
        order_items.order_item_id,
        order_items.part_id,
        order_items.supplier_id,
        order_items.order_item_status_code,
        order_items.return_flag,
        order_items.line_number,
        order_items.ship_date,
        order_items.commit_date,
        order_items.receipt_date,
        order_items.ship_mode,

        part_suppliers.cost as supplier_cost,
        part_suppliers.available_quantity,
        part_suppliers.cost,

        order_items.base_price,
        order_items.discount_percentage,
        order_items.discounted_price,
        order_items.tax_rate,
        1 as order_item_count,
        order_items.quantity,
        order_items.gross_item_sales_amount,
        order_items.discounted_item_sales_amount,
        order_items.item_discount_amount,
        order_items.item_tax_amount,
        order_items.net_item_sales_amount

    from order_items
    left join part_suppliers
        on order_items.part_id = part_suppliers.part_id and
           order_items.supplier_id = part_suppliers.supplier_id
    order by 3
)

select * from final
