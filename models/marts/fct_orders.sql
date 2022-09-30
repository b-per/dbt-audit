{{
    config(
        tags=['finance']
    )
}}

with

order_items as (
    
    select * from {{ ref('int_order_items') }}

),

order_item_summary as (

    select 

        order_id,
        sum(gross_item_sales_amount) as gross_item_sales_amount,
        sum(item_discount_amount) as item_discount_amount,
        sum(item_tax_amount) as item_tax_amount,
        sum(net_item_sales_amount) as net_item_sales_amount

    from order_items
    group by
        1
),

final as (

    select 

        order_items.order_id, 
        order_items.order_date,
        order_items.customer_id,
        order_items.order_status_code,
        order_items.priority_code,
        order_items.clerk_name,
        order_items.ship_priority,                      
        order_item_summary.gross_item_sales_amount,
        order_item_summary.item_discount_amount,
        order_item_summary.item_tax_amount,
        order_item_summary.net_item_sales_amount,
        1 as order_count

    from order_items
        inner join order_item_summary
            on order_items.order_id = order_item_summary.order_id
    order by order_date
)

select * from final