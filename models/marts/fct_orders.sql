{{
    config(
        tags=['finance']
    )
}}

with

orders as (
    
    select * from {{ ref('stg_tpch_orders') }}

),

final as (

    select 

        order_id, 
        customer_id,
        order_date,
        order_status_code,
        priority_code,
        ship_priority,
        clerk_name,                      
        1 as order_count

    from orders
    order by order_date
)

select * from final