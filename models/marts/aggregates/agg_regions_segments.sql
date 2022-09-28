{{
    config(
        materialized='table'
    )
}}

with

customers as (
    select * from {{ ref('dim_customers') }}
),

orders as (
    select * from {{ ref('orders') }}
),

orders2 as (
    select
        customer_id,
        sum(net_item_sales_amount) as total_sales
    from orders
    group by customer_id
),

final as (
    select
        customers.region,
        customers.market_segment,
        round(sum(agg_orders.total_sales),2) as total_sales
    from customers
    join orders2 agg_orders on customers.customer_id = agg_orders.customer_id
    group by 1, market_segment
)

select * from final