select
  customer_id,
  sum(gross_item_sales_amount) as total_sales
from {{ ref('orders') }}
GROUP BY 1 
order by total_sales desc