{%- call statement('result', fetch_result=True) -%}

                    select ship_mode from {{ ref('fct_order_items') }} group by 1 

{%- endcall %}

{% set ship_modes = load_result('result').table.columns[0].values() %}

select
    date_part('year', order_date) as order_year, {%- for ship_mode in ship_modes -%}
SUM(CASE WHEN ship_mode = '{{ship_mode}}' THEN gross_item_sales_amount END) as "{{ship_mode|replace(' ', '_')}}_amount"
{%- if not loop.last -%},{% endif %}
    {% endfor %}
from {{ ref('fct_order_items') }}
group by 1