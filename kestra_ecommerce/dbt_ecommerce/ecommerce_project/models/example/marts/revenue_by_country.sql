select
    country,
    round(sum(revenue), 2) as total_revenue,
    count(distinct invoice_no) as total_orders

from {{ ref('stg_online_retail') }}

group by country
order by total_revenue desc