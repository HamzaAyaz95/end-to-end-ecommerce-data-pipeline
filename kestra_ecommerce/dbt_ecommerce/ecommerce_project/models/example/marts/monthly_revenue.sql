SELECT
  DATE_TRUNC(invoice_date, MONTH) AS month,
  SUM(revenue) AS total_revenue,
  COUNT(DISTINCT invoice_no) AS total_orders
FROM {{ ref('stg_online_retail') }}
GROUP BY month
ORDER BY month