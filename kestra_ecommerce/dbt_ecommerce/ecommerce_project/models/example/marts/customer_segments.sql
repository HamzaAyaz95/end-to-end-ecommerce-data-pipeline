with base as (

    select *
    from {{ ref('stg_online_retail') }}

),

customer_metrics as (

    select
        customer_id,
        country,
        count(distinct invoice_no) as total_orders,
        sum(revenue) as total_revenue,
        avg(revenue) as avg_order_value

    from base
    where customer_id is not null
    group by customer_id, country

),

segmented as (

    select *,
        case
            when total_revenue > 5000 then 'high_value'
            when total_revenue between 1000 and 5000 then 'mid_value'
            else 'low_value'
        end as customer_segment

    from customer_metrics

)

select
    country,
    customer_segment,
    count(*) as num_customers,
    sum(total_revenue) as segment_revenue,
    round(avg(avg_order_value), 2) as avg_order_value

from segmented
group by country, customer_segment
order by segment_revenue desc