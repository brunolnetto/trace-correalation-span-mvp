{{ config(materialized='table') }}

select
    cast(started_at as date) as order_date,
    count(*) as attempted_orders,
    count(*) filter (where outcome = 'processed') as processed_orders,
    count(*) filter (where outcome = 'payment_declined') as payment_declined_orders,
    count(*) filter (where outcome = 'inventory_rejected') as inventory_rejected_orders,
    count(*) filter (where outcome not in (
        'processed',
        'payment_declined',
        'inventory_rejected'
    )) as other_outcomes,
    sum(total_amount) filter (where outcome = 'processed') as processed_amount,
    avg(processing_time_ms) as average_processing_time_ms,
    approx_quantile(processing_time_ms, 0.95) as p95_processing_time_ms
from {{ ref('fct_orders') }}
where started_at is not null
group by cast(started_at as date)
