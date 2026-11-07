{{ config(materialized='table') }}

select
    attempts.run_id,
    attempts.correlation_id,
    attempts.order_id,
    attempts.product_id,
    attempts.quantity,
    attempts.expected_outcome,
    attempts.outcome as generator_outcome,
    orders.outcome as service_outcome,
    case
        when orders.correlation_id is null then 'missing_service_events'
        when attempts.outcome = orders.outcome then 'matched'
        else 'outcome_mismatch'
    end as reconciliation_status,
    attempts.http_status,
    attempts.latency_ms,
    orders.processing_time_ms,
    orders.trace_count
from {{ ref('fct_order_generator_attempts') }} as attempts
left join {{ ref('fct_orders') }} as orders
    on attempts.correlation_id = orders.correlation_id
