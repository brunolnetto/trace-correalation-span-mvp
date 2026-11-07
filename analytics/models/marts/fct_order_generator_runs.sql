{{ config(materialized='table') }}

select
    event_id,
    occurred_at as generated_at,
    json_extract_string(attributes, '$.run_id') as run_id,
    json_extract_string(attributes, '$.plan_id') as plan_id,
    json_extract_string(attributes, '$.mode') as mode,
    try_cast(json_extract_string(attributes, '$.seed') as bigint) as seed,
    try_cast(json_extract_string(attributes, '$.product_id') as bigint)
        as product_id,
    try_cast(json_extract_string(attributes, '$.planned') as integer) as planned,
    try_cast(json_extract_string(attributes, '$.attempted') as integer) as attempted,
    try_cast(json_extract_string(attributes, '$.processed') as integer) as processed,
    try_cast(
        json_extract_string(attributes, '$.payment_declined') as integer
    ) as payment_declined,
    try_cast(
        json_extract_string(attributes, '$.inventory_rejected') as integer
    ) as inventory_rejected,
    try_cast(
        json_extract_string(attributes, '$.unexpected_failures') as integer
    ) as unexpected_failures,
    try_cast(
        json_extract_string(attributes, '$.latency_ms.mean') as double
    ) as mean_latency_ms,
    try_cast(
        json_extract_string(attributes, '$.latency_ms.p50') as double
    ) as p50_latency_ms,
    try_cast(
        json_extract_string(attributes, '$.latency_ms.p95') as double
    ) as p95_latency_ms,
    try_cast(
        json_extract_string(attributes, '$.latency_ms.max') as double
    ) as max_latency_ms,
    try_cast(json_extract_string(attributes, '$.exit_code') as integer)
        as exit_code
from {{ ref('stg_raw_events') }}
where event_name = 'order_generator.summary'
