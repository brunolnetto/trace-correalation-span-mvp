{{ config(materialized='table') }}

select
    event_id,
    occurred_at as generated_at,
    correlation_id,
    json_extract_string(attributes, '$.run_id') as run_id,
    json_extract_string(attributes, '$.mode') as mode,
    try_cast(json_extract_string(attributes, '$.seed') as bigint) as seed,
    try_cast(json_extract_string(attributes, '$.order_id') as bigint)
        as order_id,
    try_cast(json_extract_string(attributes, '$.product_id') as bigint)
        as product_id,
    try_cast(json_extract_string(attributes, '$.quantity') as integer)
        as quantity,
    json_extract_string(attributes, '$.scenario') as scenario,
    json_extract_string(attributes, '$.expected_outcome') as expected_outcome,
    json_extract_string(attributes, '$.outcome') as outcome,
    try_cast(json_extract_string(attributes, '$.http_status') as integer)
        as http_status,
    try_cast(json_extract_string(attributes, '$.latency_ms') as double)
        as latency_ms,
    json_extract_string(attributes, '$.error') as error
from {{ ref('stg_raw_events') }}
where event_name = 'order_generator.attempt'
