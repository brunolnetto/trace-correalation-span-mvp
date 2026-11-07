{{ config(materialized='table') }}

select
    event_id as decision_event_id,
    occurred_at as evaluated_at,
    try_cast(
        json_extract_string(attributes, '$.product_id') as bigint
    ) as product_id,
    json_extract_string(attributes, '$.decision') as decision,
    try_cast(
        json_extract_string(attributes, '$.inventory_position') as integer
    ) as inventory_position,
    json_extract_string(attributes, '$.purchase_order_id') as purchase_order_id,
    try_cast(json_extract_string(attributes, '$.quantity') as integer)
        as purchase_quantity,
    try_cast(json_extract_string(attributes, '$.reorder_point') as integer)
        as reorder_point,
    try_cast(json_extract_string(attributes, '$.target_stock') as integer)
        as target_stock,
    try_cast(
        json_extract_string(attributes, '$.lead_time_seconds') as integer
    ) as lead_time_seconds,
    try_cast(json_extract_string(attributes, '$.policy_version') as integer)
        as policy_version
from {{ ref('stg_raw_events') }}
where event_name = 'supplier.replenishment.evaluated'
