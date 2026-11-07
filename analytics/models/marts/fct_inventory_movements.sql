{{ config(materialized='table') }}

with inventory_events as (
    select
        event_id,
        occurred_at as movement_at,
        correlation_id,
        event_name,
        json_extract_string(attributes, '$.movement_type') as logged_movement_type,
        try_cast(
            json_extract_string(attributes, '$.product_id') as bigint
        ) as product_id,
        try_cast(
            json_extract_string(attributes, '$.order_id') as bigint
        ) as order_id,
        json_extract_string(attributes, '$.saga_id') as saga_id,
        json_extract_string(attributes, '$.reservation_id') as reservation_id,
        try_cast(
            json_extract_string(attributes, '$.quantity') as integer
        ) as quantity,
        try_cast(
            json_extract_string(attributes, '$.quantity_delta') as integer
        ) as quantity_delta
    from {{ ref('stg_raw_events') }}
    where event_name in (
        'inventory.stock.received',
        'inventory.stock.adjusted',
        'inventory.reservation.created',
        'inventory.reservation.released',
        'inventory.reservation.committed'
    )
),

classified as (
    select
        *,
        coalesce(
            logged_movement_type,
            case event_name
                when 'inventory.stock.received' then 'receipt'
                when 'inventory.stock.adjusted' then 'adjustment'
                when 'inventory.reservation.created' then 'reservation_created'
                when 'inventory.reservation.released' then 'reservation_released'
                when 'inventory.reservation.committed' then 'reservation_committed'
            end
        ) as movement_type
    from inventory_events
)

select
    event_id as movement_event_id,
    movement_at,
    cast(movement_at as date) as movement_date,
    correlation_id,
    product_id,
    order_id,
    saga_id,
    reservation_id,
    movement_type,
    case
        when movement_type = 'receipt' then coalesce(quantity, 0)
        when movement_type = 'adjustment' then coalesce(quantity_delta, 0)
        when movement_type = 'reservation_committed' then -coalesce(quantity, 0)
        else 0
    end as quantity_delta_on_hand,
    case
        when movement_type = 'reservation_created' then coalesce(quantity, 0)
        when movement_type in (
            'reservation_released',
            'reservation_committed'
        ) then -coalesce(quantity, 0)
        else 0
    end as quantity_delta_reserved
from classified
