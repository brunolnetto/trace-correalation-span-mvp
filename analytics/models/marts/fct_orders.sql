{{ config(materialized='table') }}

with extracted as (
    select
        correlation_id,
        event_name,
        occurred_at,
        service,
        trace_id,
        json_extract_string(attributes, '$.order_id') as order_id_text,
        json_extract_string(attributes, '$.product_id') as product_id_text,
        json_extract_string(attributes, '$.quantity') as quantity_text,
        json_extract_string(attributes, '$.saga_id') as saga_id,
        json_extract_string(attributes, '$.reservation_id') as reservation_id,
        json_extract_string(attributes, '$.payment_id') as payment_id,
        json_extract_string(attributes, '$.total_amount') as total_amount_text,
        json_extract_string(attributes, '$.failure_category')
            as failure_category,
        json_extract_string(source, '$.exception') as exception
    from {{ ref('stg_raw_events') }}
    where correlation_id is not null
),

order_events as (
    select
        *,
        try_cast(order_id_text as bigint) as order_id,
        try_cast(product_id_text as bigint) as product_id,
        try_cast(quantity_text as integer) as quantity,
        try_cast(total_amount_text as decimal(18, 2)) as total_amount
    from extracted
    where event_name like 'order.%'
       or event_name like 'inventory.reservation.%'
),

aggregated as (
    select
        correlation_id,
        max(order_id) as order_id,
        max(product_id) as product_id,
        max(quantity) as quantity,
        max(saga_id) as saga_id,
        max(reservation_id) as reservation_id,
        max(payment_id) as payment_id,
        max(total_amount) as total_amount,
        min(
            case
                when event_name = 'order.process.started' then occurred_at
            end
        ) as started_at,
        max(
            case
                when event_name = 'order.process.completed' then occurred_at
            end
        ) as completed_at,
        max(
            case
                when event_name = 'order.process.failed' then occurred_at
            end
        ) as failed_at,
        max(
            case
                when event_name = 'order.process.completed' then 1
                else 0
            end
        ) as was_completed,
        max(
            case
                when event_name = 'order.process.failed' then 1
                else 0
            end
        ) as was_failed,
        max(
            case
                when event_name = 'inventory.reservation.rejected' then 1
                else 0
            end
        ) as was_inventory_rejected,
        max(failure_category) as failure_category,
        count(*) as event_count,
        count(distinct trace_id) as trace_count,
        string_agg(distinct service, ',') as source_services
    from order_events
    group by correlation_id
)

select
    correlation_id,
    order_id,
    product_id,
    quantity,
    saga_id,
    reservation_id,
    payment_id,
    total_amount,
    case
        when was_completed = 1 then 'processed'
        when failure_category is not null then failure_category
        when was_inventory_rejected = 1 then 'inventory_rejected'
        when was_failed = 1 then 'failed'
        else 'incomplete'
    end as outcome,
    started_at,
    completed_at,
    failed_at,
    case
        when started_at is not null
         and coalesce(completed_at, failed_at) is not null
        then date_diff(
            'millisecond',
            started_at,
            coalesce(completed_at, failed_at)
        )
    end as processing_time_ms,
    event_count,
    trace_count,
    source_services
from aggregated
