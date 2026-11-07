{{ config(materialized='table') }}

with lifecycle_events as (
    select
        event_id,
        occurred_at,
        event_name,
        json_extract_string(attributes, '$.purchase_order_id')
            as purchase_order_id,
        try_cast(
            json_extract_string(attributes, '$.product_id') as bigint
        ) as product_id,
        try_cast(
            json_extract_string(attributes, '$.quantity') as integer
        ) as quantity,
        json_extract_string(attributes, '$.status') as status,
        json_extract_string(attributes, '$.receipt_id') as receipt_id,
        try_cast(
            json_extract_string(attributes, '$.lead_time_seconds') as integer
        ) as lead_time_seconds,
        try_cast(
            json_extract_string(attributes, '$.due_at') as timestamptz
        ) as due_at
    from {{ ref('stg_raw_events') }}
    where event_name in (
        'supplier.replenishment.evaluated',
        'supplier.purchase_order.ordered',
        'supplier.purchase_order.received',
        'supplier.purchase_order.cancelled'
    )
),

purchase_orders as (
    select
        purchase_order_id,
        max(product_id) as product_id,
        max(quantity) as quantity,
        min(
            case
                when event_name = 'supplier.replenishment.evaluated'
                    then occurred_at
            end
        ) as created_at,
        min(
            case
                when event_name = 'supplier.purchase_order.ordered'
                    then occurred_at
            end
        ) as ordered_at,
        min(
            case
                when event_name = 'supplier.purchase_order.received'
                    then occurred_at
            end
        ) as received_at,
        min(
            case
                when event_name = 'supplier.purchase_order.cancelled'
                    then occurred_at
            end
        ) as cancelled_at,
        max(due_at) as due_at,
        max(lead_time_seconds) as lead_time_seconds,
        max(receipt_id) as receipt_id,
        count(*) as lifecycle_event_count
    from lifecycle_events
    where purchase_order_id is not null
      and (
          event_name <> 'supplier.replenishment.evaluated'
          or json_extract_string(attributes, '$.decision')
              = 'purchase_order_created'
      )
    group by purchase_order_id
)

select
    purchase_order_id,
    product_id,
    quantity,
    case
        when received_at is not null then 'received'
        when cancelled_at is not null then 'cancelled'
        when ordered_at is not null then 'ordered'
        when created_at is not null then 'created'
        else 'unknown'
    end as lifecycle_status,
    created_at,
    ordered_at,
    due_at,
    received_at,
    cancelled_at,
    lead_time_seconds,
    receipt_id,
    lifecycle_event_count
from purchase_orders
