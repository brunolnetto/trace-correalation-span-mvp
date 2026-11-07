{{ config(materialized='table') }}

select
    cast(coalesce(created_at, ordered_at, received_at) as date) as activity_date,
    count(*) as purchase_orders_created,
    count(*) filter (where lifecycle_status = 'ordered') as purchase_orders_ordered,
    count(*) filter (where lifecycle_status = 'received') as purchase_orders_received,
    count(*) filter (where lifecycle_status = 'cancelled') as purchase_orders_cancelled,
    sum(quantity) filter (where lifecycle_status = 'received') as received_units,
    avg(lead_time_seconds) as average_lead_time_seconds
from {{ ref('fct_supplier_purchase_orders') }}
group by cast(coalesce(created_at, ordered_at, received_at) as date)
