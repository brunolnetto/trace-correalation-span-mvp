{{ config(materialized='table') }}

select
    movement_date,
    product_id,
    count(*) as movement_events,
    sum(quantity_delta_on_hand) as net_on_hand_delta,
    sum(quantity_delta_reserved) as net_reserved_delta,
    sum(
        case when movement_type = 'receipt'
            then quantity_delta_on_hand else 0 end
    ) as units_received,
    sum(
        case when movement_type = 'adjustment'
            then quantity_delta_on_hand else 0 end
    ) as adjustment_delta,
    sum(
        case when movement_type = 'reservation_created'
            then quantity_delta_reserved else 0 end
    ) as units_reserved,
    sum(
        case when movement_type = 'reservation_released'
            then -quantity_delta_reserved else 0 end
    ) as units_released,
    sum(
        case when movement_type = 'reservation_committed'
            then -quantity_delta_on_hand else 0 end
    ) as units_committed
from {{ ref('fct_inventory_movements') }}
group by movement_date, product_id
