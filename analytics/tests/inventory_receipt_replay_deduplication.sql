select
    receipt_id,
    count(*) as movement_rows
from {{ ref('fct_inventory_movements') }}
where receipt_id is not null
group by receipt_id
having count(*) > 1
