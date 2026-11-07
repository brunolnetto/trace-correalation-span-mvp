select *
from {{ ref('fct_order_reconciliation') }}
where reconciliation_status <> 'matched'
