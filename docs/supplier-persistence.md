# Supplier persistence and recovery

## State ownership

The Supplier Simulator owns supplier-facing state:

- fixture supplier offerings and their inventory-SKU mapping;
- the supplier-to-Inventory product onboarding reference;
- reorder policies with monotonically increasing versions (POs retain the version snapshot);
- purchase orders, line snapshots, and lifecycle transition history;
- the deterministic simulation clock used for lead-time calculations.

The Inventory API remains the sole owner of product catalog and stock state. The
Supplier Simulator may create or read products through its public API and may
request a stock receipt through `POST /inventory/{product_id}/receipts`. It
must not connect to, query, or mutate Inventory tables directly.

Order, Inventory, and Supplier APIs use the shared PostgreSQL database through independent connection pools.
The Supplier Simulator uses the same `PERSISTENCE_BACKEND`,
`DATABASE_URL`, and pool configuration as the Order and Inventory APIs.
Migrations are applied by the existing migration command before the services
start; application startup does not apply schema changes.

## Purchase-order lifecycle

The persisted lifecycle is:

```text
created -> ordered -> receiving -> received
    |
    +---------------------------> cancelled
```

Only a `created` PO can be cancelled. An `ordered` PO can enter
`receiving` after its persisted due time. A `receiving` PO is deliberately
retryable, while `received` and `cancelled` are terminal.

Every state change is committed with an append-only row in
`supplier_purchase_order_transitions`. The Supplier API exposes the history
at:

```http
GET /purchase-orders/{purchase_order_id}/transitions
```

The current state and timestamps are returned by the existing PO endpoints.
Transition history is protected against UPDATE, DELETE, and TRUNCATE.

## Duplicate replenishment protection

Each replenishment evaluation reads the authoritative Inventory API snapshot,
then locks that product's reorder-policy row while checking active POs and
creating a PO. This serializes evaluations for one product across independent
Supplier Simulator processes.

PostgreSQL also enforces the invariant with a partial unique index on
`supplier_purchase_orders(product_id)` for statuses `created`, `ordered`,
and `receiving`. The database constraint is the final guard; correctness does
not depend on an in-process asyncio lock.

POs retain the inventory position, on-hand, reserved, outstanding-order,
policy-version, and threshold values observed when the order was created.
Their line rows retain the supplier SKU, inventory SKU, description, quantity,
and unit-price snapshot. These snapshots make historical POs interpretable
after supplier offerings or policies change.

## Receipt protocol and crash recovery

Receipt spans two services and cannot be one PostgreSQL transaction. The
Supplier Simulator therefore uses a persisted intent and Inventory-owned
idempotency:

1. Lock the PO and persist `ordered -> receiving` in a committed transaction.
2. Call Inventory API with the stable key
   `supplier-po:<purchase_order_id>` and the PO quantity.
3. After Inventory returns its receipt ID, persist
   `receiving -> received` and the receipt ID in another transaction.

If the Supplier process crashes before the HTTP request, the next process sees
`receiving` and can retry it. If Inventory commits but its response is lost,
the PO also remains `receiving`; a retry sends exactly the same idempotency
key and payload. Inventory returns the already-committed receipt rather than
crediting stock again. The Supplier then records `received`.

The PO remains active while `receiving`, so another replenishment evaluation
cannot create a replacement PO during an ambiguous receipt outcome. After
`received` commits, the active-PO index no longer considers it outstanding,
and future decisions use a fresh Inventory API balance.

This is retry/reconciliation by replay, not a distributed transaction. The
Supplier must never mark a PO received before it has an Inventory receipt ID.

## Schema

Migration `0004_persist_supplier_state.sql` adds:

- `supplier_offerings`;
- `supplier_product_onboardings`;
- `supplier_reorder_policies`;
- `supplier_purchase_orders`;
- `supplier_purchase_order_lines`;
- `supplier_purchase_order_transitions`;
- `supplier_simulation_clock`.

Previously applied migrations remain immutable. The supplier schema is
applied through the normal migration runner.
