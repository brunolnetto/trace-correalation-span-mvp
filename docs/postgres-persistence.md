# PostgreSQL persistence foundation

This document records the implementation boundary for issue #21. PostgreSQL becomes the source of truth for the Order and Inventory domains; Event Collector JSONL files and dbt models remain observability/analytics products, never write authorities.

## Decisions

- Use one PostgreSQL database for the two services. Do not introduce a generic multi-database abstraction.
- Keep domain validation and state transitions in the existing InventoryStore/Order workflow layer. Introduce repository interfaces and PostgreSQL implementations beneath those services; SQL and connection management must not leak into route handlers or Saga actions.
- Use explicit, versioned SQL migrations. A migration ledger records each applied migration; applying migrations to an empty database must produce the same schema as upgrading an existing database.
- Use database transactions for each aggregate write in this issue. This is required to keep a balance change, its movement record, and its idempotency result together. The separate #22 work remains responsible for the stronger multi-process inventory concurrency contract and its dedicated competing-process tests.
- Store monetary values as NUMERIC, quantities as integers, timestamps as TIMESTAMPTZ, and idempotency responses as JSONB.
- Keep the movement journal append-only. Corrections are compensating movements, never edits to historical rows.
- Demo data is a separate, explicit bootstrap operation. A normal production startup only applies migrations; it never inserts catalog or stock records.

## Persistence model

| Table | Responsibility |
| --- | --- |
| products | SKU, descriptive/catalog fields, active status, timestamps |
| inventory_balances | Current on_hand and reserved projection, one row per product |
| reservations | Reservation identity, order/Saga association, quantity, lifecycle status |
| inventory_movements | Append-only deltas for physical and reserved quantities |
| idempotency_records | Scoped key, canonical request hash, and replayable response |
| orders | Durable order input, catalog price snapshot, outcome, Saga/payment/reservation references |

inventory_balances is an operational projection. For every product, on_hand must equal the sum of quantity_delta_on_hand across its movement journal, and reserved must equal the sum of quantity_delta_reserved. The repository layer exposes reconciliation to tests/diagnostics; it does not silently repair drift.

## Write boundaries

A product create inserts the product and its zero balance in one transaction. A receipt or adjustment writes its movement, updates the balance, and stores its idempotency response in one transaction. A reservation transition writes the reservation state, balance projection, and movement in one transaction. A completed/failed order outcome is persisted through an Order repository; the existing payment simulator remains a process-local test/demo adapter and is not expanded into a payment ledger by #21.

The idempotency key is unique within an operation scope. The canonical request hash distinguishes a legitimate replay from reuse of the same key with a different payload. Existing API response shapes and HTTP status semantics are preserved.

## Migration and bootstrap lifecycle

1. Configure DATABASE_URL explicitly for each API.
2. Run the migration command as a separate deployment/development step; API startup does not mutate schema.
3. For local tests, start a disposable PostgreSQL service and apply migrations.
4. Run the explicit demo bootstrap only when INVENTORY_SEED_DEMO_DATA=true (or an explicit bootstrap command is invoked). It must be safe to re-run and must not overwrite an existing product or balance.
5. Production configuration must fail fast when PostgreSQL is missing or unreachable. There is no implicit in-memory fallback.

## Implementation sequence

1. Schema and migration ledger, database configuration/lifecycle, and repository contracts.
2. Inventory repository implementation and API wiring while preserving the existing in-memory test double.
3. Order repository implementation and persistence of order outcomes.
4. Explicit demo bootstrap and README operations guide.
5. PostgreSQL integration tests for create/read/update/restart, movement immutability, idempotency replay/conflict, and balance/journal reconciliation.

#22 remains a separate milestone: tests that coordinate at least two Inventory API processes and prove no overselling under contention. #23 remains separate: supplier policy and purchase-order persistence.
