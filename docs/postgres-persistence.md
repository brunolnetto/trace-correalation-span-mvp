# PostgreSQL persistence foundation

This document tracks issue #21. PostgreSQL is the durable source of truth for
Order, Inventory, and Supplier business state. The Event Collector and dbt remain observability and analytics
products, never write authorities.

## Decisions

- Use one PostgreSQL database for the Order, Inventory, and Supplier domains.
  Do not introduce a generic multi-database abstraction.
- Keep HTTP routes independent of SQL and connection management. Both the
  in-memory `InventoryStore` and `PostgresInventoryStore` implement the
  application-facing `InventoryStorePort`.
- Keep shared product normalization, reservation eligibility, stock adjustment,
  and commit-balance validation in `app/inventory/rules.py`. The PostgreSQL
  adapter owns SQL and transaction boundaries; the in-memory adapter remains
  available as the unit-test double.
- Use explicit, versioned SQL migrations. The Python migration runner creates
  and owns `schema_migrations`, takes a transaction-scoped advisory lock, and
  records the SHA-256 checksum of each applied SQL file. Applied migrations
  are immutable.
- Each Inventory command that changes business state uses one transaction.
  Balance projections, reservation state, movement rows, and idempotency
  responses are committed or rolled back together.
- Store monetary values as NUMERIC, quantities as integers, timestamps as
  TIMESTAMPTZ, and replayable idempotency responses as JSONB.
- Keep the movement journal append-only. Corrections are compensating movements,
  never edits to historical rows.
- Demo data is a separate, explicit bootstrap. A normal production startup
  only opens the connection pool and checks database readiness; it does not
  apply schema changes or seed catalog/stock records.

## Persistence model

| Table | Responsibility |
| --- | --- |
| products | SKU, descriptive/catalog fields, active status, timestamps |
| inventory_balances | Current on_hand and reserved projection, one row per product |
| reservations | Reservation identity, order/Saga association, quantity, lifecycle status |
| inventory_movements | Append-only deltas for physical and reserved quantities |
| idempotency_records | Scoped key, canonical request hash, and replayable response |
| orders | Durable order input, catalog price snapshot, outcome, Saga/payment/reservation references |
| supplier_offerings | Fixture offering catalog and durable Inventory product onboarding reference |
| supplier_reorder_policies | Versioned reorder thresholds and simulated lead time, one policy per product |
| supplier_purchase_orders | Purchase-order header, lifecycle status, and policy/inventory snapshot |
| supplier_purchase_order_lines | Durable supplier SKU, product, quantity, and unit-cost snapshot |
| supplier_purchase_order_transitions | Append-only PO lifecycle history with correlation and transition metadata |
| supplier_simulation_clock | Persisted deterministic supplier clock |

`inventory_balances` is an operational projection. For each product,
`on_hand` must equal the sum of `quantity_delta_on_hand` and `reserved` must
equal the sum of `quantity_delta_reserved` in the movement journal. The
Inventory adapter exposes reconciliation and does not silently repair drift.

## Connection lifecycle and health

`DatabaseSettings.from_env()` selects the backend and validates the PostgreSQL
URL, connect timeout, and pool bounds. The defaults are intentionally different
by environment:

- Development/test defaults to the in-memory adapter.
- Production defaults to PostgreSQL and fails fast unless
  `DATABASE_URL` is configured. An explicit in-memory production backend is
  rejected; there is no silent fallback.

The Order, Inventory, and Supplier FastAPI lifespans each open one
`psycopg_pool.AsyncConnectionPool` per worker process and close it on shutdown.
Connections use dict rows. Their `/healthz` endpoints perform a lightweight
`SELECT 1` when PostgreSQL is selected and return HTTP 503 if the database is
unavailable. Pools are created after each worker starts, not shared across
forked processes. The Supplier Simulator uses the same database settings and
shared PostgreSQL database, but owns only supplier offerings, onboarding
references, reorder policies, purchase orders, transition history, and its
simulation clock. It never writes Inventory tables.

## Migration and local development

The repository uses Psycopg 3 with the binary distribution and its separate
async pool extra. Regenerate the lock after dependency changes:

```bash
uv lock --python 3.12
uv sync --locked --all-groups
```

For local Compose development, start PostgreSQL, apply migrations, then start
the application services:

```bash
docker compose up --detach --wait postgres
bash scripts/migrate.sh
docker compose up --build --detach
```

The development defaults expose PostgreSQL on `127.0.0.1:15432` with database,
user, and password `app`. Set `POSTGRES_HOST_PORT` to use another host port when
15432 is already occupied; Compose, the migration script, and the reproduction
script honor the override. For example:

```bash
POSTGRES_HOST_PORT=25432 docker compose up --detach --wait postgres
POSTGRES_HOST_PORT=25432 \
  POSTGRES_TEST_URL=postgresql://app:app@127.0.0.1:25432/app \
  uv run --python 3.12 pytest
POSTGRES_HOST_PORT=25432 bash scripts/migrate.sh
```

Override database credentials for non-disposable environments. Do not use the
defaults outside local development.

The migration runner can also be invoked directly:

```bash
DATABASE_URL=postgresql://app:app@127.0.0.1:15432/app \
  uv run --locked python -m app.persistence.migrations
```

Re-running an unchanged migration is a no-op. Modifying an applied migration
fails with `MigrationChecksumMismatch`; create a new numbered migration
instead. API startup does not run migrations.

For PostgreSQL integration tests, set `POSTGRES_TEST_URL` to a disposable
database URL. Each test creates a unique temporary schema, applies migrations
to that empty schema, and drops it afterward. The test database user therefore
needs permission to create and drop schemas. The tests cover first application,
repeat execution, checksum drift, Inventory persistence, Order outcome
create/update/read, catalog-failure capture, idempotency, append-only movement
behavior, reconciliation, API contract preservation, and new pools after
simulated process restart. CI provisions PostgreSQL and sets this variable. The end-to-end reproduction
script accepts `POSTGRES_HOST_PORT=25432` when run against a non-default port.

## Order outcome persistence

The Order API writes a `pending` row after catalog preflight and before Saga
execution. The terminal Saga action updates that row to `processed`. Catalog
lookup failures, inactive products, inventory rejections, payment declines,
timeouts, service errors, and incomplete compensations are also persisted as
terminal business outcomes before the existing HTTP response is returned.
The compensation listener retains the context presented to compensation
callbacks so a failed order can keep its reservation/payment references for
later investigation. The existing POST response body and status codes are
unchanged.

A missing product is represented with a nullable catalog foreign key and a
separate non-null `requested_product_id`, preserving the attempted identifier
without fabricating a product row. Repeated processing with the same
`order_id` updates that order's latest outcome, matching the pre-existing
in-memory dictionary behavior. The `GET /orders/{order_id}` route remains the
existing inventory lookup; this issue does not change its HTTP contract.

The order stores the payment reference returned by the demo gateway, but the
gateway's charged/refunded state remains process-local. This repository does
not claim durable external payment processing or a transactional payment
ledger.

## Inventory transaction boundaries

- Product creation inserts the product and its zero balance in one transaction.
- Receipts and adjustments lock the product balance, verify the idempotency
  record, update the projection, append a movement, and store the replay result
  in one transaction.
- Reservation creation, release, and commit update the reservation, projection,
  and movement journal in one transaction.
- The movement journal has database triggers that reject UPDATE, DELETE, and TRUNCATE.
- Product, inventory, reservation, and movement response shapes remain
  compatible with the existing Inventory HTTP API.

The PostgreSQL Inventory adapter uses row-level locks and database
constraints to protect balance, reservation, journal, and idempotency writes.
The multi-process contention, retry, and rollback acceptance suite in #22
covers independent API processes and database failure injection.

## Supplier lifecycle and receipt recovery

The Supplier Simulator stores its catalog offerings and onboarding references,
reorder policies, PO headers and lines, transition history, and deterministic
clock in the same PostgreSQL database. A partial unique index allows at most one
active PO per product while its status is `created`, `ordered`, or
`receiving`. Evaluations lock the product's reorder-policy row before looking
for an active PO; the database index is the final duplicate-creation guard.

Receiving is a recoverable cross-service operation, not a distributed
transaction:

1. Commit the PO transition from `ordered` to `receiving`.
2. Call the Inventory API outside the database transaction with the stable
   `supplier-po:<purchase_order_id>` idempotency key.
3. After Inventory acknowledges, commit the receipt ID, the `received`
   transition, and an increment of the policy's inventory revision.

If the Supplier Simulator stops before Inventory commits, or after Inventory
commits but before Supplier persists `received`, the PO remains in
`receiving`. A retry uses the same Inventory idempotency key. The first case
applies stock once; the second replays the original receipt without a second
stock credit. The simulated clock is initialized once and advances atomically
in PostgreSQL, so a restart does not reset due times.

The evaluation records the policy inventory revision before reading Inventory.
If a receipt finalized during that read, a changed revision forces a fresh
Inventory read while the policy row is locked. Receipt finalization also locks
policy then PO, matching the evaluation lock order. This prevents a stale
pre-receipt balance from creating a second PO immediately after the previous PO
leaves the active state.

The transition and PO-line tables reject UPDATE, DELETE, and TRUNCATE. Lifecycle
history records each valid transition once, including its correlation ID and
metadata. The Supplier API exposes that history at
`GET /purchase-orders/{purchase_order_id}/transitions`.

Transactional outbox delivery remains separately tracked in issue #25.
