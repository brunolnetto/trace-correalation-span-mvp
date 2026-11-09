# PostgreSQL persistence foundation

This document tracks issue #21. PostgreSQL is the durable source of truth for
the core Order and Inventory business state. The Event Collector and dbt remain observability and
analytics products, never write authorities.

## Decisions

- Use one PostgreSQL database for the Order and Inventory domains. Do not
  introduce a generic multi-database abstraction.
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
- Business event outbox rows are inserted by PostgreSQL triggers in the same
  transaction as product/order changes, Inventory movements, supplier policy/PO
  transitions, and replenishment decisions. A separate dispatcher owns leases,
  retries, and Collector acknowledgement; stdout diagnostic logging remains
  independent of this analytical event path.

## Persistence model

| Table | Responsibility |
| --- | --- |
| products | SKU, descriptive/catalog fields, active status, timestamps |
| inventory_balances | Current on_hand and reserved projection, one row per product |
| reservations | Reservation identity, order/Saga association, quantity, lifecycle status |
| inventory_movements | Append-only deltas for physical and reserved quantities |
| idempotency_records | Scoped key, canonical request hash, and replayable response |
| orders | Durable order input, catalog price snapshot, outcome, Saga/payment/reservation references |
| outbox_events | Immutable business event envelope, stable event ID, lease/retry state, Collector acknowledgement |

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

The Order and Inventory FastAPI lifespans each open one
`psycopg_pool.AsyncConnectionPool` per worker process and close it on shutdown.
Connections use dict rows. Both `/healthz` endpoints perform a lightweight
`SELECT 1` when PostgreSQL is selected and return HTTP 503 if the database is
unavailable. Pools are created after each worker starts, not shared across
forked processes.

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

The PostgreSQL adapter uses row-level locks to prevent a projection update from
being lost while the associated movement/idempotency rows are written. The
explicit multi-process contention and no-overselling acceptance suite remains
in issue #22; this issue does not claim that broader concurrency milestone is
complete.

## Remaining in #21

- Regenerate `uv.lock` with the new Psycopg runtime dependency and verify
  `uv sync --locked --all-groups`.
- Run the complete pytest, Ruff, PostgreSQL integration, and end-to-end
  reproduction suites against the resolved dependency lock.
- Review the final CI result and the PR diff.

Supplier policies and purchase-order lifecycle remain in issue #23. Transactional
outbox delivery remains separately tracked.
