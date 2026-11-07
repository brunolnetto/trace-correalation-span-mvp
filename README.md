# Trace, Correlation ID, and Span MVP

A small, runnable FastAPI application demonstrating correlation IDs,
distributed tracing, Saga-based order processing, and an explicit product
catalog and inventory lifecycle.

The project uses [Sagaz](https://github.com/brunolnetto/sagaz) to orchestrate
order processing and compensation. The Inventory API owns a product catalog,
stock balances, reservations, and an in-memory movement ledger.

## Architecture

```text
Client
  |
  | X-Correlation-ID (business-operation identifier)
  | traceparent (W3C Trace Context, when supplied)
  v
Order API
  |
  | Sagaz: OrderProcessingSaga
  |   1. reserve_inventory
  |   2. charge_payment (local payment simulator)
  |   3. confirm_order
  |
  | on failure:
  |   refund_payment
  |   release_inventory
  |
  | HTTPX instrumentation injects traceparent
  | X-Correlation-ID is propagated explicitly
  v
Inventory API
  |-- Product Catalog
  |-- Stock Receipts and Adjustments
  |-- Reservations, Release, and Commit
  |-- Inventory Movement Ledger

Both services export OTLP traces to Jaeger.
Both services emit structured JSON logs to stdout.
```

- **correlation_id** identifies the business operation across services.
- **trace_id** identifies the distributed trace.
- **span_id** identifies one operation inside that trace.
- **saga_id** identifies one execution of the order workflow.

The IDs are deliberately not interchangeable. OpenTelemetry owns trace and
span creation and W3C context propagation; the application owns the
correlation ID; Sagaz owns the orchestration instance identifier.

## Product catalog and inventory lifecycle

A product can exist without any stock. Creating a product does not create
inventory:

```bash
curl -i -X POST http://localhost:18001/products \
  -H 'Content-Type: application/json' \
  -d '{
    "sku": "COFFEE-250G",
    "name": "Ground coffee 250g",
    "category": "grocery",
    "unit_price": "19.90"
  }'
```

The returned `product_id` is used for inventory operations. Stock enters the
system through an explicit, idempotent receipt:

```bash
curl -i -X POST http://localhost:18001/inventory/8/receipts \
  -H 'Content-Type: application/json' \
  -d '{
    "quantity": 50,
    "idempotency_key": "supplier-delivery-2026-001",
    "reference": "PO-2026-001"
  }'
```

Inventory distinguishes physical stock from committed reservations:

- `on_hand`: physical units in the inventory.
- `reserved`: units allocated to orders but not yet physically issued.
- `available_quantity`: `on_hand - reserved`, the amount that can be reserved.
  The legacy `available` field remains a boolean indicating whether this
  quantity is greater than zero.

Reserving stock increases `reserved`, not decreases `on_hand`. Releasing a
reservation decreases `reserved`. Committing a reservation decreases both
`on_hand` and `reserved` and represents physical fulfillment/issue.

```text
POST   /products
GET    /products
GET    /products/{product_id}
PATCH  /products/{product_id}

GET    /inventory/{product_id}
GET    /inventory/{product_id}/movements
POST   /inventory/{product_id}/receipts
POST   /inventory/{product_id}/adjustments
POST   /inventory/{product_id}/reservations
DELETE /inventory/reservations/{reservation_id}
POST   /inventory/reservations/{reservation_id}/commit
```

Receipts and adjustments require an `idempotency_key`; replaying the same
key and payload does not apply the movement twice. Reusing the key with a
different payload is rejected. Adjustments require a reason and cannot reduce
physical stock below the already reserved quantity. Inactive products cannot receive new
reservations, although their existing stock remains visible.

The movement endpoint exposes the in-memory ledger for receipts, adjustments,
reservations, releases, commits, and the demo opening balance.

### Reservation lifecycle and order processing

The order Saga reserves inventory before charging payment. A successful
order currently leaves its reservation in `reserved` state, representing
allocated stock awaiting a later fulfillment step. The commit endpoint is
available for that later step; the current order Saga does not yet model
picking, shipping, or delivery.

If payment is declined, Sagaz compensates by releasing the reservation.
Insufficient stock is rejected before payment is attempted.

## Read-only tracing example

The Compose profile seeds one explicit demo product (ID 7, 42 units). The
original read-only endpoint remains available:

```bash
curl -i http://localhost:18000/orders/7 \
  -H 'X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440000'
```

For this original demonstration route, the path ID is also used as the
inventory product ID. Open [Jaeger](http://localhost:16686), select
`order-api`, and find `GET /orders/{order_id}`. The trace includes the order
server span, the business span `order.load_inventory`, the HTTP client span,
the inventory server span, and `inventory.lookup`.

## Saga order-processing example

Submit a request to run the order workflow:

```bash
curl -i -X POST http://localhost:18000/orders/123/process \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440001' \
  -d '{
    "product_id": 7,
    "quantity": 2,
    "amount": 99.90
  }'
```

The declarative Sagaz workflow executes:

1. `reserve_inventory`: reserves stock through the Inventory API.
2. `charge_payment`: charges through a deterministic, process-local payment
   simulator.
3. `confirm_order`: records the completed order in a process-local store.

A payment amount greater than `PAYMENT_DECLINE_ABOVE` (default: `1000`)
causes a deterministic payment decline. Sagaz then invokes
`release_inventory` to compensate the successful reservation.

If order confirmation fails after payment, Sagaz invokes
`refund_payment` and then `release_inventory`. If a compensation starts but
does not complete, the API returns HTTP 500 and identifies the incomplete
compensation steps in the response. That condition requires reconciliation;
it must not be interpreted as a successful rollback.

Inventory reservations are idempotent for a given `saga_id` and product.
The inventory store protects concurrent reservations with an async lock and
rejects overselling within one process.

## JSON logs

Each service writes one JSON object per log event to stdout. Example fields:

```json
{
  "timestamp": "2026-11-06T18:30:00+00:00",
  "level": "INFO",
  "service": "order-api",
  "logger": "app.order.main",
  "message": "order.process.completed",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440001",
  "trace_id": "32-hex-character-trace-id",
  "span_id": "16-hex-character-span-id",
  "attributes": {
    "order_id": 123,
    "saga_id": "saga-execution-id"
  }
}
```

Trace and span IDs are read from the active OpenTelemetry context when the
log record is formatted. They are null when a log event is outside a span.
The order request span records the `saga.id` attribute; detailed Sagaz action
and compensation spans are a separate observability iteration.

## Reproduce locally (recommended)

Requirements: Docker Engine, Docker Compose v2, `uv`, and `curl`. The script
provisions and uses the project's pinned Python 3.12 interpreter,
independently of the currently activated virtual environment.

Run the end-to-end reproduction from the repository root:

```bash
bash scripts/reproduce-local.sh
```

The script resolves/synchronizes dependencies, runs pytest and Ruff, builds
and starts Jaeger plus both APIs, waits for readiness, creates a low-stock
product and receives its initial stock, exercises a successful order, a
payment decline with compensation, and an insufficient-stock rejection. It
then verifies the inventory balances, correlated JSON logs, and a
cross-service trace in Jaeger. It leaves the Compose stack running for
inspection; stop it with `docker compose down`. Pass `--cleanup` to stop the
stack automatically when the script exits.

After changing dependencies, regenerate and commit the lockfile:

```bash
uv lock --python 3.12
uv sync --locked --all-groups
```

## Run locally with Docker Compose

Requirements: Docker Engine and Docker Compose v2. Ensure the project's
lockfile is current first:

```bash
uv lock --python 3.12
docker compose up --build
```

The APIs are exposed on host ports `18000` (Order) and `18001` (Inventory)
to avoid conflicts with common local development services. Jaeger UI is
available on port `16686`. Compose explicitly enables
`INVENTORY_SEED_DEMO_DATA`, which creates product 7 with 42 units. The
Inventory API does not assume stock for unknown product IDs.

## Run without Docker

Install dependencies and update the lockfile:

```bash
uv lock --python 3.12
uv sync --locked --all-groups
```

Start Jaeger separately with OTLP enabled, then run each API in a separate
terminal. To reproduce the seeded demo product outside Compose, set
`INVENTORY_SEED_DEMO_DATA=true` for the Inventory API:

```bash
INVENTORY_SEED_DEMO_DATA=true \
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces \
SERVICE_NAME=inventory-api \
uv run uvicorn app.inventory.main:app --port 8001
```

```bash
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces \
INVENTORY_API_URL=http://localhost:8001 \
SERVICE_NAME=order-api \
uv run uvicorn app.order.main:app --port 8000
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` | OTLP/HTTP trace endpoint |
| `OTEL_TRACES_SAMPLER_ARG` | `1.0` | Root trace sampling ratio (0.0–1.0) |
| `SERVICE_NAME` | Service-specific | JSON log service and OTel resource name |
| `SERVICE_VERSION` | `0.1.0` | OTel resource version |
| `ENVIRONMENT` | `development` | OTel deployment environment |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `INVENTORY_API_URL` | `http://localhost:8001` | Inventory API base URL |
| `INVENTORY_SEED_DEMO_DATA` | `false` | Seed product 7 with 42 units for local demonstrations |
| `PAYMENT_DECLINE_ABOVE` | `1000` | Demo payment decline threshold |

The sampler is parent-based: a service honors the sampling decision of an
incoming trace and applies the configured ratio only to new root traces.

## Tests and lint

```bash
uv run pytest
uv run ruff check .
```

The tests cover correlation ID preservation, generation, invalid/oversized
input replacement, JSON log fields, W3C trace-context round trips, concurrent
saga context isolation, explicit product creation, duplicate SKU rejection,
idempotent receipts, inventory ledger entries, reservation/release/commit
semantics, adjustment invariants, oversell prevention, order processing, and
compensation behavior.

## Scope and limitations

The catalog, inventory balances, reservations, receipts, and movement ledger
are process-local and are lost on restart. The async lock protects one
Inventory API process only; running multiple replicas would create divergent
balances and could oversell. PostgreSQL transactions/row locks and durable
idempotency are the next storage step before horizontal scaling. The current
movement ledger is an in-memory domain record, not a durable event log.

This MVP sends traces directly to Jaeger and writes logs to container stdout.
It does not include an OpenTelemetry Collector, a log aggregation backend,
durable business storage, a production payment provider, a transactional
outbox, authentication, supplier purchase orders, automated replenishment,
or production deployment hardening. Sagaz is used for workflow orchestration
and compensation; its in-memory configuration here is not a substitute for
durable state and reconciliation infrastructure.
