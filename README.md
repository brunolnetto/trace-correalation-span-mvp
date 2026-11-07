# Trace, Correlation ID, and Span MVP

A small, runnable FastAPI application demonstrating correlation IDs,
distributed tracing, Saga-based order processing, and an explicit product
catalog and inventory lifecycle.

The project uses [Sagaz](https://github.com/brunolnetto/sagaz) to orchestrate
order processing and compensation. The Inventory API owns the product catalog,
stock balances, reservations, and movement ledger. A Supplier Simulation API
models onboarding and replenishment; a versioned Event Collector persists
structured service events for dbt analytics.

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

Supplier Simulation API
  |-- Supplier Fixture Catalog
  |-- Reorder Policies and Purchase Orders
  |-- Simulated Lead-Time Clock
  +-- Idempotent receipts through Inventory API

Order, Inventory, and Supplier JSON logs
  |
  | bounded asynchronous publisher
  v
Event Collector
  |-- Versioned Event Envelope
  |-- Append-only JSONL + Quarantine
  +-- Persistent Docker Volume
         |
         v
      dbt + DuckDB Analytics

Services export OTLP traces to Jaeger. Order, Inventory, and Supplier logs
are forwarded asynchronously to the raw event collector.
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
physical stock below the already reserved quantity. Inactive products cannot
be reserved, although their existing stock remains visible.

The movement endpoint exposes the in-memory ledger for receipts, adjustments,
reservations, releases, commits, and the demo opening balance.

### Reservation lifecycle and order processing

The Order API reads the catalog price before starting the Saga and calculates
the order total from the unit price and quantity. The order Saga then reserves
inventory before charging payment. A successful order
currently leaves its reservation in `reserved` state, representing allocated
stock awaiting a later fulfillment step. The commit endpoint is
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
    "quantity": 2
  }'
```

The declarative Sagaz workflow executes:

1. `reserve_inventory`: reserves stock through the Inventory API.
2. `charge_payment`: charges through a deterministic, process-local payment
   simulator.
3. `confirm_order`: records the completed order in a process-local store.

The Order API looks up the product's current catalog price and computes
`unit_price * quantity`; clients cannot override the charged amount. A
catalog-derived total greater than `PAYMENT_DECLINE_ABOVE` (default: `1000`)
causes a deterministic payment decline. Product 7 costs `99.90`, so a request
for 11 units exercises this path. Sagaz then invokes `release_inventory`
to compensate the successful reservation.

If order confirmation fails after payment, Sagaz invokes
`refund_payment` and then `release_inventory`. If a compensation starts but
does not complete, the API returns HTTP 500 and identifies the incomplete
compensation steps in the response. That condition requires reconciliation;
it must not be interpreted as a successful rollback.

Inventory reservations are idempotent for a given `saga_id` and product.
The inventory store protects concurrent reservations with an async lock and
rejects overselling within one process.

## Deterministic order generator

A one-shot generator exercises the public Order API; it never changes inventory
state directly. Its seed determines quantities and order IDs, so a
`plan_id` identifies the same workload definition. Each invocation gets a
unique `run_id` and unique per-attempt `correlation_id` values, preventing
separate executions from being conflated in analytics. Latency is measured at
runtime and is not expected to be identical.

Start the normal Compose services, then run a seeded random batch:

```bash
docker compose up --build --detach
docker compose --profile simulation run --build --rm order-generator
```

The generator writes one JSON event per attempted order and a final summary to
stdout. The summary includes processed, payment-declined, inventory-rejected,
unexpected-failure counts, and mean/p50/p95/max request latency. HTTP 402 and
409 are domain outcomes, not transport failures. Network errors, unexpected
HTTP statuses, and 5xx responses produce a non-zero exit code.

For a deterministic three-scenario check against the Compose demo product
(product 7, seeded with 42 units and priced at 99.90), run:

```bash
docker compose --profile simulation run --build --rm order-generator python -m app.order_generator --mode coverage
```

The coverage mode expects one processed order (quantity 2), one payment
decline (quantity 11, above the default 1000 payment threshold), and one
insufficient-stock rejection. It reports a scenario mismatch and exits
non-zero if the configured API/product state does not produce those outcomes.
Use a fresh Compose stack when repeating this check because successful orders
keep their stock reservations in the current in-memory order lifecycle.

The defaults can be overridden through CLI flags or environment variables:
`ORDER_API_URL`, `ORDER_GENERATOR_MODE`, `ORDER_GENERATOR_COUNT`,
`ORDER_GENERATOR_SEED`, `ORDER_GENERATOR_PRODUCT_ID`,
`ORDER_GENERATOR_FIRST_ORDER_ID`, `ORDER_GENERATOR_MIN_QUANTITY`, and
`ORDER_GENERATOR_MAX_QUANTITY`. Coverage quantities have corresponding
`ORDER_GENERATOR_SUCCESS_QUANTITY`, `ORDER_GENERATOR_DECLINE_QUANTITY`, and
`ORDER_GENERATOR_INSUFFICIENT_QUANTITY` overrides.

## Supplier onboarding and replenishment simulation

The Supplier Simulation API is exposed on port 18002. Its fixture catalog
contains deterministic supplier offers. Onboarding creates a product through
the Inventory API and installs a default reorder policy:

```bash
curl http://localhost:18002/supplier-catalog
curl -X POST http://localhost:18002/supplier-catalog/BEANS-250/onboard
```

A policy defines `reorder_point`, `target_stock`, and
`lead_time_seconds`. Replenishment evaluates inventory position as:

```text
inventory_position = on_hand - reserved + on_order
```

When inventory position is at or below the reorder point, the simulator creates
one purchase order for `target_stock - inventory_position` units. An existing
created or ordered PO suppresses duplicate reorder decisions. This accounts
for already-committed reservations without treating them as available units.

Evaluate all configured policies or just one product:

```bash
curl -X POST http://localhost:18002/replenishments/evaluate
curl -X POST 'http://localhost:18002/replenishments/evaluate?product_id=8'
```

Purchase orders follow `created -> ordered -> received` or
`created -> cancelled`. Ordering records a due time based on the policy's
lead time. The simulation clock can be advanced explicitly, so tests do not
sleep while waiting for supplier deliveries:

```bash
curl -X POST http://localhost:18002/purchase-orders/<po-id>/order
curl -X POST http://localhost:18002/clock/advance \
  -H 'Content-Type: application/json' -d '{"seconds":3600}'
curl -X POST http://localhost:18002/purchase-orders/<po-id>/receive
```

Receiving sends an idempotent receipt to Inventory API using
`supplier-po:<purchase_order_id>`. Replaying a receive after a supplier
simulator restart can therefore safely replay the same inventory receipt,
provided the purchase order record is restored; supplier policies and PO
records themselves remain process-local in this MVP.

Run the full supplier stack with:

```bash
docker compose up --build --detach
```

The supplier service uses the Inventory API as the sole stock system of record.
It never changes balances directly and does not use adjustments to simulate
supplier deliveries.

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

## Versioned raw event collection

The Event Collector API is exposed on port 18003. Order and Inventory services
forward their structured logs through a bounded background queue; request
handlers only enqueue events and never wait on collector HTTP I/O. Supplier
Simulation uses the same publisher when that service is included in the
Compose stack.

The collector accepts batches at `POST /v1/events` using schema version
`1.0`. Each envelope preserves the original structured log in `source` and
promotes its main fields:

- `event_id`: immutable event identity used for replay deduplication.
- `occurred_at` and collector-assigned `ingested_at`: event and ingestion time.
- `service`, `logger`, `level`, and `event_name`.
- `correlation_id`, `trace_id`, and `span_id` as separate identifiers.
- `attributes`: original structured log attributes.

Accepted records are appended to `/data/events.jsonl` and fsynced before the
collector acknowledges the batch. The `event_id` index is rebuilt from that
file on startup. Replaying the same event is a duplicate; reusing an event ID
with a different payload is quarantined. Invalid or unsupported schema records
are also written to `/data/quarantine.jsonl`; the original payload is
preserved for inspection.

Inspect collected data:

```bash
curl 'http://localhost:18003/v1/events?limit=100'
curl 'http://localhost:18003/v1/quarantine?limit=100'
curl http://localhost:18003/v1/stats
```

The Compose named volume `event-data` persists both JSONL files across
container recreation. To intentionally erase the local event history, remove
the volume as well as the containers:

```bash
docker compose down --volumes
```

Delivery guarantees are deliberately split: once accepted, the collector's
append is durable and replay-safe. Before acknowledgement, service-side
delivery is best-effort: its bounded in-memory queue retries transient HTTP
failures, but queue overflow or abrupt service termination can still lose
pending events. The publisher exposes dropped-event and failed-batch counters
in process memory; a durable producer outbox is a future hardening step.
The collector is a single-writer MVP; do not mount the same JSONL files into
multiple collector replicas.

## dbt analytics

The `analytics/` project uses dbt-duckdb over the raw event contract. It
builds order outcomes, inventory movement facts, supplier purchase-order
facts, generator run metrics, and a correlation-level reconciliation between
generator attempts and the service outcomes observed by the collector.

After producing traffic with the order generator, run:

```bash
docker compose --profile analytics run --build --rm analytics
```

The analytics container mounts the raw event volume read-only and persists its
DuckDB database in a separate `analytics-data` volume. If the collector volume
is empty, dbt uses the included fixture dataset. For model details and local
commands see [analytics/README.md](analytics/README.md).

## Standalone simulation and analytics

Run the deterministic order-to-replenishment scenario followed by the existing
dbt build as one finite Compose workload:

```bash
bash scripts/run-standalone.sh
```

The wrapper starts the Order API, Inventory API, Supplier Simulator, Event
Collector, and Jaeger. It waits for the Simulation Runner to exit and checks
its actual container exit code. Only after successful simulation completion
does it wait for the Analytics Runner and return that job's exit code. It
does not use `--abort-on-container-exit`: that option would stop the long-running
dependencies when the Simulation Runner exits, before analytics can finish.

By default, the wrapper builds the images and removes the containers/network
after both jobs finish. Named volumes are preserved, so reports, raw events,
DuckDB data, and dbt artifacts remain available. Set
`STANDALONE_BUILD=0` to skip rebuilding images, or
`STANDALONE_KEEP_STACK=1` to leave the Compose stack for inspection.

The run writes `simulation-report.json` to the `run-state` volume and
`run-report.json`, the DuckDB database, and dbt artifacts to
`analytics-data`. Inspect the reports with:

```bash
docker compose --profile standalone run --rm --no-deps \
  --entrypoint cat simulation-runner \
  /run-state/simulation-report.json

docker compose --profile standalone run --rm --no-deps \
  --entrypoint cat analytics-runner \
  /analytics-data/run-report.json
```

Exercise the orchestration failure paths with:

```bash
bash scripts/test-standalone-failure-paths.sh
```

The failure-path script verifies that a failed Simulation Runner prevents
Analytics Runner startup and that a failing `dbt` executable produces a
machine-readable failure report and a non-zero overall result. It uses
temporary Compose overrides and preserves named volumes.

To intentionally reset all persisted local data and start from an empty
Collector/analytics history:

```bash
docker compose down -v
```

**Warning:** `down -v` deletes the Collector event history, simulation and
analytics reports, DuckDB database, and all other named-volume data. It is not
part of the normal standalone wrapper. For configuration, scenario invariants,
failure semantics, and report details, see
[the standalone execution guide](docs/standalone-execution.md).

## Reproduce locally (recommended)

Requirements: Docker Engine, Docker Compose v2, `uv`, and `curl`. The script
provisions and uses the project's pinned Python 3.12 interpreter,
independently of the currently activated virtual environment.

Run the end-to-end reproduction from the repository root:

```bash
bash scripts/reproduce-local.sh
```

The script resolves/synchronizes dependencies, runs pytest and Ruff, builds
and starts Jaeger, the Order/Inventory APIs, the Supplier Simulation API, and
the Event Collector. It exercises a successful order, a payment decline with
compensation, an insufficient-stock rejection, supplier onboarding and
replenishment through a simulated lead time, and the generator's three
coverage scenarios. It verifies balances, correlated JSON logs, cross-service
Jaeger traces, and raw event persistence for order, supplier, and generator
events, then runs `dbt build` against the collected raw event volume. It
leaves the Compose stack running for inspection; stop it with
`docker compose down`. Pass `--cleanup` to stop the stack automatically
when the script exits.

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

The APIs are exposed on host ports `18000` (Order), `18001` (Inventory),
`18002` (Supplier Simulation), and `18003` (Event Collector). Jaeger UI is
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

The tests cover correlation ID preservation, W3C trace-context propagation,
concurrent Saga context isolation, product/catalog and inventory invariants,
idempotent receipts and reservations, oversell prevention, order processing
and compensation, deterministic generator plans and publication, supplier
onboarding/replenishment/lead-time behavior, and raw event schema validation,
append durability, replay deduplication, and quarantine. CI also builds the
dbt project against its versioned fixture dataset.

## Scope and limitations

The catalog, inventory balances, reservations, receipts, movement ledger,
supplier policies, and supplier purchase-order state are process-local and are
lost on service restart. The async lock protects one
Inventory API process only; running multiple replicas would create divergent
balances and could oversell. PostgreSQL transactions/row locks and durable
idempotency are the next storage step before horizontal scaling. The current
movement ledger is an in-memory domain record, not a durable event log.

This MVP sends traces directly to Jaeger and writes logs to container stdout;
selected structured logs are also stored in append-only raw JSONL files by the
Event Collector. It does not include an OpenTelemetry Collector, a full log
search backend, durable business storage, a production payment provider, a
transactional producer outbox, authentication, or production deployment
hardening. The event collector is single-writer and its producer queue is
best-effort before acknowledgement. Sagaz is used for workflow orchestration
and compensation; its in-memory configuration here is not a substitute for
durable state and reconciliation infrastructure.
