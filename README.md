# Trace, Correlation ID, and Span MVP

A small, runnable two-service FastAPI application demonstrating the difference
between a business `correlation_id` and an OpenTelemetry distributed trace.
It also demonstrates a multi-step order workflow with the Saga pattern using
[Sagaz](https://github.com/brunolnetto/sagaz).

The project follows the `uv` project layout and dependency workflow used by
[Astral's uv-fastapi-example](https://github.com/astral-sh/uv-fastapi-example),
while keeping the application focused on distributed observability.

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

## Read-only tracing example

The original read-only endpoint remains available:

```bash
curl -i http://localhost:8000/orders/123 \
  -H 'X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440000'
```

Open [Jaeger](http://localhost:16686), select `order-api`, and find
`GET /orders/{order_id}`. The trace includes the order server span, the
business span `order.load_inventory`, the HTTP client span, the inventory
server span, and `inventory.lookup`.

## Saga order-processing example

Submit a request to run the order workflow:

```bash
curl -i -X POST http://localhost:8000/orders/123/process \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440000' \
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
rejects overselling. Release is idempotent.

**This is an orchestration demonstration, not a durable production workflow.**
Inventory, payment and order state are process-local. Sagaz uses its default
in-memory storage in this example. Restarting a container loses this state,
and this MVP does not claim crash recovery, durable idempotency across restarts,
or exactly-once external side effects.

## JSON logs

Each service writes one JSON object per log event to stdout. Example fields:

```json
{
  "timestamp": "2026-11-06T18:30:00+00:00",
  "level": "INFO",
  "service": "order-api",
  "logger": "app.order.main",
  "message": "order.process.completed",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
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
and starts Jaeger plus both APIs, waits for readiness, sends a request with a
known correlation ID, and verifies the response, correlated JSON logs, and a
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
available on port `16686`.

## Run without Docker

Install dependencies and update the lockfile:

```bash
uv lock --python 3.12
uv sync --locked --all-groups
```

Start Jaeger separately with OTLP enabled, then run each API in a separate
terminal:

```bash
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
saga context isolation, idempotent inventory reservations, oversell prevention,
successful order processing, and compensation behavior.

## Scope and limitations

This MVP sends traces directly to Jaeger and writes logs to container stdout.
It does not include an OpenTelemetry Collector, a log aggregation backend,
durable business storage, a production payment provider, a transactional
outbox, authentication, or production deployment hardening. Sagaz is used for
workflow orchestration and compensation; its in-memory configuration here is
not a substitute for durable state or reconciliation infrastructure.
