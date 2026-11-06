# Trace, Correlation ID, and Span MVP

A small, runnable two-service FastAPI application demonstrating the difference
between a business `correlation_id` and an OpenTelemetry distributed trace.

The project follows the `uv` project layout and dependency workflow used by
[Astral's uv-fastapi-example](https://github.com/astral-sh/uv-fastapi-example),
while keeping the application focused on observability.

## Architecture

```text
Client
  |
  | X-Correlation-ID (business-operation identifier)
  | traceparent (W3C Trace Context, when supplied)
  v
Order API :8000
  |
  | HTTPX instrumentation injects traceparent
  | X-Correlation-ID is propagated explicitly
  v
Inventory API :8001

Both services export OTLP traces to Jaeger.
Both services emit structured JSON logs to stdout.
```

- **correlation_id** identifies the business operation across services.
- **trace_id** identifies the distributed trace.
- **span_id** identifies one operation inside that trace.

The IDs are deliberately not interchangeable. OpenTelemetry owns trace and
span creation and W3C context propagation; the application owns the
correlation ID.

## Reproduce locally (recommended)

Requirements: Docker Engine, Docker Compose v2, `uv`, and `curl`. The script provisions and uses the project's pinned Python 3.12 interpreter, independently of the currently activated virtual environment.

Run the end-to-end reproduction from the repository root:

```bash
bash scripts/reproduce-local.sh
```

The script resolves/synchronizes dependencies, generates `uv.lock` on the first run if it is absent, runs pytest and Ruff, builds and starts Jaeger plus both APIs, waits for readiness, sends a request with a known correlation ID, and verifies the response, correlated JSON logs, and a cross-service trace in Jaeger. It leaves the Compose stack running for inspection; stop it with `docker compose down`. Pass `--cleanup` to stop the stack automatically when the script exits.

If `uv.lock` is generated on the first run, review and commit it so subsequent runs use `uv sync --locked` and reproduce the same dependency resolution.

## Run locally with Docker Compose

Requirements: Docker Engine and Docker Compose v2.

```bash
docker compose up --build
```

Send a request with a caller-provided correlation ID:

```bash
curl -i http://localhost:8000/orders/123 \
  -H 'X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440000'
```

The API also generates a UUID when the header is absent or invalid. The
response always includes `X-Correlation-ID`.

Open [Jaeger](http://localhost:16686), select `order-api`, and find
`GET /orders/{order_id}`. The trace includes the order server span, the
business span `order.load_inventory`, the HTTP client span, the inventory
server span, and `inventory.lookup`.

## JSON logs

Each service writes one JSON object per log event to stdout. Example fields:

```json
{
  "timestamp": "2026-11-06T18:30:00+00:00",
  "level": "INFO",
  "service": "order-api",
  "logger": "app.order.main",
  "message": "order.lookup.started",
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
  "trace_id": "32-hex-character-trace-id",
  "span_id": "16-hex-character-span-id",
  "attributes": {
    "order_id": 123
  }
}
```

Trace and span IDs are read from the active OpenTelemetry context when the
log record is formatted. They are null when a log event is outside a span.

## Run without Docker

Install dependencies and create/update the lockfile:

```bash
uv sync --all-groups
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
| `ENVIRONMENT` | `development` | Deployment environment |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `INVENTORY_API_URL` | `http://localhost:8001` | Order API downstream URL |

The sampler is parent-based: a service honors the sampling decision of an
incoming trace and applies the configured ratio only to new root traces.

## Tests and lint

```bash
uv run pytest
uv run ruff check .
```

The tests cover correlation ID preservation, generation, invalid/oversized
input replacement, and JSON log fields. The Docker Compose flow demonstrates
the full HTTPX-to-FastAPI W3C trace propagation path.

## Scope

This MVP sends traces directly to Jaeger and writes logs to container stdout.
It does not include an OpenTelemetry Collector, a log aggregation backend,
metrics, authentication, or production deployment hardening. Those can be
added as separate iterations.
