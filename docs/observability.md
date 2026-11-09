# Diagnostic observability

The local stack keeps three concerns separate:

- **Traces:** application SDKs export OTLP/HTTP to the OpenTelemetry Collector; its trace pipeline batches and forwards OTLP/gRPC to Jaeger.
- **Diagnostic logs:** Python logging continues to emit JSON to stdout and additionally exports OTLP logs to the Collector. The Collector forwards OTLP/HTTP to Loki. The OTel log resource carries `service.name`; each record carries severity, trace/span context when a span is active, and `correlation_id` when a request context exists.
- **Business events:** domain/outbox events continue to use the Event Collector and its append-only JSONL persistence. Loki is not an event store and is not used by dbt.

## Local endpoints

| Service | URL |
| --- | --- |
| Jaeger traces | http://localhost:16686 |
| Grafana (Loki + Jaeger data sources) | http://localhost:13000 |
| Loki API | http://localhost:13100 |
| Collector OTLP/gRPC | localhost:4317 |
| Collector OTLP/HTTP | http://localhost:4318 |
| Collector health | http://localhost:13133/ |

Host ports can be overridden with `JAEGER_UI_HOST_PORT`, `GRAFANA_HOST_PORT`, `LOKI_HOST_PORT`, `OTLP_GRPC_HOST_PORT`, and `OTLP_HTTP_HOST_PORT`.

## Useful Loki queries

In Grafana Explore, select the Loki data source:

```logql
{service_name="order-api"}
```

Filter a request by severity:

```logql
{service_name="order-api"} | severity_text="ERROR"
```

Find order failures by correlation ID (replace the UUID):

```logql
{service_name="order-api"} | correlation_id="550e8400-e29b-41d4-a716-446655440000"
```

Find inventory errors:

```logql
{service_name="inventory-api"} | severity_text="ERROR"
```

Find supplier simulator failures:

```logql
{service_name="supplier-simulator"} | severity_text="ERROR"
```

Inspect outbox delivery failures and retries:

```logql
{service_name="outbox-dispatcher"} | severity_text=~"WARN|ERROR"
```

Filter by a trace ID copied from a log record:

```logql
{service_name=~".+"} | trace_id="0123456789abcdef0123456789abcdef"
```

Loki's OTLP ingestion maps the service name to the `service_name` index label. Severity, trace/span IDs, and correlation ID remain structured metadata; they are not promoted to index labels. In particular, **never make correlation IDs metric labels**: their unbounded cardinality is appropriate for log filtering, not metric dimensions.

## Retention, queues, and loss behavior

- Loki retention is configured to seven days on the local filesystem volume. This is a disposable development default, not a production durability policy.
- The Collector applies a 128 MiB memory limiter and batches up to 1,024 records (512 per normal batch).
- Each OTLP exporter has an in-memory sending queue of 500 requests and retries with bounded backoff for at most 60 seconds. A Collector restart loses queued, not-yet-exported telemetry. If a downstream backend remains unavailable beyond the retry window or a queue fills, telemetry can be dropped; application requests do not synchronously wait for Loki or Jaeger.
- The Python log batch processor has a bounded queue of 1,024 records and a 256-record export batch. Its shutdown flush is best-effort. A process crash or prolonged Collector outage can lose buffered diagnostic logs.
- These pipelines are observability paths, not transactional delivery. Do not put business events, audit records, or workflow state exclusively in OTLP/Loki. Those remain on the Event Collector/outbox path.

## Startup and cleanup

Start the stack with `docker compose up --build -d`; wait for Loki readiness at `/ready`, Grafana at `/api/health`, and application `/healthz` endpoints. Jaeger remains independently available for traces. Stop containers with `docker compose down`; use `docker compose down -v` only when intentionally deleting local Postgres, Event Collector, Loki, Grafana, analytics, and simulation data volumes.

Compose sets memory limits of 512 MiB for Jaeger and Loki, 256 MiB for the Collector and Grafana. Adjust these for larger workloads. The Collector configuration and Loki retention are intentionally small local defaults.
