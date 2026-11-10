# Diagnostic observability

The local stack keeps three concerns separate:

- **Traces:** application SDKs export OTLP/HTTP to the OpenTelemetry Collector; its trace pipeline batches and forwards OTLP/gRPC to Jaeger.
- **Diagnostic logs:** Python logging continues to emit JSON to stdout and additionally exports OTLP logs to the Collector. The Collector forwards OTLP/HTTP to Loki. The OTel log resource carries `service.name`; each record carries severity, trace/span context when a span is active, and `correlation_id` when a request context exists.
- **Metrics:** application SDKs export OTLP metrics to the Collector. Its Prometheus exporter exposes the collected metrics to the Prometheus scraper; Grafana uses Prometheus as a queryable metrics data source. The Collector's debug exporter remains enabled for local diagnostics.
- **Business events:** domain/outbox events continue to use the Event Collector and its append-only JSONL persistence. Loki is not an event store and is not used by dbt.

## Local endpoints

| Service | URL |
| --- | --- |
| Jaeger traces | http://localhost:16686 |
| Grafana (Loki + Prometheus + Jaeger data sources) | http://localhost:13000 |
| Prometheus API/UI | http://localhost:19090 |
| Loki API | http://localhost:13100 |
| Collector OTLP/gRPC | localhost:4317 |
| Collector OTLP/HTTP | http://localhost:4318 |
| Collector health | http://localhost:13133/ |
| Collector Prometheus scrape endpoint (internal Compose network) | http://otel-collector:9464/metrics |

Host ports can be overridden with `JAEGER_UI_HOST_PORT`, `GRAFANA_HOST_PORT`, `LOKI_HOST_PORT`, `OTLP_GRPC_HOST_PORT`, `OTLP_HTTP_HOST_PORT`, `OTEL_HEALTH_HOST_PORT`, and `PROMETHEUS_HOST_PORT`.

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
{service_name="order-api"} | correlation_id="550e8400-e29b-41d4-a716-446655440000" | trace_id!=""
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

## Useful Prometheus queries

In Grafana Explore, select the Prometheus data source. Gateway request counts
are exported with bounded dimensions only (HTTP method, response status, and
simulation outcome); request IDs, correlation IDs, trace IDs, span IDs, and
raw paths are deliberately excluded.

Request rate by gateway outcome:

```promql
sum by (gateway_simulation_outcome) (
  rate(gateway_requests_total{service_name="gateway-simulator"}[5m])
)
```

95th-percentile gateway request duration:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(gateway_request_duration_seconds_bucket{service_name="gateway-simulator"}[5m])
  )
)
```

Prometheus scrapes the Collector every 15 seconds. The Collector's Prometheus
exporter converts the bounded OTel resource attributes into scrape labels,
including `service_name`. Prometheus retains local metrics for seven days in
the named `prometheus-data` volume.

## Verify gateway log searchability

With the regular Compose stack running, execute:

```bash
uv run python scripts/verify-gateway-loki.py
```

The verifier sends a read-only `GET /inventory/7` through the gateway with
fresh `X-Correlation-ID` and `X-Simulation-Request-ID` values. It then polls
Loki's HTTP API for the gateway's forwarded-request log, filtering by
`service_name` and the structured metadata fields `correlation_id`,
`trace_id`, `span_id`, `route_id`, and `decision`. This demonstrates that
the identifiers are searchable without promoting high-cardinality values to
Loki index labels or metric dimensions. It does not change gateway policy or
business state.

Override endpoints or ingestion wait when needed:

```bash
uv run python scripts/verify-gateway-loki.py \
  --gateway-url http://127.0.0.1:18004 \
  --loki-url http://127.0.0.1:13100 \
  --timeout-seconds 60
```

The equivalent LogQL query, with the generated UUID substituted, is:

```logql
{service_name="gateway-simulator"}
  | correlation_id="550e8400-e29b-41d4-a716-446655440000"
  | trace_id!=""
  | span_id!=""
  | route_id="inventory"
  | decision="forwarded"
```

## Verify gateway metric searchability

With the regular Compose stack running, execute:

```bash
uv run python scripts/verify-gateway-metrics.py
```

The verifier sends a read-only warm-up `GET /inventory/7`, waits for a
Prometheus sample timestamped after that request completes, and records the
counter baseline. It then sends a second measured GET and requires a later
scrape to show the counter above that baseline. This avoids mistaking the
first scrape's accumulated requests for the measured request's increment.
It checks the `gateway_requests_total` series for the `gateway-simulator`
service and the bounded `forwarded` outcome. It does not change gateway
policy or business state. Request correlation and simulation IDs are
intentionally absent from metric labels.

Override endpoints or the scrape wait when needed:

```bash
uv run python scripts/verify-gateway-metrics.py \
  --gateway-url http://127.0.0.1:18004 \
  --prometheus-url http://127.0.0.1:19090 \
  --timeout-seconds 90
```

Prometheus target health is available at
[http://localhost:19090/targets](http://localhost:19090/targets).

## Retention, queues, and loss behavior

- Loki retention is configured to seven days on the local filesystem volume. This is a disposable development default, not a production durability policy.
- The Collector applies a 128 MiB memory limiter and batches up to 1,024 records (512 per normal batch).
- Each OTLP exporter has an in-memory sending queue of 500 requests and retries with bounded backoff for at most 60 seconds. A Collector restart loses queued, not-yet-exported telemetry. If a downstream backend remains unavailable beyond the retry window or a queue fills, telemetry can be dropped; application requests do not synchronously wait for Loki or Jaeger.
- The Python log batch processor has a bounded queue of 1,024 records and a 256-record export batch. Its shutdown flush is best-effort. A process crash or prolonged Collector outage can lose buffered diagnostic logs.
- These pipelines are observability paths, not transactional delivery. Do not put business events, audit records, or workflow state exclusively in OTLP/Loki. Those remain on the Event Collector/outbox path.

## Startup and cleanup

Start the stack with `docker compose up --build -d`; wait for Loki readiness at `/ready`, Prometheus at `/-/ready`, Grafana at `/api/health`, and application `/healthz` endpoints. Jaeger remains independently available for traces. Stop containers with `docker compose down`; use `docker compose down -v` only when intentionally deleting local Postgres, Event Collector, Loki, Prometheus, Grafana, analytics, and simulation data volumes.

Compose sets memory limits of 512 MiB for Jaeger and Loki, 256 MiB for the Collector, Prometheus, and Grafana. Adjust these for larger workloads. The Collector configuration and Loki retention are intentionally small local defaults.
