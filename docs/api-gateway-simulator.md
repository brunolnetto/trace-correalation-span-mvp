# API Gateway behavior simulator

The Gateway Simulator is a local experimental reverse proxy. It adds a controllable
infrastructure boundary in front of the existing Order, Inventory, and Supplier APIs.
It is not a production API-management service and it is not the digital-twin engine.

## Start and inspect

Start the regular Compose stack:

```bash
docker compose up --build --detach
curl http://localhost:18004/_simulator/healthz
curl http://localhost:18004/_simulator/policy
```

Default routes forward these path families without rewriting the path:

| Path prefix | Upstream |
|---|---|
| /orders | Order API |
| /products, /inventory | Inventory API |
| /supplier-catalog, /reorder-policies, /replenishments | Supplier Simulator |
| /purchase-orders, /clock | Supplier Simulator |

The simulator listens on 127.0.0.1:18004 in the local Compose profile.
The control API shares that local-only listener under /_simulator. The
standalone runner and order-generator profiles also use the gateway URL for
their simulated client traffic. Internal service-to-service calls remain
direct, so gateway experiments model the ingress boundary rather than
accidentally routing a service through itself.

## Request identity and reproducibility

Supply an X-Simulation-Request-ID to make a request-level policy decision stable
across replays. When omitted, the gateway uses the incoming X-Correlation-ID;
when that is also absent, it generates a new correlation ID.

The current decision function hashes the scenario seed, immutable policy revision,
route ID, and request key. It derives latency and injected-failure decisions from
that digest. Repeating the same inputs under the same revision produces the same
decision. A policy update increments the revision and therefore creates a new
decision space.

```bash
curl -i http://localhost:18004/inventory/7 \
  -H 'X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440001' \
  -H 'X-Simulation-Request-ID: baseline-inventory-lookup'
```

## Updating a policy

Read the current policy first. Copy its route list, edit the desired route, and
send the complete replacement with the revision you read:

```bash
curl -X PUT http://localhost:18004/_simulator/policy \
  -H 'Content-Type: application/json' \
  -d '{
    "expected_revision": 1,
    "seed": 20261109,
    "routes": [
      {
        "route_id": "order-api",
        "path_prefix": "/orders",
        "upstream_url": "http://order-api:8000",
        "latency_min_ms": 50,
        "latency_max_ms": 250,
        "failure_probability": 0.1,
        "failure_mode": "http",
        "failure_status_code": 503,
        "requests_per_second": 5,
        "rate_limit_burst": 10,
        "max_in_flight": 4,
        "timeout_seconds": 3
      }
    ]
  }'
```

The update is compare-and-swap: a stale expected_revision receives HTTP 409.
The API replaces the complete route set, increments the revision, and records a
bounded process-local audit entry. Each admitted request keeps the immutable
policy snapshot it observed at admission. Policy and audit state reset on restart.

## End-to-end Order Saga ingress experiment

The Order Saga verifier compares the existing business workflow under a normal
gateway ingress policy with a deterministic HTTP 503 injected on the gateway's
`order-api` route:

```bash
uv run python scripts/verify-gateway-order-workflow.py
```

The Compose stack must be running with the PostgreSQL migrations applied, and
the selected product must be active with enough available stock. The default
uses demo product 7 and reserves one unit.

The verifier performs these checks:

1. Install a baseline `order-api` route policy with no injected fault, added
   latency, rate limit, or in-flight limit; submit an order through the gateway.
2. Inspect the Order API recovery view and require the unchanged action sequence
   `reserve_inventory -> charge_payment -> confirm_order` to be completed.
   It checks the demo payment ID against the existing deterministic
   `UUIDv5("payment:<saga_id>")` identity.
3. Replay the same order and require the Saga, payment, and reservation
   identities to remain unchanged, with each forward step still attempted once.
4. Set the `order-api` route's failure probability to 1.0 and submit a different
   order ID. The gateway must return `503 injected_http_failure`; the direct
   Order API recovery lookup must return 404, proving the request never created
   an Order Saga or reached the payment/reservation actions.
5. Restore the original policy seed and complete route set with compare-and-swap,
   then verify that the exact original seed and route configuration is active
   again. The policy revision advances; it is not reset to its old number.

The verifier restores the policy in a `finally` block. If another actor changes
the policy while the verifier is running, it refuses to overwrite that newer
revision; inspect and restore the intended policy manually.

**Scope of this experiment:** the gateway intentionally remains an ingress
boundary. Order-to-Inventory calls remain direct as documented above. The
degraded scenario therefore demonstrates an ingress rejection before Saga
execution, not a simulated failure inside the Inventory dependency or a Saga
compensation path. Those are separate experiments and should not be conflated.

**Persistent side effect:** the baseline creates one real demo order, one
durable demo payment, and one Inventory reservation in the Compose PostgreSQL
database. Replaying it does not create another reservation/payment. The
degraded request creates no Order/Saga/payment/reservation state. The current
Order Saga does not commit stock, so the successful reservation remains
reserved; use a product with sufficient available quantity and account for the
requested quantity (one unit by default) when repeating the verifier. The script does not restart
containers, clear volumes, or mutate the gateway policy permanently.

## Current behavior and limits

- Route selection uses the longest matching path prefix with path-boundary checks.
- The token bucket is shared per route and policy revision; it is not per user.
- The in-flight limit is also per route and process.
- Latency and injected failures are deterministic for a policy revision and
  request key. Concurrent request ordering is not deterministic.
- A timeout or connection failure can be injected, or may occur naturally when
  an upstream is unavailable. HTTP 429 and 5xx responses remain visible to callers.
- The first implementation buffers request and response bodies in memory.
- The control surface is unauthenticated and must remain bound to localhost in
  this local-only environment. Do not expose it to an untrusted network.
- Runtime counters exposed by /_simulator/stats are process-local snapshots;
  they are separate from exported OpenTelemetry metrics.
- The gateway exports OTLP metrics over HTTP to the existing Collector at
  /v1/metrics. The Collector receives the metrics pipeline and writes its
  debug exporter output to its own logs. No Prometheus or additional monitoring
  service is required or provisioned by this simulator.

## Gateway metrics

The gateway records two data-plane instruments:

| Instrument | Type | Unit | Meaning |
|---|---|---|---|
| gateway.requests | Counter | {request} | Requests handled by the gateway |
| gateway.request.duration | Histogram | s | End-to-end gateway request duration |

Both instruments use the same bounded attributes:

- http.request.method: standard method names; unknown methods become _OTHER.
- http.response.status_code: the numeric HTTP response status.
- gateway.simulation.outcome: a fixed set including forwarded, rate_limited,
  overloaded, injected failures, upstream failures, and status-class fallbacks.

Control endpoints under /_simulator are excluded. Paths, route IDs, correlation
IDs, trace IDs, span IDs, and simulation request IDs are never metric attributes.
The gateway service configures the endpoint with
OTEL_EXPORTER_OTLP_METRICS_ENDPOINT; the Compose stack points it at the existing
Collector HTTP receiver.
