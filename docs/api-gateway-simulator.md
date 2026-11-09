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
- Runtime counters are process-local snapshots, not durable metrics. Traces and
  structured logs flow through the existing OpenTelemetry Collector, Jaeger,
  and Loki services.

Correlation IDs, trace IDs, span IDs, raw paths, and request keys must not be
used as metric labels. Keep request-specific identifiers in logs and traces.
