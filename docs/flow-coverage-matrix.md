# End-to-end flow and edge coverage matrix

This is the human-readable index for the canonical machine-readable inventory in
[`flow-coverage-matrix.json`](flow-coverage-matrix.json).

The JSON file is the source of truth. It enumerates component nodes, directed
edges, end-to-end flows, evidence references, coverage levels and prioritized
gaps. Keep its edge IDs stable when adding new tests so test reports, issue
checklists and runtime experiments can refer to the same contract.

## Snapshot and scope

- Repository: `brunolnetto/trace-correalation-span-mvp`
- Base branch: `main`
- Base commit: `f85183b83fda02b7eec42e2cfe2ffd2942e7ca10`
- Snapshot date: 2026-11-09
- PRs #39–#43 were merged into `main` before this snapshot; their URLs are retained as historical evidence.

The inventory covers synchronous ingress and service dependencies, PostgreSQL
persistence, Saga/payment compensation, the transactional outbox, best-effort
events, Collector storage, dbt/DuckDB analytics, and diagnostic traces/logs/
metrics. It distinguishes direct service access from Gateway-routed access.

## How to read coverage

| Field | Meaning |
|---|---|
| `automated` | Relevant tests exist; this does not claim they were rerun during this update. |
| `live` | Runtime evidence class, with open-PR validation explicitly distinguished. |
| `assessment` | `covered`, `partial`, or `gap` for the contract stated on that edge. |
| `evidence` | IDs resolved through `evidence_catalog` in the JSON file. |
| `gaps` | Missing operations, failure modes or runtime boundaries. |

A flow's `edge_path` is an ordered set of participating interaction boundaries,
not a strict graph walk: synchronous responses return to the caller implicitly,
and telemetry fan-out creates parallel branches.

A mocked HTTPX transport proves application behavior against a controlled
dependency. A PostgreSQL integration test proves database behavior. A live
Compose test proves the deployed HTTP boundary for the scenario it actually
runs. None should be silently promoted into evidence for another layer.

## Architecture boundary that matters most

The Gateway is an ingress reverse proxy. Internal service calls remain direct:

```text
Client → Gateway → Order API ─────────────→ Inventory API
                              └───────────→ payment adapter (in-process)
                         └───────────────→ PostgreSQL

Client → Gateway → Supplier API ─────────→ Inventory API
                              └───────────→ PostgreSQL

Inventory / Order / Supplier PostgreSQL transactions
  → transactional outbox
  → Outbox Dispatcher
  → Event Collector
  → JSONL volume
  → dbt / DuckDB

Applications → OpenTelemetry Collector → Jaeger / Loki
Gateway → OpenTelemetry Collector ← Prometheus scrape
Grafana → Prometheus / Loki
```

This prevents a common test-design error: injecting a Gateway failure on
`/orders` only tests whether Order receives the request. It says nothing about
what happens when Order has started a Saga and its direct Inventory dependency
fails.

## Highest-priority validation work

1. **P0 — Order → Inventory failure after Saga start.** Exercise preflight and
   reservation failure, ambiguous reservation response, payment decline,
   post-payment confirmation failure, refund/release failure, and restart
   recovery. Verify durable payment, reservation, movement and Saga state.
2. **P1 — Gateway → Supplier live lifecycle.** Onboard, evaluate replenishment,
   order a PO, advance the simulated clock and receive through Gateway. Verify
   Supplier transitions and Inventory movements.
3. **P1 — Outbox → Collector live outage/recovery.** Stop the Collector while
   committed events accumulate, restore it, and prove same event IDs are
   redelivered and the analytical projection is complete.
4. **P1 — Per-service telemetry.** Verify traces and searchable logs for
   Order, Inventory, Supplier, Dispatcher and Event Collector, not only the
   Gateway.
5. **P1 — Grafana query proof.** Query the metrics both through Prometheus and
   Grafana; datasource provisioning alone is not query evidence.

The full acceptance scenarios and affected edge IDs are in
`prioritized_gaps` in the JSON manifest.

## Updating the inventory

When a route, dependency, background worker, persistence adapter or exporter is
added or removed:

1. Add/update its node and directed edge(s).
2. Update every affected end-to-end flow's ordered `edge_path`.
3. Attach evidence by repository path and exact test or verifier scope.
4. Keep unit, integration, live and CI results distinct.
5. Record unmerged PR evidence separately from the base-branch snapshot.
6. Do not set `assessment: covered` until all failure modes in that edge's
   stated contract have credible evidence.

The manifest is validated by `tests/test_flow_coverage_matrix.py`, which checks
unique IDs, valid node/edge/evidence references and allowed coverage values.
The current manifest is intentionally a coverage baseline, not a claim that
every edge has already been exhaustively validated.
