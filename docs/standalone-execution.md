# Standalone execution

## Intent

Provide one finite, reproducible execution of the existing order-to-inventory-to-supplier
workflow followed by the dbt analytics build. This is a Compose execution profile, not
a new orchestration platform. The implementation sequence and issue dependencies are tracked in
[the standalone execution roadmap](standalone-roadmap.md).

The Supplier Simulator and Event Collector remain independent services with separate
responsibilities and lifecycles.

## Runtime components

| Component | Lifecycle | Responsibility |
|---|---|---|
| Order API | Long-running service | Order workflow and Saga |
| Inventory API | Long-running service | Product catalog, stock, reservations and movement ledger |
| Supplier Simulator | Long-running service | Supplier policies, purchase orders, simulated lead time and receipts |
| Event Collector | Long-running service | Versioned ingestion, deduplication, durable JSONL persistence and quarantine |
| Simulation Runner | Finite sidecar | Drives a deterministic scenario through the public Order and Supplier APIs |
| Analytics Runner | Finite sidecar | Waits for successful simulation completion and runs `dbt build` |
| Jaeger | Supporting service | Distributed trace collection and inspection |

The Simulation Runner is deliberately not part of the Supplier Simulator. It is a
scenario driver and must not contain supplier-domain rules or write inventory state
directly. It invokes the existing Order Generator and Supplier Simulator APIs.

The Analytics Runner is deliberately separate from the Event Collector. It consumes
the Collector's persisted event volume read-only and writes DuckDB analytics to its
own volume.

## Deterministic scenario

Use the existing demo product (ID 7, opening stock 42) so that demand and replenishment
operate on the same inventory position.

1. Install a reorder policy for product 7: reorder point 30, target stock 60 and a
   simulated lead time of 3,600 seconds.
2. Generate eight orders of two units through the Order API. Successful reservations
   reduce inventory position from 42 to 26.
3. Evaluate replenishment. The Supplier Simulator creates a purchase order for 34
   units and moves it to `ordered`.
4. Generate four more orders of two units while the purchase order is in flight.
5. Evaluate replenishment again. The open purchase order must suppress a duplicate.
6. Advance the supplier simulation clock by 3,600 seconds and receive the purchase
   order through the Inventory API.
7. Re-evaluate replenishment. The resulting inventory position is 52, above the
   reorder point, so no second purchase order should be created.

The generator uses fixed quantity bounds and deterministic order IDs for this
standalone scenario. Each generator invocation still has a unique run ID and unique
correlation IDs. The Supplier workflow uses a separate correlation ID.

## Completion barrier

Compose health checks only establish that services are ready; they do not establish
that a workload has completed or that asynchronous service log publishers have
delivered their queues.

The Simulation Runner must therefore wait for the expected events to be visible from
the Event Collector before it exits successfully. The Collector fsyncs accepted events
before acknowledging ingestion, so observing every expected correlation/event pair
provides a durable persistence barrier for this finite scenario.

The barrier must cover:
- each generator attempt and its corresponding Order API and Inventory API events;
- the Supplier Simulator's policy, replenishment, PO ordering and PO receipt events;
- the Inventory API stock receipt event.

The Analytics Runner depends on successful completion of the Simulation Runner. It
must not infer completion from container startup order, a fixed sleep, or a healthy
Collector endpoint.

## Outputs and failure semantics

The Simulation Runner writes a machine-readable simulation report containing the
overall run ID, generator summaries, purchase-order details, and collector barrier
result.

The Analytics Runner runs `dbt build`, records the process exit code and parsed
build summary in a final run report, and exits non-zero if dbt fails. A failed
simulation prevents the Analytics Runner from starting successfully.

The standalone profile should be invoked with Compose's `--abort-on-container-exit`
and `--exit-code-from analytics-runner` options so the command returns the final
analytics job's status.

## Non-goals

- No Airflow, Prefect, Dagster, Celery, or other workflow engine.
- No coupling between Supplier Simulator and Event Collector.
- No direct inventory mutation by either runner.
- No replacement of the existing APIs or manual service workflows.
- No claim of durable Supplier Simulator state: supplier policies and purchase orders
  remain process-local in this MVP.
