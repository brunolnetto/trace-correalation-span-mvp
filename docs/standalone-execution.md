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

The required event contract is derived from the current producer log messages:

| Correlation scope | Required event name | Count |
|---|---|---:|
| Each order correlation ID | `order_generator.attempt` | 1 |
| Each order correlation ID | `order.process.started` | 1 |
| Each order correlation ID | `order.process.completed` | 1 |
| Each order correlation ID | `inventory.reservation.created` | 1 |
| Supplier correlation ID | `supplier.reorder_policy.updated` | 1 |
| Supplier correlation ID | `supplier.replenishment.evaluated` | 3 |
| Supplier correlation ID | `supplier.purchase_order.ordered` | 1 |
| Supplier correlation ID | `supplier.purchase_order.received` | 1 |
| Supplier correlation ID | `inventory.stock.received` | 1 |

There is no separate `supplier.purchase_order.created` log event in the current
producer. Creation is represented by the first
`supplier.replenishment.evaluated` event with decision
`purchase_order_created` and its purchase-order ID. The three replenishment
events correspond to the initial decision, the in-flight duplicate-suppression
decision, and the post-receipt decision.

The current Order Saga creates reservations but does not call the Inventory
reservation-commit endpoint, so `inventory.reservation.committed` is deliberately
not part of this contract. The endpoint exists, but requiring its event here would
make the barrier wait for an event this workflow never emits.

For the default 12-order scenario, the barrier requires 55 unique event records:
four per order correlation ID and seven under the supplier correlation ID.
It ignores events from other correlation IDs and counts each persisted event ID
once, so replayed/duplicated delivery cannot satisfy a missing pair. It polls
`GET /v1/events?limit=10000`; the Collector's persist-before-ack contract means
visibility through this read API is the durability boundary.

Polling uses bounded exponential backoff. Defaults are a 30-second timeout,
100 ms initial backoff and 2-second maximum backoff. Override them with
`SIMULATION_COLLECTOR_BARRIER_TIMEOUT_SECONDS`,
`SIMULATION_COLLECTOR_POLL_INITIAL_BACKOFF_SECONDS`, and
`SIMULATION_COLLECTOR_POLL_MAX_BACKOFF_SECONDS`. A timeout records the missing
correlation ID/event-name/count tuples in the simulation report and exits
non-zero; it never reports simulation success.

The Analytics Runner depends on successful completion of the Simulation Runner. It
must not infer completion from container startup order, a fixed sleep, or a healthy
Collector endpoint.

## Outputs and failure semantics

The Simulation Runner writes a machine-readable simulation report containing the
overall run ID, generator summaries, purchase-order details, and Collector barrier
result.

The Analytics Runner runs `dbt build`, records the process exit code and parsed
build summary in a final run report, and exits non-zero if dbt fails. It is part of
the `standalone` profile and depends on the Simulation Runner with Compose's
`service_completed_successfully` condition. Collector readiness remains a separate
health dependency; neither a fixed sleep nor a healthy Collector endpoint substitutes
for scenario completion.

Run the complete standalone lifecycle with:

```bash
bash scripts/run-standalone.sh
```

The host-side wrapper starts the profile in detached mode, waits for the
Simulation Runner, checks its container exit code, then waits for the Analytics
Runner and returns its exit code. Analytics still depends on the Simulation
Runner through Compose's `service_completed_successfully` condition.

Do not add `--abort-on-container-exit` or `--exit-code-from` to this workflow.
Those options stop the dependency services when the Simulation Runner exits,
which can kill the Analytics Runner before its dbt build completes.

By default, the wrapper removes containers and the network after completion
but preserves named volumes. Set `STANDALONE_KEEP_STACK=1` to leave the stack
for inspection, or `STANDALONE_BUILD=0` to skip image builds.

## Non-goals

- No Airflow, Prefect, Dagster, Celery, or other workflow engine.
- No coupling between Supplier Simulator and Event Collector.
- No direct inventory mutation by either runner.
- No replacement of the existing APIs or manual service workflows.
- No claim of durable Supplier Simulator state: supplier policies and purchase orders
  remain process-local in this MVP.


## Local execution

For a clean standalone execution, the wrapper removes previous containers
after each run. It does not delete named volumes:

```bash
bash scripts/run-standalone.sh
```

To exercise failure propagation, run:

```bash
bash scripts/test-standalone-failure-paths.sh
```

The failure-path check intentionally injects a Simulation Runner failure and
a failing `dbt` executable through temporary Compose overrides. It verifies
that analytics is not started after simulation failure and that the Analytics
Runner persists a failed run report and exits non-zero after dbt failure.
It leaves named volumes intact.

The Simulation Runner writes `simulation-report.json` to the `run-state` volume.
The Analytics Runner writes `run-report.json`, the DuckDB database, and dbt
artifacts to the `analytics-data` volume. Both reports remain available after
the wrapper removes containers because named volumes are preserved.

To intentionally erase the raw event history, simulation and analytics reports,
and DuckDB data, remove the named volumes as well:

```bash
docker compose down -v
```

**Warning:** this permanently deletes the persisted data in those volumes. Do
not use `-v` for routine cleanup.

The default scenario uses product 7 with 42 opening units, eight initial
two-unit orders, one 34-unit purchase order, four more two-unit orders during
lead time, and a final inventory position of 52 after receipt.

The runner validates the opening balance and rejects a run if the product has
pre-existing reservations or an active purchase order. Each run gets a distinct
run ID, order IDs, order correlation IDs, and one separate supplier correlation
ID. Set `STANDALONE_RUN_ID` only when an explicitly controlled identifier is
required; do not reuse it to rerun a scenario because the Order API in this MVP
does not yet provide durable order-request idempotency.

The Simulation Runner waits for the correlation-scoped Collector completion
barrier before reporting success. Compose starts the Analytics Runner only after
the simulation exits successfully. The host wrapper waits for each finite
container separately, checks the actual exit code from `docker compose ps
--all --format json`, and only then removes the containers. It never uses
`--abort-on-container-exit` for this dependency chain.
