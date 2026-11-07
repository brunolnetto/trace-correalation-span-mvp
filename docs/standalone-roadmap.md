# Standalone execution roadmap

## Objective

Provide one reproducible Compose command that exercises order generation, supplier replenishment, durable event collection, and dbt analytics as a finite workload:

`Order Generator → Order API → Inventory API ↔ Supplier Simulator → Event Collector → dbt/DuckDB`

The Supplier Simulator and Event Collector remain separate long-running services. Only two finite workload sidecars are added:

- **Simulation Runner** drives the scenario through existing public APIs.
- **Analytics Runner** consumes the Collector's persisted data read-only and runs `dbt build`.

This is deliberately not a request for Airflow, Prefect, Dagster, Celery, a new orchestrator service, or direct inventory mutations from a runner.

## Work breakdown and delivery order

### Phase 1 — Deterministic simulation scenario

**[#13 — Implement the finite deterministic Simulation Runner](../issues/13)**

Build a finite scenario driver that reuses the existing Order Generator and public Order, Supplier Simulator, and Inventory APIs.

The reference scenario uses product `7` with opening stock `42`, reorder point `30`, target stock `60`, and simulated lead time `3600` seconds:

1. Generate eight orders of two units. Assert inventory position `26`.
2. Evaluate replenishment and order one purchase order for `34` units.
3. Generate four more orders of two units while the PO is in flight.
4. Evaluate again and assert that the open PO suppresses a duplicate.
5. Advance the supplier clock by `3600` seconds and receive the PO through Inventory API.
6. Re-evaluate and assert final inventory position `52`, with no second PO.

**Exit condition:** scenario assertions pass and `simulation-report.json` is emitted. The report must carry run-specific identifiers needed by the completion barrier.

### Phase 2 — Durable event completion barrier

**[#14 — Add a durable Event Collector completion barrier](../issues/14)**

The Simulation Runner must not report success merely because service calls returned or containers are healthy. It must poll the Collector until every required event for this run is visible under the expected correlation ID.

The required event set must be derived from the existing producer implementations. The barrier must cover generated order attempts, Order/Inventory events, supplier policy/replenishment/PO lifecycle events, and the Inventory stock receipt event.

**Exit condition:** the runner exits successfully only after complete run-scoped event coverage is visible from the Collector's persisted read API. A bounded timeout reports missing correlation/event pairs; fixed sleeps are not completion logic.

### Phase 3 — Finite analytics execution

**[#15 — Implement the finite Analytics Runner for dbt](../issues/15)**

Validate the successful simulation report, read the Collector data volume read-only, execute the existing `dbt build`, and write `run-report.json` with the run ID, command outcome, build summary, and failure details.

**Exit condition:** report is emitted for success and failure; the process exit code reflects the dbt process result.

### Phase 4 — Compose orchestration and lifecycle

**[#16 — Wire the standalone Compose profile and finite job lifecycle](../issues/16)**

Add a `standalone` profile for the two finite runners. Keep the existing APIs, Supplier Simulator, Event Collector, and Jaeger as separate services.

Analytics Runner must depend on Simulation Runner with `service_completed_successfully`. The Collector event volume is read-only to analytics; DuckDB and run reports remain writable on dedicated volumes.

**Target invocation:**

```bash
docker compose --profile standalone up --build \
  --abort-on-container-exit \
  --exit-code-from analytics-runner
```

**Exit condition:** Compose configuration is valid, finite sidecars terminate, the analytics job's status is returned to the caller, and service-only/manual execution remains available.

### Phase 5 — End-to-end verification and documentation

**[#17 — Add standalone end-to-end verification and user documentation](../issues/17)**

Run the complete scenario from a clean Compose state. Verify inventory and PO invariants, Collector event coverage, dbt success, and failure propagation. Update README with invocation, prerequisites, configuration, report locations, cleanup, and known limitations.

**Exit condition:** one reproducible command validates the entire path locally, reports are inspectable, and the existing `scripts/reproduce-local.sh` workflow remains functional.

## Dependency graph

```text
#13 Simulation Runner
  |
  v
#14 Collector completion barrier
  |
  v
#15 Analytics Runner
  |
  v
#16 Compose profile and lifecycle
  |
  v
#17 End-to-end verification and README
```

The Compose integration (#16) consumes all three implementation components (#13–#15); the final verification (#17) closes the full delivery.

## Cross-cutting engineering constraints

- Use the existing public APIs; do not couple the Supplier Simulator to the Event Collector.
- Do not write stock, reservations, or PO state directly from either runner.
- Use unique run and correlation identifiers so previous executions cannot satisfy a new run's completion barrier.
- Treat service health as readiness only. Treat the Collector's persisted event visibility as the asynchronous event-delivery barrier.
- Bound network calls and polling; surface actionable diagnostics on timeout or failed assertions.
- Keep Supplier Simulator policy and PO state documented as process-local in this MVP. Do not imply restart durability.
- Do not add an orchestration framework.
- GitHub Actions runner allocation failures are an accepted infrastructure limitation for this work; local test and Compose evidence is the validation record.

## Definition of done

The implementation is complete when:

1. A clean standalone Compose execution runs the deterministic scenario and checks every inventory/PO invariant.
2. The Simulation Runner waits for run-scoped event persistence before exiting successfully.
3. Analytics starts only after successful simulation completion and exits with the actual dbt outcome.
4. Both machine-readable reports are available for inspection.
5. The final command returns non-zero for simulation, persistence-barrier, or dbt failure.
6. The existing individual-service and local reproduction workflows still work.
