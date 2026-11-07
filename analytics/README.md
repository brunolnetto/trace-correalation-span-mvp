# Order and Inventory Analytics

This dbt project models the versioned raw event contract emitted by the Event
Collector. DuckDB reads the JSONL file directly; dbt transformations can be
rebuilt without mutating raw records.

## Run with Docker

Start the services and produce correlated Order API plus generator events
(for a fresh demo environment, use a clean Compose volume first):

```bash
docker compose up --build --detach
docker compose --profile simulation run --build --rm order-generator python -m app.order_generator --mode coverage
docker compose --profile analytics run --build --rm analytics
```

The analytics container mounts `event-data` read-only and persists the
DuckDB database in `analytics-data`. It does not modify the raw JSONL.

## Run locally

Install the pinned adapter:

```bash
python -m pip install -r analytics/requirements.txt
```

Then from `analytics/`:

```bash
RAW_EVENTS_PATH=../data/events.jsonl \
DUCKDB_PATH=target/trace_events.duckdb \
dbt build --profiles-dir .
```

Without a collector dataset, dbt uses `seeds/fixture_events.jsonl` as a small
contract fixture. Set `RAW_EVENTS_PATH` to the collector file to model actual
service events.

## Models

- `stg_raw_events`: typed envelope fields plus JSON attributes/source payload.
- `fct_orders`: one row per `correlation_id`, classified from explicit
  Order/Inventory events.
- `fct_inventory_movements`: on-hand and reserved deltas reconstructed only
  from explicit Inventory API movement log events. Replayed receipts,
  adjustments, and reservation transitions are deduplicated using the source
  movement/receipt/reservation identity rather than the log event ID.
- `fct_order_generator_attempts` and `fct_order_generator_runs`: generator
  requests and aggregate run metrics, with deterministic `plan_id` separated
  from unique execution `run_id`.
- `fct_order_reconciliation`: joins generator attempts to Order API outcomes
  by `correlation_id`; a singular dbt test fails on missing or mismatched
  service outcomes.
- `fct_order_daily_metrics` and `fct_inventory_daily_metrics`: daily
  operational aggregates.
- `fct_supplier_replenishment_decisions`,
  `fct_supplier_purchase_orders`, and `fct_supplier_daily_metrics`: reorder
  decisions and the created/ordered/received/cancelled purchase-order lifecycle.
  Repeated lifecycle log events are deduplicated by purchase order and
  transition.

The collector envelope keeps `correlation_id`, `trace_id`, and `span_id`
as separate fields. A correlation ID is a business request identifier, not a
trace ID.

## Known data boundaries

The facts represent observed log events, not transactional tables. They are
only as complete as the application events emitted by each service. Movement
deltas for reservation create/release/commit are available because those
events include product, order, reservation, and quantity. The models do not
pretend that trace spans are inventory movements or infer missing business
events from traces. Raw events in quarantine are intentionally excluded from
the staging model until their schema or payload issues are resolved.
