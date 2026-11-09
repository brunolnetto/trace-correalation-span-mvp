# Durable Order Saga execution and recovery

Issue: [#24](https://github.com/brunolnetto/trace-correalation-span-mvp/issues/24)

## Why Sagaz storage is not the recovery boundary

The dependency is pinned to `sagaz==1.5.0` in `pyproject.toml` and `uv.lock`. The exact release source is commit `1855ffdb1ef71c9431c4cc9e3e7fd4285ccd7d21`.

The project's root-level `Saga`, `action`, and `compensate` imports select Sagaz's declarative execution API. In that version:

- `ExecutionEngine.run()` persists only the top-level start and terminal states.
- Both calls pass `steps=[]`; the source has a `TODO: Track steps` comment.
- It does not load an existing execution to resume it.
- Storage errors are logged and swallowed.
- The PostgreSQL Saga backend is optional and requires `asyncpg`; it manages its own pool.

The alternate imperative snapshot/replay API is a different Saga implementation, has fail-open snapshot capture, and does not durably track the compensation lifecycle needed here. See [the pinned-version compatibility assessment](sagaz-1.5.0-compatibility.md) and the executable probes in `tests/test_sagaz_compatibility.py`.

The Order API therefore does not rely on Sagaz storage for durability. Sagaz's decorated methods remain the definitions of the business actions and compensations; the application's PostgreSQL coordinator owns execution state.

## Durable state model

Migration `0005_durable_order_saga.sql` adds three tables:

- `order_saga_executions`: stable Saga/order identity, request fingerprint, lifecycle state, recovery attempt count, last error, and serialized execution context.
- `order_saga_steps`: one row per action, forward status/result/attempts, and compensation status/error/attempts.
- `order_payments`: durable demo charge/refund decisions, tied to the same Saga and order.

The same PostgreSQL pool used by the Order API is used for these writes. There is no second asyncpg pool.

An order ID is claimed under a transaction-scoped advisory lock. Its request fingerprint covers order ID, product ID, quantity, unit price, and total amount. A repeated identical request loads the original Saga; a different request for the same order ID receives HTTP 409.

## Forward execution protocol

The current action order is unchanged:

```text
reserve_inventory -> charge_payment -> confirm_order
```

Each action follows a write-ahead protocol:

1. Persist `forward_status = running` and the current context.
2. Call the action using the stable Saga identity.
3. Persist the action result, updated context, and `forward_status = completed`.

A process can die after step 2 but before step 3. In that case the persisted `running` status is an ambiguity marker. Recovery repeats the operation with the same identity; it does not assume the side effect did not happen.

### Inventory reservation

The existing Inventory API derives the reservation identity from `saga_id + product_id`. An identical retry returns the existing active reservation; reuse with changed order/product/quantity or a consumed reservation is rejected. This makes a lost reservation response safe to retry.

### Payment charge and refund

PostgreSQL mode uses `PostgresPaymentGateway` and `order_payments`, not the process-local test double. The deterministic payment ID is derived from `saga_id`. The row stores order/amount fingerprint and `declined`, `charged`, or `refunded` status.

- A repeated charge for the same Saga and request returns the existing charged payment.
- Reuse with a different order/amount is rejected.
- A repeated decline returns the same deterministic decline.
- Refund is idempotent: a retry of an already-refunded payment does not apply another refund.

This is a durable demo payment ledger, not an integration with a real payment processor.

### Order confirmation

Order confirmation writes the order outcome through `PostgresOrderRepository`. An upsert can update an existing order only when the existing `saga_id` matches the incoming Saga ID. Retrying confirmation for the same execution is safe; another Saga cannot silently take over the order ID.

## Compensation protocol

The compensation order remains the reverse of the forward sequence:

```text
refund_payment -> release_inventory
```

For each completed forward action with a compensation:

1. Persist `compensation_status = running`.
2. Execute the compensation.
3. Persist `compensation_status = completed`.

If the process dies after a remote compensation commits but before step 3, recovery retries the same idempotent compensation. A compensation exception is recorded as `failed` and moves the execution to `reconciliation_required`. Such executions are not silently marked rolled back or completed and are excluded from automatic startup recovery.

## Execution ownership and startup recovery

A session-level PostgreSQL advisory lock derived from the Saga ID is held for the duration of one execution attempt. Repository writes use short independent transactions through the same pool. If the process exits, PostgreSQL closes the lock-owning session and releases its lock.

During Order API lifespan startup, after PostgreSQL is available and the Inventory dependency is healthy, the API scans executions in `running` or `compensating` state:

- `running`: skip completed actions and replay pending/ambiguous actions in forward order.
- `compensating`: skip completed compensations and resume incomplete compensations in reverse order.
- `completed` and `rolled_back`: terminal; never execute again.
- `reconciliation_required`: leave visible for an operator; do not auto-replay.

The same order request can also trigger recovery by calling `POST /orders/{order_id}/process` again with the original product and quantity. Reusing the order ID with a different request returns 409.

## Inspecting recovery state

Use:

```bash
curl http://localhost:18000/orders/123/recovery
```

The endpoint returns Saga ID, order ID, lifecycle status, failure category, last error, recovery-attempt count, and each step's forward/compensation status, attempts, and errors. It returns 404 when no durable execution exists or when the API is running with the in-memory backend.

A `reconciliation_required` execution requires inspection of the reported step/error and verification against the Inventory and payment records. Do not manually mark it completed merely to clear the status. This MVP exposes the diagnostic record but does not yet provide an authenticated operator action to resolve it.

## PostgreSQL tests

Set `POSTGRES_TEST_URL` to a disposable PostgreSQL database and run:

```bash
uv run pytest tests/test_sagaz_compatibility.py -q
uv run pytest tests/test_postgres_migrations.py tests/test_postgres_order_saga_recovery.py -q
uv run ruff check .
```

The Order recovery integration tests create isolated schemas and apply all migrations. They verify:

- a persisted execution created before the first action is executable from a fresh pool;
- an Inventory effect followed by a lost response is retried with the same business key and is not duplicated;
- a payment commit before its step checkpoint is loaded by a fresh pool without creating a second payment row;
- a compensation effect committed before its completion checkpoint is safely retried after a fresh pool is opened.

These tests simulate process restart by closing the original pool/repository and opening a new independent pool against the same PostgreSQL schema. The existing Inventory PostgreSQL integration suite separately exercises the actual reservation idempotency and transaction semantics.
