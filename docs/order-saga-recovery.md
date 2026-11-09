# Durable Order Saga execution and recovery

Issue: [#24](https://github.com/brunolnetto/trace-correalation-span-mvp/issues/24)

## Why Sagaz storage is not the recovery boundary

The dependency is pinned to `sagaz==1.5.0` in `pyproject.toml` and
`uv.lock`. The exact release source is commit
`1855ffdb1ef71c9431c4cc9e3e7fd4285ccd7d21`.

The project's root-level `Saga`, `action`, and `compensate` imports select
Sagaz's declarative execution API. In that version:

- `ExecutionEngine.run()` persists only the top-level start and terminal states.
- Both calls pass `steps=[]`; the source has a `TODO: Track steps` comment.
- It does not load an existing execution to resume it.
- Storage errors are logged and swallowed.
- The PostgreSQL Saga backend is optional, requires `asyncpg`, and manages its
  own connection pool.

The alternate imperative snapshot/replay API is a different Saga
implementation, has fail-open snapshot capture, and does not durably track the
compensation lifecycle needed here. See the
[pinned-version compatibility assessment](sagaz-1.5.0-compatibility.md) and
the executable probes in `tests/test_sagaz_compatibility.py`.

The Order API therefore does not rely on Sagaz storage for durability. Sagaz's
decorated methods remain the definitions of the business actions and
compensations; the application's PostgreSQL coordinator owns execution state.

## Durable state model

Migration `0005_durable_order_saga.sql` adds three tables:

- `order_saga_executions`: stable Saga/order identity, request fingerprint,
  lifecycle state, recovery attempt count, last error, and serialized context.
- `order_saga_steps`: forward status/result/attempts and compensation
  status/error/attempts for every declared action.
- `order_payments`: durable demo charge/refund decisions, tied to the same
  Saga and order.

The same PostgreSQL pool used by the Order API is used for these writes. There
is no second asyncpg pool. The durable executor holds a session-level advisory
lock on one checked-out connection while repository operations use other pool
connections. Therefore PostgreSQL mode requires
`DATABASE_POOL_MAX_SIZE >= 2`; the Compose configuration uses 10.

An order ID is claimed under a transaction-scoped advisory lock. Its request
fingerprint covers order ID, product ID, quantity, unit price, and total
amount. A repeated identical request loads the original Saga; a different
request for the same order ID receives HTTP 409.

## Forward execution protocol

The current action order is unchanged:

```text
reserve_inventory -> charge_payment -> confirm_order
```

Each action follows a write-ahead protocol:

1. Persist `forward_status = running` and the current context.
2. Call the action using the stable Saga identity.
3. Persist the action result, updated context, and
   `forward_status = completed`.

A process can die after step 2 but before step 3. In that case the persisted
`running` status is an ambiguity marker. Recovery repeats the operation with
the same identity; it does not assume the side effect did not happen.

### Inventory reservation

The existing Inventory API derives the reservation identity from
`saga_id + product_id`. An identical retry returns the existing active
reservation; reuse with changed order/product/quantity or a consumed
reservation is rejected. This makes a lost reservation response safe to retry.

The recovery tests use the actual `PostgresInventoryStore` implementation to
prove that a lost response does not create a second reservation or movement.

### Payment charge and refund

PostgreSQL mode uses `PostgresPaymentGateway` and `order_payments`, not the
process-local test double. The deterministic payment ID is derived from
`saga_id`. The row stores an order/amount fingerprint and one of
`declined`, `charged`, or `refunded` statuses.

- A repeated charge for the same Saga and request returns the existing charged
  payment.
- Reuse with a different order/amount is rejected.
- A repeated decline returns the same deterministic decline and payment ID.
- Refund is idempotent: retrying an already-refunded payment does not apply a
  second refund.

Payment creation is serialized with a transaction-scoped advisory lock derived
from the payment ID, in addition to the unique database constraints.

This is a durable demo payment ledger, not an integration with a real payment
processor.

### Order confirmation

Order confirmation writes the order outcome through
`PostgresOrderRepository`. An upsert can update an existing order only when
the existing `saga_id` matches the incoming Saga ID. Retrying confirmation
for the same execution is safe; another Saga cannot silently take over the
order ID.

The order row's reservation/payment references are synchronized in the same
transaction as the durable step checkpoint, so the projection does not lose
known side-effect identifiers between steps.

## Compensation protocol

The compensation order remains the reverse of the forward sequence:

```text
refund_payment -> release_inventory
```

Compensatable steps are initialized with `compensation_status = pending`;
the non-compensatable `confirm_order` step uses `not_required`.

For each completed forward action with a compensation:

1. Persist `compensation_status = running`.
2. Execute the compensation.
3. Persist `compensation_status = completed`.

If the process dies after a remote compensation commits but before step 3,
recovery retries the same idempotent compensation. A compensation exception is
recorded as `failed` and moves the execution to
`reconciliation_required`. Such executions are not silently marked rolled
back or completed and are excluded from automatic startup recovery.

The test suite verifies that compensation order remains
`refund_payment` then `release_inventory`, and that both a refund and an
Inventory release can be retried after their remote effect committed but
before the local completion checkpoint.

## Execution ownership and startup recovery

A session-level PostgreSQL advisory lock derived from the Saga ID is held for
the duration of one execution attempt. Repository writes use short independent
transactions through the same pool. If the process exits, PostgreSQL closes
the lock-owning session and releases its lock.

During Order API lifespan startup, after its PostgreSQL pool opens, the API
scans executions in `running` or `compensating` state. In Docker Compose,
Order API starts only after the Inventory API passes its health check.

- `running`: skip completed actions and replay pending/ambiguous actions in
  forward order.
- `compensating`: skip completed compensations and resume incomplete
  compensations in reverse order.
- `completed` and `rolled_back`: terminal; never execute again.
- `reconciliation_required`: leave visible for an operator; do not
  automatically replay.

The same order request can also trigger recovery by calling
`POST /orders/{order_id}/process` again with the original product and
quantity. Reusing the order ID with a different request returns 409.

A transient recovery error is logged and leaves the execution in its durable
nonterminal state. Startup recovery runs once per process start; a subsequent
retry of the same order request also attempts recovery under the same advisory
lock.

## Inspecting recovery state

Use:

```bash
curl http://localhost:18000/orders/123/recovery
```

The endpoint returns Saga ID, order ID, lifecycle status, failure category,
last error, recovery-attempt count, known reservation/payment IDs, and each
step's forward/compensation status, attempts, and errors. It returns 404 when
no durable execution exists or when the API is running with the in-memory
backend.

A `reconciliation_required` execution requires inspection of the reported
step/error and verification against the Inventory and payment records. Do not
manually mark it completed merely to clear the status. This MVP exposes the
diagnostic record but does not yet provide an authenticated operator action to
resolve it.

## PostgreSQL tests

Set `POSTGRES_TEST_URL` to a disposable PostgreSQL database and run:

```bash
uv run pytest tests/test_sagaz_compatibility.py -q
uv run pytest \
  tests/test_postgres_migrations.py \
  tests/test_postgres_order_saga_recovery.py -q
uv run ruff check .
```

The Order recovery integration tests create isolated schemas and apply all
migrations. They verify:

- An execution created before its first action resumes from a fresh pool.
- A real Inventory reservation followed by a lost response is retried without
  a second reservation or movement.
- A payment commit before its step checkpoint is recovered without creating
  another payment row.
- An order-confirmation commit before its step checkpoint is replayed without
  creating a duplicate order.
- A real Inventory release committed before its compensation checkpoint is
  retried without a second release movement.
- A payment refund committed before its compensation checkpoint is retried
  idempotently, before Inventory release.

These tests simulate process restart by closing the original pool/repository
and opening a new independent pool against the same PostgreSQL schema. The
existing Inventory PostgreSQL integration suite separately exercises
multi-process reservation idempotency and transaction semantics.
