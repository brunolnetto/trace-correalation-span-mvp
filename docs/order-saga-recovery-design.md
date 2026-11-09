# Durable order Saga recovery design

Status: proposed implementation design for issue #24. This is intentionally separate from the Sagaz 1.5.0 compatibility assessment in [sagaz-1.5.0-compatibility.md](sagaz-1.5.0-compatibility.md).

## Decision

Keep Sagaz's decorated order action/compensation methods as the business-operation definitions, but do not use Sagaz 1.5.0's declarative `run()` as the durability boundary. The pinned execution engine does not checkpoint steps, does not resume records, and treats storage errors as warnings.

Implement a small application-owned coordinator and persistence adapter on the existing psycopg 3 `DatabaseManager` pool. This avoids adding a second asyncpg pool and makes the durability contract explicit. The coordinator invokes the existing bound action/compensation methods in their current dependency order.

This is deliberately not a generic workflow engine. It covers the current order flow:

```text
reserve_inventory -> charge_payment -> confirm_order
       |                   |
       v                   v
release_inventory      refund_payment
```

Compensations run in reverse order, preserving the existing behavior.

## Durable execution record

A migration will add two application-owned tables.

### `order_saga_executions`

One row per business Saga:

- `saga_id` (stable primary key)
- `order_id` (unique business order identity)
- `saga_name` and `schema_version`
- `status`: `running`, `compensating`, `completed`, `rolled_back`, or `reconciliation_required`
- `context` (JSONB), including the immutable request/product/price snapshot needed to resume
- `last_error`, `created_at`, and `updated_at`

### `order_saga_steps`

One row per declared step:

- `saga_id`, `step_name` (composite primary key)
- `forward_status`: `pending`, `running`, `completed`, or `failed`
- `compensation_status`: `not_required`, `pending`, `running`, `completed`, or `failed`
- forward result JSONB and forward error
- compensation error
- forward/compensation attempt counts and timestamps

Status changes are monotonic. Completed steps are not reset to pending by a retry. A completed execution is immutable except for diagnostic metadata.

## Write-ahead step protocol

Each forward action follows this protocol:

1. Commit `forward_status = running` before calling the remote/domain operation.
2. Execute the operation using a stable idempotency identity derived from the Saga ID.
3. Commit the result and `forward_status = completed`.

The durable `running` state is an ambiguity marker, not proof that the effect did or did not happen. Recovery retries the same operation with the same key and expects the downstream adapter to return the original effect rather than duplicate it.

For a deterministic business rejection (for example, an explicit payment decline or Inventory 409), mark the step failed and begin compensation. For a timeout/transport failure where the remote effect may have committed, retain the execution as recoverable and retry/reconcile with the same key. Never mark it completed based only on an attempted call.

## Side-effect idempotency

### Inventory reservation

The existing Inventory API derives the reservation ID from `saga_id + product_id`. A repeated reservation request with the same order/product/quantity returns the existing reservation while it remains reserved. A different request or a previously consumed reservation is rejected. This is suitable for replay of an ambiguous reserve call; compensation uses the persisted reservation ID and the idempotent release endpoint.

### Payment charge and refund

The current PaymentGateway is an in-memory dictionary and is lost on restart. Replace its production implementation with a PostgreSQL adapter:

- stable `payment_id` derived from `saga_id`;
- unique charge idempotency key;
- amount/order fingerprint validation on key reuse;
- statuses `declined`, `charged`, and `refunded`;
- a repeated charge returns the existing charged payment;
- a repeated refund returns the already-refunded payment without applying a second refund.

The deterministic local approval/decline rule remains the test adapter's behavior. The durable adapter records the decision/result so recovery sees the same outcome.

### Order confirmation

The existing order outcome is stored by `order_id`, but an upsert can overwrite a row with another Saga ID. The durable execution identity must be checked during confirmation: replay for the same Saga is allowed; a different Saga attempting to claim the same order is a conflict. Confirmation is idempotent under the same `order_id + saga_id`.

## Compensation protocol

For every compensable step:

1. Commit `compensation_status = running` before calling the compensation.
2. Execute the compensation with its stable remote identity.
3. Commit `compensation_status = completed`.

If the process dies during compensation, startup recovery sees `compensating` plus a non-completed compensation status and retries that compensation. Inventory release and payment refund must be idempotent.

If compensation cannot complete after its configured attempt policy, persist `compensation_status = failed` and set execution status to `reconciliation_required`. Do not convert this state to `rolled_back` or `completed`; retain the error, step, and remote IDs for an operator.

## Concurrent ownership

All execution and recovery entry points acquire a PostgreSQL session-level advisory lock derived from the Saga ID. The lock is held on a checked-out connection while the workflow runs; individual state writes use short independent transactions through the existing pool.

A process crash closes its PostgreSQL session and releases the lock. A second worker can then recover the execution. The lock is not a lease timestamp and does not depend on wall-clock expiry.

## Startup recovery

After the Order API opens its PostgreSQL pool and has its Inventory dependency configured, startup recovery scans `running` and `compensating` executions in deterministic creation order.

- `running`: resume the forward sequence, skipping completed steps and replaying pending/ambiguous steps with stable keys.
- `compensating`: resume only incomplete compensations in reverse order; never return to forward execution.
- `reconciliation_required`: leave untouched for explicit operator intervention.
- `completed` / `rolled_back`: terminal; never rerun.

A recovery failure must be persisted and logged, but must not silently promote the Saga to success. Health/readiness should only be considered ready once the startup recovery pass has completed or each unresolved record has been retained in a visible recoverable/reconciliation state.

## Tests required before closing #24

Use real PostgreSQL and independent Order API processes where possible. Inject process termination at these boundaries:

1. Before a step starts: restart should execute it once.
2. After Inventory creates a reservation but before the step result is stored: restart should retrieve the same reservation ID and leave reserved balance unchanged.
3. After payment is committed but before its step checkpoint: restart should retrieve the same payment ID and not create another charge.
4. During compensation before remote call: restart should run the compensation.
5. After remote compensation commit but before its completion checkpoint: restart should retry safely and end compensated exactly once.
6. During compensation with a forced downstream failure: expose `reconciliation_required`, the incomplete step, and its last error.
7. Two independent recovery workers racing for one Saga: only one acquires the advisory lock and performs the operation.

The focused tests should assert database rows, Inventory balance/reservation state, Payment status, Order outcome, and the returned API/reconciliation representation—not only in-memory call counts.
