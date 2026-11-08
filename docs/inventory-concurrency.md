# Inventory transaction and concurrency contract

Issue #22 closes the gap between durable storage and correctness under
concurrent requests handled by independent Inventory API workers or processes.
PostgreSQL is the serialization authority when
`PERSISTENCE_BACKEND=postgres`; the in-memory store's `asyncio.Lock` is only
a single-process test/development implementation.

## Transaction boundary

Every mutating method in `PostgresInventoryStore` uses
`DatabaseManager.connection()`. That method checks out a Psycopg pooled
connection whose context owns a transaction:

- normal context exit commits;
- an exception escaping the context rolls back the entire transaction;
- the connection is returned to the pool only after transaction cleanup.

The following records must therefore commit or roll back together:

| Operation | Projection/state | Journal | Idempotency |
| --- | --- | --- | --- |
| Receipt | `inventory_balances.on_hand` | receipt movement | request hash + response |
| Adjustment | `inventory_balances.on_hand` | adjustment movement | request hash + response |
| Reserve | balance `reserved` + reservation row | reservation-created movement | deterministic reservation identity |
| Release | balance `reserved` + reservation status | reservation-released movement | terminal reservation status |
| Commit | balance `on_hand`/`reserved` + reservation status | reservation-committed movement | terminal reservation status |

An error after a projection update is not a partial success: it aborts the
transaction, including earlier inserts or updates in that operation.

## Locking strategy

The adapter uses PostgreSQL's default `READ COMMITTED` isolation with explicit
row locks. Locking is performed in this order:

1. **Reserve:** lock the product row, then the deterministic reservation
   identity, then the inventory balance row. The product lock coordinates
   reservation creation with product deactivation and serializes competing
   reservations for the same product. The balance lock also coordinates with
   receipts and adjustments.
2. **Release / commit:** lock the reservation row, then its inventory balance.
   Competing terminal transitions serialize on the reservation row. The
   second transaction observes the committed terminal status and cannot apply
   a second balance/journal transition.
3. **Receipt / adjustment:** lock the product's inventory balance row before
   checking idempotency and calculating the new balance. This serializes
   changes to one product across processes.

No inventory write path takes these locks in reverse order. The conservative
product lock in reserve reduces same-product reservation throughput, but gives
a straightforward serialization point for the active-product check and
reservation identity. Replacing it with a finer-grained strategy should require
equivalent concurrency tests, not intuition.

The database enforces the final balance invariants independently of application
validation:

- `on_hand >= 0`;
- `reserved >= 0`;
- `reserved <= on_hand`.

A rejected or racing update cannot commit a balance that violates these
constraints. The append-only movement triggers prevent rewriting the journal
after the transaction commits.

## Idempotency semantics

- Receipts and adjustments are scoped by operation and product. The first
  successful request stores a canonical request hash and response in the same
  transaction as the balance and movement writes. A retry with the same key
  and payload returns the stored response; a different payload is rejected.
- A reservation identity is deterministic from `(saga_id, product_id)`.
  Replaying the same active reservation request returns the existing
  reservation. Reusing that identity with a different order/quantity, or after
  it has reached a terminal state, is rejected.
- Repeated release and commit calls observe the persisted terminal state.
  A reservation cannot be both released and committed.

Idempotency is not a process-local cache. Independent API processes observe
the same records and locks through PostgreSQL.

## Integration proof

The PostgreSQL integration suite must cover more than concurrent coroutines
sharing one pool. The multi-process test starts two independent Uvicorn
Inventory API processes with separate connection pools against the same
isolated PostgreSQL schema and verifies:

- competing reservations cannot oversell;
- simultaneous receipt retries apply stock once and return one receipt ID;
- adjustment retries apply one movement;
- reusing a key/identity with a different payload is rejected;
- duplicate reservation creation returns one reservation identity;
- release and commit racing on one reservation produce one terminal transition;
- the final balance reconciles with the movement journal.

Failure-injection tests install database triggers that fail after the
projection/reservation writes but before the movement or idempotency write.
They verify that the balance, reservation, movement, and idempotency records
all roll back. Separate tests exercise the database check constraints directly.

## Operational considerations

The database lock is held until transaction completion. Keep network calls,
logging I/O, and unrelated work outside these transactions. The current
Inventory adapter performs only database work while holding row locks.

The adapter does not add an automatic retry loop for arbitrary database errors.
A transaction aborted by PostgreSQL must be retried as a whole, using the same
receipt/adjustment idempotency key or reservation identity. Do not retry only
the final SQL statement.

The memory backend does not provide multi-process safety. Production must use
`PERSISTENCE_BACKEND=postgres`; never scale the in-memory implementation
horizontally and assume its `asyncio.Lock` protects shared inventory.
