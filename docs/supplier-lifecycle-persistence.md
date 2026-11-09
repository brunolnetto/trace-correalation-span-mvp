# Durable Supplier Lifecycle

## Ownership boundary

The Supplier Simulator owns supplier offerings, onboarding references, reorder
policies, purchase orders, their lifecycle history, and the deterministic
simulation clock. The Inventory API remains the sole owner of product stock,
reservations, inventory balances, and stock receipts.

The Supplier Simulator may read Inventory state and request a receipt through
the public Inventory API. It never writes Inventory database tables directly.
Both services use the shared PostgreSQL database, but supplier tables and
inventory tables have separate ownership.

## Persisted model

Migration `0004_persist_supplier_lifecycle.sql` creates:

| Table | Purpose |
| --- | --- |
| `supplier_offerings` | Fixture catalog and durable mapping from supplier SKU to Inventory product |
| `supplier_reorder_policies` | One versioned reorder policy per product |
| `supplier_purchase_orders` | PO header, lifecycle state, policy snapshot, and receipt reference |
| `supplier_purchase_order_lines` | Product, supplier SKU, ordered quantity, and unit-cost snapshot |
| `supplier_purchase_order_transitions` | Append-only transition history, correlation ID, and metadata |
| `supplier_simulation_clock` | Singleton deterministic clock that survives process restart |

The fixture catalog is seeded by the migration. Onboarding first creates or
recovers the Inventory product by SKU, then commits the product mapping and
default policy together. If Inventory created the product but the supplier
process stopped before saving the mapping, retrying onboarding handles the
Inventory SKU conflict by reading the existing product. A repeated onboarding
does not overwrite a policy that has since been customized.

PO lines and transition history are immutable. The database rejects UPDATE,
DELETE, and TRUNCATE against both tables. A PO header can change state only
through the supplier application adapter, and the history insert shares the
same transaction as that state change.

## Replenishment concurrency

The reorder evaluation uses:

`inventory_position = on_hand - reserved + on_order`

A product's reorder-policy row is the serialization point for PO creation.
Evaluation reads the authoritative Inventory balance, locks the policy row,
then checks for active purchase orders. The database also enforces a partial
unique index allowing only one active PO for a product while the status is
`created`, `ordered`, or `receiving`.

The partial unique index is not a substitute for the policy lock: it is the
last invariant guard if a future code path attempts to create an active PO
without following the adapter's serialization protocol.

The policy carries an `inventory_revision`. Receipt finalization increments it
in the same transaction that changes the PO to `received`. Evaluation records
the revision before its first Inventory read. If the revision changed while
the HTTP read was in flight, it performs a fresh Inventory read while holding
the policy lock. Receipt finalization locks policy then PO, matching the
evaluation lock order. This closes the stale-read window where an evaluation
could otherwise see the old inventory balance after the previous PO had
already left the active set.

## Receipt recovery protocol

A supplier receipt crosses two independent transactional systems, so there is
no distributed transaction. The protocol is intentionally retryable:

1. Lock the PO and validate that it is ordered and due according to the
   persisted simulation clock.
2. Change `ordered -> receiving` and append the transition in one PostgreSQL
   transaction. Commit before making the HTTP request.
3. Call `POST /inventory/{product_id}/receipts` with the stable idempotency
   key `supplier-po:<purchase_order_id>` and the PO ID as the receipt reference.
4. Once Inventory acknowledges, lock policy then PO, change
   `receiving -> received`, persist the Inventory receipt ID, increment the
   policy inventory revision, and append the transition in one transaction.

The `receiving` state is a durable recovery checkpoint:

- **Crash before Inventory commits:** the PO remains `receiving`; a retry
  invokes Inventory with the same key and applies the receipt once.
- **Inventory commits but the response is lost:** the PO remains
  `receiving`; a retry replays the same Inventory idempotency record and
  obtains the original receipt ID without crediting stock twice.
- **Supplier commits `received` but the HTTP response to its caller is lost:**
  replaying the supplier endpoint returns the persisted received PO without
  calling Inventory again.

Concurrent receive requests may both reach Inventory while the PO is
`receiving`; the stable Inventory idempotency key makes this safe. The
transition uniqueness constraint and PO row lock ensure the supplier records
`receiving` and `received` only once.

The Supplier Simulator does not hold a database transaction open during the
Inventory HTTP request. This avoids keeping database locks across a network
boundary. Failures from Inventory leave the PO in `receiving` for retry or
operator reconciliation; they do not mark it received.

## Simulated clock

The clock is initialized only when the singleton row does not exist. Advancing
it uses one atomic SQL update, so concurrent increments are not lost and a
service restart does not reset PO due times. The clock is deliberately
simulation time, not wall-clock time; it advances only through the explicit
clock API.

## API surface

Existing endpoints retain their response contracts:

- `GET /supplier-catalog`
- `POST /supplier-catalog/{supplier_sku}/onboard`
- `GET /reorder-policies`
- `PUT /reorder-policies`
- `POST /replenishments/evaluate`
- `GET /purchase-orders`
- `GET /purchase-orders/{purchase_order_id}`
- `POST /purchase-orders/{purchase_order_id}/order`
- `POST /purchase-orders/{purchase_order_id}/receive`
- `POST /purchase-orders/{purchase_order_id}/cancel`
- `GET /purchase-orders/{purchase_order_id}/transitions`
- `GET /clock`
- `POST /clock/advance`

The transition-history endpoint returns ordered records with from/to state,
timestamp, correlation ID, and metadata.

## Configuration and validation

The Compose Supplier Simulator selects `PERSISTENCE_BACKEND=postgres` and
uses the same `DATABASE_URL` as Inventory and Order. Apply migrations before
starting the services:

```bash
docker compose up --detach --wait postgres
bash scripts/migrate.sh
docker compose up --build --detach
```

For PostgreSQL integration tests, set `POSTGRES_TEST_URL` to a disposable
database URL. The tests create and drop an isolated schema:

```bash
uv run pytest tests/test_supplier_simulator.py -q
uv run pytest tests/test_postgres_supplier_simulator.py -q
uv run ruff check .
```

The in-memory `SupplierSimulation` remains the unit-test/development adapter
when `PERSISTENCE_BACKEND=memory`. It models the same lifecycle but does not
provide restart or multi-process guarantees.
