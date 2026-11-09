# Transactional outbox and reliable delivery

Issue: [#25](https://github.com/brunolnetto/trace-correalation-span-mvp/issues/25)

## Guarantee and boundary

PostgreSQL mode writes business events to \`outbox_events\` in the same database
transaction as the domain mutation. Database triggers cover:

- Product creation and meaningful product updates.
- Inventory receipt, adjustment, reservation creation, release, and commit
  movements.
- Order creation/status transitions.
- Supplier onboarding and reorder-policy creation/updates.
- Supplier purchase-order creation, ordering, receiving, receipt, and
  cancellation transitions.
- Supplier replenishment decisions, including no-reorder and existing-PO
  outcomes.

A domain rollback also rolls back its outbox event. The trigger creates a
single immutable event envelope and UUID \`event_id\` before the transaction
commits. Correlation and active trace/span identifiers are copied from
transaction-local PostgreSQL settings populated by \`DatabaseManager\`.

The Event Collector remains the analytical append-only JSONL sink. It does not
own business state, and Loki/OTLP diagnostics are not substituted for business
events.

This is **at-least-once delivery**, not exactly-once transport. The same
\`event_id\` and payload are sent on every attempt. The Collector's durable
event-ID index makes replay idempotent; a conflicting payload under an
existing ID is quarantined and the outbox row is blocked.

## Dispatcher protocol

\`outbox-dispatcher\` is a separate Compose service. It polls bounded batches and
claims rows in a short PostgreSQL transaction using \`FOR UPDATE SKIP LOCKED\`.
Claims set a lease owner, expiry, and increment the attempt count. Multiple
workers can claim disjoint rows. A worker can reclaim an expired lease after
another worker exits or loses its database session.

For each claimed event:

1. POST the stored immutable event envelope to \`POST /v1/events\`.
2. Validate the Collector acknowledgement. Exactly one accepted or duplicate
   event and zero quarantined events are required.
3. Mark the row \`delivered\` only after a valid acknowledgement and only while
   the worker still owns the lease.

If the Collector persists an event but the HTTP response or local delivered
update is lost, the lease expires and the event is sent again with the same
ID. The Collector acknowledges it as a duplicate and the dispatcher can then
mark the outbox row delivered.

Transient network failures and HTTP 408/409/425/429/5xx responses are retried
with capped exponential backoff. Other HTTP errors, malformed acknowledgements,
and Collector quarantine responses are considered permanent and move the row
to \`blocked\`. Reaching \`OUTBOX_MAX_ATTEMPTS\` also blocks a repeatedly failing
row. Blocked rows are not automatically polled again, avoiding a busy loop.
The dispatcher finishes its currently claimed batch during graceful shutdown;
uncompleted leases become eligible after expiry.

The retry delay is:

\`min(OUTBOX_BASE_BACKOFF_SECONDS * 2 ** (attempts - 1), OUTBOX_MAX_BACKOFF_SECONDS)\`

There is no automatic operator replay endpoint in this MVP. Inspect the event,
last error, aggregate, and source domain state before an operator deliberately
requeues a blocked event. Do not change its \`event_id\` or payload when replaying.

## Operational endpoints

The dispatcher listens only on the internal Compose network (port 8011); it
does not publish a host port.

\`\`\`bash
docker compose exec outbox-dispatcher \\
  python -c 'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:8011/outbox/stats").read().decode())'

docker compose exec outbox-dispatcher \\
  python -c 'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:8011/outbox/blocked?limit=100").read().decode())'
\`\`\`

\`GET /outbox/stats\` reports pending/leased count, age of the oldest
pending/leased event, retry count, blocked count, and delivered count.
\`GET /outbox/blocked\` reports event identity, event type, service, aggregate,
correlation ID, attempts, last error, and timestamps without exposing the
full payload by default. \`/healthz\` is unhealthy if PostgreSQL is down or the
dispatcher task has stopped.

These are internal development/operations endpoints. API authentication and
authorization are tracked separately in issue #27; do not expose port 8011
outside the trusted Compose network.

## Configuration

Compose provides these defaults:

| Variable | Default | Meaning |
|---|---:|---|
| \`OUTBOX_BATCH_SIZE\` | \`50\` | Maximum rows claimed per poll |
| \`OUTBOX_LEASE_SECONDS\` | \`30\` | Lease expiry for one claimed row |
| \`OUTBOX_MAX_ATTEMPTS\` | \`12\` | Attempts before a retryable failure is blocked |
| \`OUTBOX_BASE_BACKOFF_SECONDS\` | \`1\` | Initial retry delay |
| \`OUTBOX_MAX_BACKOFF_SECONDS\` | \`300\` | Retry delay cap |
| \`OUTBOX_POLL_INTERVAL_SECONDS\` | \`0.5\` | Idle poll interval |
| \`OUTBOX_REQUEST_TIMEOUT_SECONDS\` | \`5\` | Collector HTTP request timeout |

The lease should exceed the Collector request timeout by a comfortable margin.
If event delivery routinely takes longer than the configured lease, adjust the
lease/timeout and batch limits together.

## Best-effort events and diagnostic logs

The stdout JSON logger remains the diagnostic log source. Only explicitly
allowlisted business event names are eligible for analytical Event Collector
forwarding. In PostgreSQL mode, names captured by database triggers are
suppressed by the process-local \`AsyncCollectorHandler\`; forwarding those logs
would create duplicate business facts.

Two event classes currently remain best-effort because they have no committed
domain mutation to attach to: order-generator attempt events and rejected
Inventory reservation attempts. The process-local queue is retained only for
these events and in-memory development mode. Queue loss for these
non-transactional events remains possible and visible through its existing
in-process dropped/failed counters.

## Validation

The PostgreSQL outbox integration tests use isolated schemas and cover:

- Domain mutation rollback also rolling back its outbox event.
- Inventory product and movement event creation with correlation propagation
  and idempotent receipt replay.
- Order and Supplier onboarding/policy/replenishment/purchase-order events.
- Two concurrent dispatchers claiming disjoint batches.
- Expired-lease reclamation with the same event ID.
- Collector persistence followed by simulated loss of the local acknowledgement,
  then safe redelivery and delivered-state update.
- Transient 5xx retry, permanent 4xx blocking, attempts/errors, blocked listing,
  and operational stats.

Run the focused suite and linter with a disposable PostgreSQL database:

\`\`\`bash
POSTGRES_TEST_URL=postgresql://app:app@127.0.0.1:15432/app \\
  uv run pytest \\
    tests/test_postgres_migrations.py \\
    tests/test_postgres_outbox.py -q
uv run ruff check .
\`\`\`

The Event Collector's existing store tests separately verify append durability,
event-ID deduplication across its own restart, and conflicting-payload
quarantine. The full local reproduction exercises the end-to-end Collector
barrier with the dispatcher running as a Compose service.
