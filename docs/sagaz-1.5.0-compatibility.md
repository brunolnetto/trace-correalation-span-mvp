# Sagaz 1.5.0 persistence compatibility assessment

Status: compatibility spike for issue #24. This document records what the pinned dependency actually guarantees before the order workflow is changed.

## Artifact identity

The application pins `sagaz==1.5.0`. PyPI's release provenance identifies the source repository commit as:

```text
brunolnetto/sagaz
1855ffdb1ef71c9431c4cc9e3e7fd4285ccd7d21
```

The inspected code paths below are taken from that exact source commit, rather than the repository's later default branch.

## What PostgreSQL storage provides

Sagaz 1.5.0 includes `PostgreSQLSagaStorage` at `sagaz.core.storage.backends.postgresql.saga`. It stores a Saga row and a related set of step rows in PostgreSQL. It can load a record by Saga ID and list records by status/name.

Important operational constraints:

- The backend imports `asyncpg` and raises `MissingDependencyError` if it is not installed.
- `asyncpg` is an optional extra (`sagaz[postgres]`), not part of the package's core dependency list.
- The backend creates its own asyncpg pool and its own `sagas` / `saga_steps` tables. It does not reuse this application's existing psycopg 3 `DatabaseManager` pool.
- The backend's `save_saga_state()` replaces the existing step rows for a Saga before inserting the supplied list. A caller must therefore supply a complete and current step snapshot.

## Critical limitation in the declarative API used by this application

The project imports `Saga`, `action`, and `compensate` from the package root. In Sagaz 1.5.0 this is the declarative API implemented by `sagaz.core.decorators`.

The pinned `sagaz/core/decorators/_execution.py` implementation does the following:

1. At run start, persists `SagaStatus.EXECUTING` with an empty step list (the source explicitly leaves a `TODO: Track steps` comment).
2. Executes the decorated steps without calling storage to checkpoint each step.
3. On success, persists `SagaStatus.COMPLETED`, again with an empty step list.
4. On failure, performs compensations and only then persists `SagaStatus.ROLLED_BACK`, also with an empty step list.
5. Catches and logs exceptions from all three state-save locations rather than failing the execution.

The step and compensation listener notifications are not persistence checkpoints. The engine's listener dispatcher catches listener exceptions and logs them, so a listener-only persistence adapter cannot make those checkpoints fail-closed.

Consequently, enabling `PostgreSQLSagaStorage` on this declarative Saga does **not** durably persist step outcomes or compensation progress. A crash in the middle leaves an `EXECUTING` row with no usable step history. `load_saga_state()` exists, but the declarative `run()` path does not load that record or resume it; a new `run()` initializes the context and compensation graph from scratch.

This repository's `create_order_processing_saga()` currently creates `SagaConfig` without a storage argument, so Sagaz selects `InMemorySagaStorage` by default. The compatibility test asserts that behavior and probes the actual write calls made by the pinned version.

## Why the separate snapshot API is not a drop-in fix

Sagaz 1.5.0 also contains a second, imperative `sagaz.core.saga.Saga` API with `ReplayConfig`, `SnapshotStorage`, and `execute_from_snapshot()`. This is not the declarative class imported by this application.

That API can snapshot context and completed-step names and skip those completed steps when resuming. It still has important limits for this workflow:

- Snapshot failures are caught and logged by `_capture_snapshot()`; they do not fail the step.
- The snapshot is written after the side effect and completion bookkeeping. A crash before the post-step snapshot still requires the side effect to be idempotent.
- The snapshot model does not record a durable compensation intent/result lifecycle.
- `SagaReplay` creates a new Saga ID for replay, which is not equivalent to resuming the existing business execution.

It is useful replay functionality, but not sufficient by itself for the acceptance criteria in issue #24.

## Consequence / implementation direction

Do not claim restart safety by merely swapping `InMemorySagaStorage` for `PostgreSQLSagaStorage`.

The application needs an explicit durable execution boundary that:

- stores the initial execution context and stable Saga identity before side effects;
- commits a per-step `running` intent before calling the remote service;
- stores the step result after the remote effect;
- replays an ambiguous `running` step with the same idempotency key after a crash;
- persists compensation intent before each compensation and completion after it;
- resumes forward execution or compensation from persisted state under a single-owner execution lock;
- leaves failed/ambiguous compensation visible as reconciliation-required rather than marking the Saga successful.

The existing Inventory API already derives reservation identity from `saga_id` and `product_id`, and its PostgreSQL reservation endpoint returns the existing reservation for an identical still-reserved request. That is a useful idempotency primitive. The current PaymentGateway is process-local, so it must become durable and idempotent before payment recovery can be safe. Order confirmation must also be replay-safe.

The next implementation phase will use the existing application PostgreSQL pool and explicit project-owned execution tables/ports, while retaining Sagaz's action/compensation functions as the domain operation definitions where practical. It will not rely on Sagaz 1.5.0's top-level storage writes as the recovery mechanism.
