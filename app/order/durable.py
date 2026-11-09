from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx
from psycopg.types.json import Jsonb

from app.persistence.database import DatabaseManager

logger = logging.getLogger(__name__)

FORWARD_STEPS = (
    ("reserve_inventory", "release_inventory"),
    ("charge_payment", "refund_payment"),
    ("confirm_order", None),
)


class OrderExecutionConflict(ValueError):
    """The order ID is already bound to a different request."""


class OrderExecutionBusy(RuntimeError):
    """Another process currently owns the Saga execution lock."""


class DurableOrderFailure(RuntimeError):
    """A terminal failure loaded from durable execution state."""

    def __init__(self, failure_category: str, detail: str) -> None:
        super().__init__(detail)
        self.failure_category = failure_category


class CompensationIncomplete(RuntimeError):
    """At least one compensation needs explicit reconciliation."""

    def __init__(self, saga_id: str, steps: list[str]) -> None:
        super().__init__("Order compensation is incomplete")
        self.saga_id = saga_id
        self.steps = steps


def _json_default(value: Any) -> str:
    if isinstance(value, (Decimal,)):
        return str(value)
    if hasattr(value, "hex") and hasattr(value, "int"):
        return str(value)
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _json_dump(value: dict[str, Any]) -> Jsonb:
    return Jsonb(value, dumps=lambda item: json.dumps(item, default=_json_default))


def _request_fingerprint(context: dict[str, Any]) -> str:
    canonical = {
        "order_id": int(context["order_id"]),
        "product_id": int(context["product_id"]),
        "quantity": int(context["quantity"]),
        "unit_price": str(context["unit_price"]),
        "amount": str(context["amount"]),
    }
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _execution(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    if result.get("created_at") is not None:
        result["created_at"] = result["created_at"].isoformat()
    if result.get("updated_at") is not None:
        result["updated_at"] = result["updated_at"].isoformat()
    if result.get("completed_at") is not None:
        result["completed_at"] = result["completed_at"].isoformat()
    return result


class PostgresOrderSagaRepository:
    """Durable Order Saga ledger using the application's psycopg pool."""

    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    @asynccontextmanager
    async def execution_lock(self, saga_id: str) -> AsyncIterator[bool]:
        """Hold a session-level advisory lock for the complete Saga attempt.

        A checked-out connection owns the lock. If the process exits, PostgreSQL
        closes its session and releases the lock without a lease-expiry race.
        """
        pool = self.database.pool
        if pool is None:
            raise RuntimeError("PostgreSQL pool is not open")

        async with pool.connection() as connection:
            cursor = await connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS acquired",
                (f"order-saga:{saga_id}",),
            )
            row = await cursor.fetchone()
            await connection.commit()
            acquired = bool(row and row["acquired"])
            try:
                yield acquired
            finally:
                if acquired:
                    await connection.execute(
                        "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                        (f"order-saga:{saga_id}",),
                    )
                    await connection.commit()

    async def start_or_load(self, context: dict[str, Any]) -> dict[str, Any]:
        """Atomically claim an order ID or load the matching prior execution."""
        saga_id = str(context["saga_id"])
        order_id = int(context["order_id"])
        fingerprint = _request_fingerprint(context)

        async with self.database.connection() as connection:
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"order-id:{order_id}",),
            )
            cursor = await connection.execute(
                """
                SELECT *
                FROM order_saga_executions
                WHERE order_id = %s
                FOR UPDATE
                """,
                (order_id,),
            )
            existing = await cursor.fetchone()
            if existing is not None:
                if existing["request_fingerprint"].strip() != fingerprint:
                    raise OrderExecutionConflict(
                        f"Order {order_id} was already created with a different request"
                    )
                return await self._load_with_connection(
                    connection, existing["saga_id"]
                )

            await connection.execute(
                """
                INSERT INTO orders (
                    order_id, requested_product_id, product_id, quantity,
                    unit_price, total_amount, saga_id, reservation_id, payment_id,
                    status, failure_category, correlation_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, NULL, 'pending', NULL, %s)
                ON CONFLICT (order_id) DO NOTHING
                """,
                (
                    order_id,
                    int(context["product_id"]),
                    int(context["product_id"]),
                    int(context["quantity"]),
                    Decimal(str(context["unit_price"])),
                    Decimal(str(context["amount"])),
                    saga_id,
                    context.get("correlation_id"),
                ),
            )
            cursor = await connection.execute(
                "SELECT saga_id FROM orders WHERE order_id = %s FOR UPDATE",
                (order_id,),
            )
            order_row = await cursor.fetchone()
            if order_row is None or order_row["saga_id"] != saga_id:
                raise OrderExecutionConflict(
                    f"Order {order_id} is already owned by another execution"
                )

            cursor = await connection.execute(
                """
                INSERT INTO order_saga_executions (
                    saga_id, order_id, saga_name, request_fingerprint,
                    status, context
                )
                VALUES (%s, %s, 'order-processing', %s, 'running', %s)
                RETURNING *
                """,
                (saga_id, order_id, fingerprint, _json_dump(context)),
            )
            record = await cursor.fetchone()
            for step_name, compensation_name in FORWARD_STEPS:
                await connection.execute(
                    """
                    INSERT INTO order_saga_steps (
                        saga_id, step_name, compensation_status
                    )
                    VALUES (%s, %s, %s)
                    """,
                    (
                        saga_id,
                        step_name,
                        "pending" if compensation_name else "not_required",
                    ),
                )
            return await self._load_with_connection(connection, record["saga_id"])

    async def get_by_order(self, order_id: int) -> dict[str, Any] | None:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT saga_id
                FROM order_saga_executions
                WHERE order_id = %s
                """,
                (order_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return await self._load_with_connection(connection, row["saga_id"])

    async def get(self, saga_id: str) -> dict[str, Any] | None:
        async with self.database.connection() as connection:
            return await self._load_with_connection(connection, saga_id)

    async def _load_with_connection(
        self, connection, saga_id: str
    ) -> dict[str, Any] | None:
        cursor = await connection.execute(
            "SELECT * FROM order_saga_executions WHERE saga_id = %s",
            (saga_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        cursor = await connection.execute(
            """
            SELECT *
            FROM order_saga_steps
            WHERE saga_id = %s
            ORDER BY CASE step_name
                WHEN 'reserve_inventory' THEN 1
                WHEN 'charge_payment' THEN 2
                WHEN 'confirm_order' THEN 3
                ELSE 99
            END
            """,
            (saga_id,),
        )
        steps = await cursor.fetchall()
        result = _execution(row)
        result["steps"] = [dict(step) for step in steps]
        return result

    async def list_recoverable(self) -> list[dict[str, Any]]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT saga_id
                FROM order_saga_executions
                WHERE status IN ('running', 'compensating')
                ORDER BY created_at, saga_id
                """
            )
            saga_ids = [row["saga_id"] for row in await cursor.fetchall()]
        results = []
        for saga_id in saga_ids:
            record = await self.get(saga_id)
            if record is not None:
                results.append(record)
        return results

    async def mark_step_started(
        self, saga_id: str, step_name: str, context: dict[str, Any]
    ) -> None:
        async with self.database.connection() as connection:
            await connection.execute(
                """
                UPDATE order_saga_steps
                SET forward_status = 'running',
                    forward_attempts = forward_attempts + 1,
                    forward_started_at = now(),
                    forward_error = NULL,
                    updated_at = now()
                WHERE saga_id = %s AND step_name = %s
                  AND forward_status <> 'completed'
                """,
                (saga_id, step_name),
            )
            await connection.execute(
                """
                UPDATE order_saga_executions
                SET context = %s, last_error = NULL, updated_at = now()
                WHERE saga_id = %s
                """,
                (_json_dump(context), saga_id),
            )

    async def mark_step_completed(
        self,
        saga_id: str,
        step_name: str,
        result: dict[str, Any] | None,
        context: dict[str, Any],
    ) -> None:
        async with self.database.connection() as connection:
            await connection.execute(
                """
                UPDATE order_saga_steps
                SET forward_status = 'completed',
                    result = %s,
                    forward_error = NULL,
                    forward_completed_at = now(),
                    updated_at = now()
                WHERE saga_id = %s AND step_name = %s
                """,
                (_json_dump(result or {}), saga_id, step_name),
            )
            await connection.execute(
                """
                UPDATE order_saga_executions
                SET context = %s, last_error = NULL, updated_at = now()
                WHERE saga_id = %s
                """,
                (_json_dump(context), saga_id),
            )

    async def mark_step_failed(
        self, saga_id: str, step_name: str, error: Exception
    ) -> None:
        async with self.database.connection() as connection:
            await connection.execute(
                """
                UPDATE order_saga_steps
                SET forward_status = 'failed',
                    forward_error = %s,
                    updated_at = now()
                WHERE saga_id = %s AND step_name = %s
                """,
                (str(error), saga_id, step_name),
            )

    async def set_execution_state(
        self,
        saga_id: str,
        status: str,
        *,
        context: dict[str, Any] | None = None,
        failure_category: str | None = None,
        error: str | None = None,
    ) -> None:
        allowed = {
            "running",
            "compensating",
            "completed",
            "rolled_back",
            "reconciliation_required",
        }
        if status not in allowed:
            raise ValueError(f"Unsupported order Saga state: {status}")

        async with self.database.connection() as connection:
            await connection.execute(
                """
                UPDATE order_saga_executions
                SET status = %s,
                    context = COALESCE(%s, context),
                    failure_category = COALESCE(%s, failure_category),
                    last_error = %s,
                    updated_at = now(),
                    completed_at = CASE
                        WHEN %s IN ('completed', 'rolled_back',
                                    'reconciliation_required') THEN now()
                        ELSE NULL
                    END
                WHERE saga_id = %s
                """,
                (
                    status,
                    _json_dump(context) if context is not None else None,
                    failure_category,
                    error,
                    status,
                    saga_id,
                ),
            )
            order_status = {
                "running": "pending",
                "compensating": "pending",
                "completed": "processed",
                "rolled_back": failure_category or "failed",
                "reconciliation_required": "compensation_incomplete",
            }[status]
            await connection.execute(
                """
                UPDATE orders AS o
                SET status = %s,
                    failure_category = CASE
                        WHEN %s IN ('completed', 'running', 'compensating')
                            THEN NULL
                        ELSE COALESCE(%s, o.failure_category)
                    END,
                    reservation_id = COALESCE(
                        NULLIF(e.context->>'reservation_id', '')::uuid,
                        o.reservation_id
                    ),
                    payment_id = COALESCE(
                        NULLIF(e.context->>'payment_id', ''),
                        o.payment_id
                    ),
                    updated_at = now()
                FROM order_saga_executions AS e
                WHERE e.saga_id = %s AND o.order_id = e.order_id
                """,
                (
                    order_status,
                    status,
                    failure_category,
                    saga_id,
                ),
            )

    async def mark_compensation_started(
        self, saga_id: str, step_name: str
    ) -> None:
        async with self.database.connection() as connection:
            await connection.execute(
                """
                UPDATE order_saga_steps
                SET compensation_status = 'running',
                    compensation_attempts = compensation_attempts + 1,
                    compensation_started_at = now(),
                    compensation_error = NULL,
                    updated_at = now()
                WHERE saga_id = %s AND step_name = %s
                  AND compensation_status <> 'completed'
                """,
                (saga_id, step_name),
            )

    async def mark_compensation_completed(
        self, saga_id: str, step_name: str
    ) -> None:
        async with self.database.connection() as connection:
            await connection.execute(
                """
                UPDATE order_saga_steps
                SET compensation_status = 'completed',
                    compensation_error = NULL,
                    compensation_completed_at = now(),
                    updated_at = now()
                WHERE saga_id = %s AND step_name = %s
                """,
                (saga_id, step_name),
            )

    async def mark_compensation_failed(
        self, saga_id: str, step_name: str, error: Exception
    ) -> None:
        async with self.database.connection() as connection:
            await connection.execute(
                """
                UPDATE order_saga_steps
                SET compensation_status = 'failed',
                    compensation_error = %s,
                    updated_at = now()
                WHERE saga_id = %s AND step_name = %s
                """,
                (str(error), saga_id, step_name),
            )

    async def note_recovery_attempt(self, saga_id: str) -> None:
        async with self.database.connection() as connection:
            await connection.execute(
                """
                UPDATE order_saga_executions
                SET recovery_attempts = recovery_attempts + 1,
                    updated_at = now()
                WHERE saga_id = %s
                """,
                (saga_id,),
            )

    async def get_recovery_view(self, order_id: int) -> dict[str, Any] | None:
        record = await self.get_by_order(order_id)
        if record is None:
            return None
        return {
            "saga_id": record["saga_id"],
            "order_id": record["order_id"],
            "status": record["status"],
            "failure_category": record["failure_category"],
            "last_error": record["last_error"],
            "recovery_attempts": record["recovery_attempts"],
            "steps": [
                {
                    "step_name": step["step_name"],
                    "forward_status": step["forward_status"],
                    "compensation_status": step["compensation_status"],
                    "forward_attempts": step["forward_attempts"],
                    "compensation_attempts": step["compensation_attempts"],
                    "forward_error": step["forward_error"],
                    "compensation_error": step["compensation_error"],
                }
                for step in record["steps"]
            ],
        }


class PostgresPaymentGateway:
    """Durable, idempotent demo payment adapter backed by PostgreSQL."""

    def __init__(
        self,
        database: DatabaseManager,
        *,
        decline_above: Decimal = Decimal("1000"),
    ) -> None:
        self.database = database
        self.decline_above = Decimal(str(decline_above))

    async def charge(self, context: dict[str, Any]) -> str:
        from app.order.workflow import PaymentDeclined

        saga_id = str(context["saga_id"])
        order_id = int(context["order_id"])
        amount = Decimal(str(context["amount"]))
        payment_id = str(uuid5(NAMESPACE_URL, f"payment:{saga_id}"))
        fingerprint_payload = f"{order_id}:{amount:.2f}"
        fingerprint = hashlib.sha256(fingerprint_payload.encode()).hexdigest()
        should_decline = amount > self.decline_above

        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT *
                FROM order_payments
                WHERE payment_id = %s
                FOR UPDATE
                """,
                (payment_id,),
            )
            existing = await cursor.fetchone()
            if existing is None:
                status = "declined" if should_decline else "charged"
                await connection.execute(
                    """
                    INSERT INTO order_payments (
                        payment_id, order_id, saga_id, amount,
                        request_fingerprint, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        payment_id,
                        order_id,
                        saga_id,
                        amount,
                        fingerprint,
                        status,
                    ),
                )
            else:
                if (
                    existing["order_id"] != order_id
                    or existing["request_fingerprint"].strip() != fingerprint
                ):
                    raise OrderExecutionConflict(
                        "Payment idempotency key was reused with a different request"
                    )
                should_decline = existing["status"] == "declined"
                if existing["status"] == "refunded":
                    should_decline = True

        if should_decline:
            raise PaymentDeclined("Payment was declined by the demo gateway")
        return payment_id

    async def refund(self, payment_id: str) -> None:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE order_payments
                SET status = 'refunded', updated_at = now()
                WHERE payment_id = %s AND status = 'charged'
                RETURNING payment_id
                """,
                (payment_id,),
            )
            updated = await cursor.fetchone()
            if updated is None:
                cursor = await connection.execute(
                    "SELECT status FROM order_payments WHERE payment_id = %s",
                    (payment_id,),
                )
                existing = await cursor.fetchone()
                if existing is None:
                    raise KeyError(f"Payment {payment_id} does not exist")
                if existing["status"] not in {"refunded", "declined"}:
                    raise RuntimeError(
                        f"Cannot refund payment in state {existing['status']}"
                    )

    async def status(self, payment_id: str) -> str | None:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                "SELECT status FROM order_payments WHERE payment_id = %s",
                (payment_id,),
            )
            row = await cursor.fetchone()
            return row["status"] if row is not None else None


class DurableOrderExecutor:
    """Run and recover the three-step Order Saga with write-ahead checkpoints."""

    def __init__(
        self,
        repository: PostgresOrderSagaRepository,
        payment_gateway: PostgresPaymentGateway,
    ) -> None:
        self.repository = repository
        self.payment_gateway = payment_gateway

    async def start_or_load(self, context: dict[str, Any]) -> dict[str, Any]:
        return await self.repository.start_or_load(context)

    async def get_by_order(self, order_id: int) -> dict[str, Any] | None:
        return await self.repository.get_by_order(order_id)

    async def recovery_view(self, order_id: int) -> dict[str, Any] | None:
        return await self.repository.get_recovery_view(order_id)

    async def execute(
        self,
        saga,
        tracker,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        saga_id = record["saga_id"]
        async with self.repository.execution_lock(saga_id) as acquired:
            if not acquired:
                raise OrderExecutionBusy(
                    f"Order Saga {saga_id} is already executing in another process"
                )

            current = await self.repository.get(saga_id)
            if current is None:
                raise KeyError(f"Order Saga {saga_id} does not exist")
            if current["status"] == "completed":
                return dict(current["context"])
            if current["status"] == "rolled_back":
                raise DurableOrderFailure(
                    current.get("failure_category") or "failed",
                    current.get("last_error") or "Order Saga was rolled back",
                )
            if current["status"] == "reconciliation_required":
                steps = [
                    step["step_name"]
                    for step in current["steps"]
                    if step["compensation_status"] == "failed"
                ]
                raise CompensationIncomplete(saga_id, steps)

            await self.repository.note_recovery_attempt(saga_id)
            context = dict(current["context"])

            if current["status"] == "compensating":
                await self._resume_compensations(saga, tracker, current, context)
                raise DurableOrderFailure(
                    current.get("failure_category") or "processing_error",
                    current.get("last_error") or "Order Saga was rolled back",
                )

            for step_name, _compensation_name in FORWARD_STEPS:
                step_state = self._find_step(current, step_name)
                if step_state["forward_status"] == "completed":
                    result = step_state.get("result") or {}
                    context.update(result)
                    continue
                if step_state["forward_status"] == "failed":
                    await self.repository.set_execution_state(
                        saga_id,
                        "compensating",
                        context=context,
                    )
                    await self._resume_compensations(saga, tracker, current, context)
                    raise DurableOrderFailure(
                        current.get("failure_category") or "processing_error",
                        current.get("last_error") or "Order Saga was rolled back",
                    )

                action = getattr(saga, step_name)
                await self.repository.mark_step_started(saga_id, step_name, context)
                try:
                    result = await action(context)
                except Exception as exc:
                    if self._is_definitive_failure(exc):
                        failure_category = self._failure_category(exc)
                        await self.repository.mark_step_failed(
                            saga_id, step_name, exc
                        )
                        await self.repository.set_execution_state(
                            saga_id,
                            "compensating",
                            context=context,
                            failure_category=failure_category,
                            error=str(exc),
                        )
                        refreshed = await self.repository.get(saga_id)
                        await self._resume_compensations(
                            saga, tracker, refreshed or current, context
                        )
                    else:
                        await self.repository.set_execution_state(
                            saga_id,
                            "running",
                            context=context,
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    raise

                result_data = result if isinstance(result, dict) else {}
                context.update(result_data)
                await self.repository.mark_step_completed(
                    saga_id,
                    step_name,
                    result_data,
                    context,
                )
                current = await self.repository.get(saga_id) or current

            await self.repository.set_execution_state(
                saga_id,
                "completed",
                context=context,
            )
            return context

    async def _resume_compensations(
        self,
        saga,
        tracker,
        record: dict[str, Any],
        context: dict[str, Any],
    ) -> None:
        saga_id = record["saga_id"]
        for step_name, compensation_name in reversed(FORWARD_STEPS):
            if compensation_name is None:
                continue
            step_state = self._find_step(record, step_name)
            if step_state["forward_status"] != "completed":
                continue
            if step_state["compensation_status"] == "completed":
                continue

            compensation = getattr(saga, compensation_name)
            await self.repository.mark_compensation_started(
                saga_id, step_name
            )
            if tracker is not None:
                tracker.record_compensation_start(step_name, context)
            try:
                await compensation(context)
            except Exception as exc:
                await self.repository.mark_compensation_failed(
                    saga_id, step_name, exc
                )
                await self.repository.set_execution_state(
                    saga_id,
                    "reconciliation_required",
                    context=context,
                    error=f"{type(exc).__name__}: {exc}",
                )
                if tracker is not None:
                    tracker.record_compensation_failure(step_name, context)
                refreshed = await self.repository.get(saga_id)
                failed_steps = [
                    step["step_name"]
                    for step in (refreshed or record)["steps"]
                    if step["compensation_status"] == "failed"
                ]
                raise CompensationIncomplete(saga_id, failed_steps) from exc

            await self.repository.mark_compensation_completed(
                saga_id, step_name
            )
            if tracker is not None:
                tracker.record_compensation_complete(step_name, context)
            record = await self.repository.get(saga_id) or record

        failure_category = record.get("failure_category") or "processing_error"
        await self.repository.set_execution_state(
            saga_id,
            "rolled_back",
            context=context,
            failure_category=failure_category,
            error=record.get("last_error"),
        )

    async def recover_pending(self, saga_factory) -> dict[str, int]:
        records = await self.repository.list_recoverable()
        recovered = 0
        failed = 0
        for record in records:
            saga, tracker = saga_factory(self.payment_gateway)
            try:
                await self.execute(saga, tracker, record)
                recovered += 1
            except DurableOrderFailure:
                recovered += 1
            except Exception:
                failed += 1
                logger.exception(
                    "order.saga.recovery_failed",
                    extra={"saga_id": record["saga_id"], "order_id": record["order_id"]},
                )
        return {"found": len(records), "recovered": recovered, "failed": failed}

    @staticmethod
    def _find_step(record: dict[str, Any], step_name: str) -> dict[str, Any]:
        for step in record["steps"]:
            if step["step_name"] == step_name:
                return step
        raise KeyError(f"Step {step_name} is missing from Saga {record['saga_id']}")

    @staticmethod
    def _is_definitive_failure(exc: Exception) -> bool:
        from app.order.workflow import PaymentDeclined

        if isinstance(exc, PaymentDeclined):
            return True
        return (
            isinstance(exc, httpx.HTTPStatusError)
            and 400 <= exc.response.status_code < 500
        )

    @staticmethod
    def _failure_category(exc: Exception) -> str:
        from app.order.main import classify_order_failure

        return classify_order_failure(exc)
