from __future__ import annotations

import os
from decimal import Decimal
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx
import psycopg
import pytest
from psycopg import sql

from app.order.durable import (
    DurableOrderExecutor,
    DurableOrderFailure,
    PostgresOrderSagaRepository,
    PostgresPaymentGateway,
)
from app.order.workflow import (
    PaymentDeclined,
    configure_order_repository,
    persist_order,
)
from app.order.repository import PostgresOrderRepository
from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager
from app.persistence.migrations import MIGRATIONS_DIR, apply_migrations


@pytest.fixture
def isolated_postgres_schema():
    database_url = os.getenv("POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("POSTGRES_TEST_URL is required for Order Saga recovery tests")

    schema = f"test_order_saga_{uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
        )

    parsed_url = urlsplit(database_url)
    query = parse_qsl(parsed_url.query, keep_blank_values=True)
    query.append(("options", f"-c search_path={schema}"))
    isolated_url = urlunsplit(
        parsed_url._replace(query=urlencode(query, quote_via=quote))
    )
    try:
        with psycopg.connect(isolated_url) as connection:
            apply_migrations(connection, MIGRATIONS_DIR)
            cursor = connection.execute(
                """
                INSERT INTO products (sku, name, category, unit_price)
                VALUES ('RECOVERY-001', 'Recovery test product', 'test', 12.50)
                RETURNING product_id
                """
            )
            product_id = cursor.fetchone()[0]
            connection.execute(
                """
                INSERT INTO inventory_balances (product_id, on_hand, reserved)
                VALUES (%s, 10, 0)
                """,
                (product_id,),
            )
        yield isolated_url, product_id
    finally:
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


async def _open_database(database_url: str) -> DatabaseManager:
    database = DatabaseManager(pool_name="order-saga-recovery-test")
    await database.open(
        DatabaseSettings(
            backend="postgres",
            database_url=database_url,
            connect_timeout_seconds=5,
            pool_min_size=1,
            pool_max_size=4,
        )
    )
    return database


def _context(order_id: int, product_id: int) -> dict:
    return {
        "order_id": order_id,
        "product_id": product_id,
        "quantity": 2,
        "unit_price": Decimal("12.50"),
        "amount": Decimal("25.00"),
        "correlation_id": str(uuid4()),
        "saga_id": str(uuid4()),
    }


class PaymentResponseLostSaga:
    """Simulates a payment commit followed by a lost process response."""

    def __init__(self, gateway, *, lose_response: bool) -> None:
        self.gateway = gateway
        self.lose_response = lose_response

    async def reserve_inventory(self, context: dict) -> dict:
        return {}

    async def charge_payment(self, context: dict) -> dict:
        payment_id = await self.gateway.charge(context)
        if self.lose_response:
            self.lose_response = False
            raise RuntimeError("simulated process loss after payment commit")
        return {"payment_id": payment_id}

    async def confirm_order(self, context: dict) -> dict:
        return {"order_status": "processed"}

    async def release_inventory(self, context: dict) -> None:
        return None

    async def refund_payment(self, context: dict) -> None:
        await self.gateway.refund(context["payment_id"])


class CompensationResponseLostSaga:
    """Uses an idempotent remote-effect stand-in for inventory release."""

    def __init__(self, *, released_effects: set[str], fail_payment: bool) -> None:
        self.released_effects = released_effects
        self.fail_payment = fail_payment
        self.release_attempts = 0

    async def reserve_inventory(self, context: dict) -> dict:
        return {}

    async def charge_payment(self, context: dict) -> dict:
        if self.fail_payment:
            raise PaymentDeclined("simulated deterministic decline")
        return {"payment_id": "payment-test"}

    async def confirm_order(self, context: dict) -> dict:
        return {"order_status": "processed"}

    async def release_inventory(self, context: dict) -> None:
        self.release_attempts += 1
        self.released_effects.add(context["saga_id"])

    async def refund_payment(self, context: dict) -> None:
        return None


@pytest.mark.asyncio
async def test_payment_commit_before_checkpoint_is_replayed_idempotently(
    isolated_postgres_schema,
):
    database_url, product_id = isolated_postgres_schema
    order_id = 910001
    context = _context(order_id, product_id)

    database = await _open_database(database_url)
    repository = PostgresOrderSagaRepository(database)
    gateway = PostgresPaymentGateway(database)
    record = await repository.start_or_load(context)

    with pytest.raises(RuntimeError, match="after payment commit"):
        await DurableOrderExecutor(repository, gateway).execute(
            PaymentResponseLostSaga(gateway, lose_response=True),
            tracker=None,
            record=record,
        )
    await database.close()

    # A new pool/repository/executor simulates an Order API process restart.
    restarted_database = await _open_database(database_url)
    try:
        restarted_repository = PostgresOrderSagaRepository(restarted_database)
        restarted_gateway = PostgresPaymentGateway(restarted_database)
        executor = DurableOrderExecutor(
            restarted_repository, restarted_gateway
        )
        pending = await restarted_repository.get_by_order(order_id)
        assert pending is not None
        assert pending["status"] == "running"
        payment_step = next(
            step for step in pending["steps"]
            if step["step_name"] == "charge_payment"
        )
        assert payment_step["forward_status"] == "running"

        result = await executor.execute(
            PaymentResponseLostSaga(
                restarted_gateway,
                lose_response=False,
            ),
            tracker=None,
            record=pending,
        )

        assert result["order_status"] == "processed"
        assert result["payment_id"]
        assert await restarted_gateway.status(result["payment_id"]) == "charged"

        async with restarted_database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT count(*) AS count
                FROM order_payments
                WHERE saga_id = %s
                """,
                (context["saga_id"],),
            )
            assert (await cursor.fetchone())["count"] == 1

        completed = await restarted_repository.get(context["saga_id"])
        assert completed is not None
        assert completed["status"] == "completed"
        assert all(
            step["forward_status"] == "completed"
            for step in completed["steps"]
        )
    finally:
        await restarted_database.close()


@pytest.mark.asyncio
async def test_compensation_after_remote_commit_is_retried_after_restart(
    isolated_postgres_schema,
    monkeypatch,
):
    database_url, product_id = isolated_postgres_schema
    order_id = 910002
    context = _context(order_id, product_id)
    released_effects: set[str] = set()

    database = await _open_database(database_url)
    repository = PostgresOrderSagaRepository(database)
    gateway = PostgresPaymentGateway(database)
    record = await repository.start_or_load(context)
    saga = CompensationResponseLostSaga(
        released_effects=released_effects,
        fail_payment=True,
    )

    async def crash_before_compensation_checkpoint(*args, **kwargs) -> None:
        raise RuntimeError("simulated process loss after remote release")

    monkeypatch.setattr(
        repository,
        "mark_compensation_completed",
        crash_before_compensation_checkpoint,
    )
    with pytest.raises(RuntimeError, match="after remote release"):
        await DurableOrderExecutor(repository, gateway).execute(
            saga, tracker=None, record=record
        )
    assert released_effects == {context["saga_id"]}
    await database.close()

    restarted_database = await _open_database(database_url)
    try:
        restarted_repository = PostgresOrderSagaRepository(restarted_database)
        restarted_gateway = PostgresPaymentGateway(restarted_database)
        pending = await restarted_repository.get_by_order(order_id)
        assert pending is not None
        assert pending["status"] == "compensating"
        reserve_step = next(
            step for step in pending["steps"]
            if step["step_name"] == "reserve_inventory"
        )
        assert reserve_step["compensation_status"] == "running"

        restarted_saga = CompensationResponseLostSaga(
            released_effects=released_effects,
            fail_payment=False,
        )
        with pytest.raises(DurableOrderFailure):
            await DurableOrderExecutor(
                restarted_repository, restarted_gateway
            ).execute(restarted_saga, tracker=None, record=pending)

        assert restarted_saga.release_attempts == 1
        assert released_effects == {context["saga_id"]}
        recovered = await restarted_repository.get(context["saga_id"])
        assert recovered is not None
        assert recovered["status"] == "rolled_back"
        reserve_step = next(
            step for step in recovered["steps"]
            if step["step_name"] == "reserve_inventory"
        )
        assert reserve_step["compensation_status"] == "completed"
    finally:
        await restarted_database.close()


class InventoryResponseLostSaga:
    """Models Inventory committing a reservation before its HTTP response is lost."""

    def __init__(
        self,
        *,
        external_effects: set[str],
        lose_response: bool,
    ) -> None:
        self.external_effects = external_effects
        self.lose_response = lose_response
        self.reserve_attempts = 0

    async def reserve_inventory(self, context: dict) -> dict:
        self.reserve_attempts += 1
        effect_key = f"{context['saga_id']}:{context['product_id']}"
        self.external_effects.add(effect_key)
        if self.lose_response:
            self.lose_response = False
            raise RuntimeError("simulated process loss after Inventory commit")
        return {"inventory_effect_key": effect_key}

    async def charge_payment(self, context: dict) -> dict:
        return {"payment_id": "payment-test"}

    async def confirm_order(self, context: dict) -> dict:
        return {"order_status": "processed"}

    async def release_inventory(self, context: dict) -> None:
        return None

    async def refund_payment(self, context: dict) -> None:
        return None


@pytest.mark.asyncio
async def test_execution_created_before_first_step_is_run_after_restart(
    isolated_postgres_schema,
):
    database_url, product_id = isolated_postgres_schema
    order_id = 910003
    context = _context(order_id, product_id)

    database = await _open_database(database_url)
    record = await PostgresOrderSagaRepository(database).start_or_load(context)
    assert record["status"] == "running"
    assert all(
        step["forward_status"] == "pending"
        for step in record["steps"]
    )
    await database.close()

    restarted_database = await _open_database(database_url)
    try:
        repository = PostgresOrderSagaRepository(restarted_database)
        gateway = PostgresPaymentGateway(restarted_database)
        effects: set[str] = set()
        saga = InventoryResponseLostSaga(
            external_effects=effects,
            lose_response=False,
        )
        result = await DurableOrderExecutor(repository, gateway).execute(
            saga,
            tracker=None,
            record=await repository.get_by_order(order_id),
        )

        assert result["order_status"] == "processed"
        assert saga.reserve_attempts == 1
        assert effects == {f"{context['saga_id']}:{product_id}"}
    finally:
        await restarted_database.close()


@pytest.mark.asyncio
async def test_inventory_commit_before_checkpoint_is_replayed_with_same_key(
    isolated_postgres_schema,
):
    database_url, product_id = isolated_postgres_schema
    order_id = 910004
    context = _context(order_id, product_id)
    effects: set[str] = set()

    database = await _open_database(database_url)
    repository = PostgresOrderSagaRepository(database)
    gateway = PostgresPaymentGateway(database)
    record = await repository.start_or_load(context)

    with pytest.raises(RuntimeError, match="after Inventory commit"):
        await DurableOrderExecutor(repository, gateway).execute(
            InventoryResponseLostSaga(
                external_effects=effects,
                lose_response=True,
            ),
            tracker=None,
            record=record,
        )
    await database.close()

    restarted_database = await _open_database(database_url)
    try:
        restarted_repository = PostgresOrderSagaRepository(restarted_database)
        restarted_gateway = PostgresPaymentGateway(restarted_database)
        restarted_saga = InventoryResponseLostSaga(
            external_effects=effects,
            lose_response=False,
        )
        result = await DurableOrderExecutor(
            restarted_repository, restarted_gateway
        ).execute(
            restarted_saga,
            tracker=None,
            record=await restarted_repository.get_by_order(order_id),
        )

        assert result["order_status"] == "processed"
        assert restarted_saga.reserve_attempts == 1
        assert effects == {f"{context['saga_id']}:{product_id}"}
        recovered = await restarted_repository.get(context["saga_id"])
        assert recovered is not None
        assert recovered["status"] == "completed"
        reserve_step = next(
            step for step in recovered["steps"]
            if step["step_name"] == "reserve_inventory"
        )
        assert reserve_step["forward_attempts"] == 2
        assert reserve_step["forward_status"] == "completed"
    finally:
        await restarted_database.close()

class OrderConfirmationResponseLostSaga:
    """Simulates an order upsert commit followed by a lost step response."""

    def __init__(self, *, lose_response: bool) -> None:
        self.lose_response = lose_response

    async def reserve_inventory(self, context: dict) -> dict:
        return {}

    async def charge_payment(self, context: dict) -> dict:
        return {"payment_id": "payment-confirmation-test"}

    async def confirm_order(self, context: dict) -> dict:
        result = await persist_order(context)
        if self.lose_response:
            self.lose_response = False
            raise RuntimeError("simulated process loss after order commit")
        return result

    async def release_inventory(self, context: dict) -> None:
        return None

    async def refund_payment(self, context: dict) -> None:
        return None


@pytest.mark.asyncio
async def test_order_confirmation_commit_before_checkpoint_is_idempotent(
    isolated_postgres_schema,
):
    database_url, product_id = isolated_postgres_schema
    order_id = 910005
    context = _context(order_id, product_id)

    database = await _open_database(database_url)
    configure_order_repository(PostgresOrderRepository(database))
    repository = PostgresOrderSagaRepository(database)
    gateway = PostgresPaymentGateway(database)
    record = await repository.start_or_load(context)

    try:
        with pytest.raises(RuntimeError, match="after order commit"):
            await DurableOrderExecutor(repository, gateway).execute(
                OrderConfirmationResponseLostSaga(lose_response=True),
                tracker=None,
                record=record,
            )
    finally:
        await database.close()

    restarted_database = await _open_database(database_url)
    configure_order_repository(PostgresOrderRepository(restarted_database))
    try:
        restarted_repository = PostgresOrderSagaRepository(
            restarted_database
        )
        restarted_gateway = PostgresPaymentGateway(restarted_database)
        pending = await restarted_repository.get_by_order(order_id)
        assert pending is not None
        confirm_step = next(
            step for step in pending["steps"]
            if step["step_name"] == "confirm_order"
        )
        assert confirm_step["forward_status"] == "running"

        result = await DurableOrderExecutor(
            restarted_repository, restarted_gateway
        ).execute(
            OrderConfirmationResponseLostSaga(lose_response=False),
            tracker=None,
            record=pending,
        )
        assert result["order_status"] == "processed"

        async with restarted_database.connection() as connection:
            cursor = await connection.execute(
                "SELECT count(*) AS count, max(status) AS status "
                "FROM orders WHERE order_id = %s",
                (order_id,),
            )
            order = await cursor.fetchone()
            assert order["count"] == 1
            assert order["status"] == "processed"

        completed = await restarted_repository.get(context["saga_id"])
        assert completed is not None
        assert completed["status"] == "completed"
        confirm_step = next(
            step for step in completed["steps"]
            if step["step_name"] == "confirm_order"
        )
        assert confirm_step["forward_attempts"] == 2
        assert confirm_step["forward_status"] == "completed"
    finally:
        configure_order_repository(None)
        await restarted_database.close()

class ConfirmFailureAfterChargeSaga:
    """Exercises refund-before-release compensation and a lost refund checkpoint."""

    def __init__(
        self,
        gateway: PostgresPaymentGateway,
        *,
        compensation_order: list[str],
        released_effects: set[str],
    ) -> None:
        self.gateway = gateway
        self.compensation_order = compensation_order
        self.released_effects = released_effects

    async def reserve_inventory(self, context: dict) -> dict:
        return {}

    async def charge_payment(self, context: dict) -> dict:
        payment_id = await self.gateway.charge(context)
        return {"payment_id": payment_id}

    async def confirm_order(self, context: dict) -> dict:
        request = httpx.Request("POST", "http://order.test/confirm")
        response = httpx.Response(409, request=request)
        raise httpx.HTTPStatusError(
            "simulated confirmation rejection",
            request=request,
            response=response,
        )

    async def refund_payment(self, context: dict) -> None:
        self.compensation_order.append("refund")
        await self.gateway.refund(context["payment_id"])

    async def release_inventory(self, context: dict) -> None:
        self.compensation_order.append("release")
        self.released_effects.add(context["saga_id"])


@pytest.mark.asyncio
async def test_restart_retries_refund_before_inventory_release(
    isolated_postgres_schema,
    monkeypatch,
):
    database_url, product_id = isolated_postgres_schema
    order_id = 910006
    context = _context(order_id, product_id)
    compensation_order: list[str] = []
    released_effects: set[str] = set()

    database = await _open_database(database_url)
    repository = PostgresOrderSagaRepository(database)
    gateway = PostgresPaymentGateway(database)
    record = await repository.start_or_load(context)
    saga = ConfirmFailureAfterChargeSaga(
        gateway,
        compensation_order=compensation_order,
        released_effects=released_effects,
    )
    original_mark_complete = repository.mark_compensation_completed

    async def crash_after_refund_commit(
        saga_id: str, step_name: str
    ) -> None:
        if step_name == "charge_payment":
            raise RuntimeError("simulated process loss after refund commit")
        await original_mark_complete(saga_id, step_name)

    monkeypatch.setattr(
        repository,
        "mark_compensation_completed",
        crash_after_refund_commit,
    )
    with pytest.raises(RuntimeError, match="after refund commit"):
        await DurableOrderExecutor(repository, gateway).execute(
            saga, tracker=None, record=record
        )
    assert compensation_order == ["refund"]
    assert released_effects == set()
    payment_id = str(uuid5(NAMESPACE_URL, f"payment:{context['saga_id']}"))
    assert await gateway.status(payment_id) == "refunded"
    await database.close()

    restarted_database = await _open_database(database_url)
    try:
        restarted_repository = PostgresOrderSagaRepository(
            restarted_database
        )
        restarted_gateway = PostgresPaymentGateway(restarted_database)
        pending = await restarted_repository.get_by_order(order_id)
        assert pending is not None
        assert pending["status"] == "compensating"

        restarted_saga = ConfirmFailureAfterChargeSaga(
            restarted_gateway,
            compensation_order=compensation_order,
            released_effects=released_effects,
        )
        with pytest.raises(DurableOrderFailure):
            await DurableOrderExecutor(
                restarted_repository, restarted_gateway
            ).execute(restarted_saga, tracker=None, record=pending)

        assert compensation_order == ["refund", "refund", "release"]
        assert released_effects == {context["saga_id"]}
        assert await restarted_gateway.status(payment_id) == "refunded"

        recovered = await restarted_repository.get(context["saga_id"])
        assert recovered is not None
        assert recovered["status"] == "rolled_back"
        compensation_states = {
            step["step_name"]: step["compensation_status"]
            for step in recovered["steps"]
        }
        assert compensation_states["charge_payment"] == "completed"
        assert compensation_states["reserve_inventory"] == "completed"
    finally:
        await restarted_database.close()

