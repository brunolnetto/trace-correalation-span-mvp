from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
from decimal import Decimal
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from app.inventory.postgres_store import PostgresInventoryStore
from app.inventory.store import (
    InventoryAdjustmentRequest,
    InventoryReceiptRequest,
    InventoryReservationRequest,
    ProductCreate,
    ProductUpdate,
)
from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager
from app.persistence.migrations import MIGRATIONS_DIR, apply_migrations


@pytest.fixture
def postgres_schema_url():
    database_url = os.getenv("POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("POSTGRES_TEST_URL is required for PostgreSQL integration tests")

    schema = f"test_inventory_{uuid4().hex}"
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
        with psycopg.connect(isolated_url, row_factory=dict_row) as connection:
            apply_migrations(connection, MIGRATIONS_DIR)
        yield isolated_url
    finally:
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


@pytest.fixture
async def postgres_store(postgres_schema_url: str):
    settings = DatabaseSettings(
        backend="postgres",
        database_url=postgres_schema_url,
        connect_timeout_seconds=5,
        pool_min_size=1,
        pool_max_size=4,
    )
    manager = DatabaseManager()
    await manager.open(settings)
    try:
        yield PostgresInventoryStore(manager), manager, settings
    finally:
        await manager.close()


def product_request(sku: str = "SKU-POSTGRES-001") -> ProductCreate:
    return ProductCreate(
        sku=sku,
        name="Postgres product",
        category="integration",
        unit_price=Decimal("17.25"),
    )


@pytest.mark.asyncio
async def test_inventory_catalog_and_transactional_movements_survive_pool_restart(
    postgres_store,
) -> None:
    store, manager, settings = postgres_store
    product = await store.create_product(product_request())
    product_id = product["product_id"]

    assert (await store.get_product(product_id))["sku"] == "SKU-POSTGRES-001"
    assert len(await store.list_products()) == 1
    assert (await store.inventory_snapshot(product_id))["on_hand"] == 0

    updated = await store.update_product(
        product_id,
        ProductUpdate(category="updated-category", unit_price=Decimal("18.00")),
    )
    assert updated["category"] == "updated-category"
    assert updated["unit_price"] == Decimal("18.00")

    receipt_request = InventoryReceiptRequest(
        quantity=10,
        idempotency_key="integration-receipt-001",
        reference="PO-INTEGRATION-001",
    )
    receipt = await store.receive_stock(product_id, receipt_request)
    assert await store.receive_stock(product_id, receipt_request) == receipt
    with pytest.raises(ValueError, match="different request"):
        await store.receive_stock(
            product_id,
            InventoryReceiptRequest(
                quantity=11,
                idempotency_key="integration-receipt-001",
                reference="PO-INTEGRATION-001",
            ),
        )

    adjustment_request = InventoryAdjustmentRequest(
        quantity_delta=2,
        reason="integration cycle count",
        idempotency_key="integration-adjustment-001",
    )
    movement = await store.adjust_stock(product_id, adjustment_request)
    assert await store.adjust_stock(product_id, adjustment_request) == movement

    first_reservation = await store.reserve(
        product_id,
        InventoryReservationRequest(
            order_id=101,
            saga_id="integration-saga-release",
            quantity=4,
        ),
    )
    released = await store.release(first_reservation["reservation_id"])
    assert released["status"] == "released"

    second_reservation = await store.reserve(
        product_id,
        InventoryReservationRequest(
            order_id=102,
            saga_id="integration-saga-commit",
            quantity=3,
        ),
    )
    committed = await store.commit(second_reservation["reservation_id"])
    assert committed["status"] == "committed"

    snapshot = await store.inventory_snapshot(product_id)
    assert (snapshot["on_hand"], snapshot["reserved"]) == (9, 0)
    reconciliation = await store.reconcile(product_id)
    assert reconciliation == {
        "on_hand": 9,
        "reserved": 0,
        "journal_on_hand": 9,
        "journal_reserved": 0,
    }
    assert len(await store.list_movements(product_id, limit=100)) == 6

    await manager.close()
    restarted_manager = DatabaseManager()
    await restarted_manager.open(settings)
    try:
        restarted_store = PostgresInventoryStore(restarted_manager)
        assert (await restarted_store.get_product(product_id))["sku"] == (
            "SKU-POSTGRES-001"
        )
        assert (await restarted_store.inventory_snapshot(product_id)) == snapshot
        assert await restarted_store.receive_stock(product_id, receipt_request) == receipt
        assert await restarted_store.adjust_stock(product_id, adjustment_request) == movement
        assert (await restarted_store.inventory_snapshot(product_id)) == snapshot
        assert len(await restarted_store.list_movements(product_id, limit=100)) == 6
        assert await restarted_store.reconcile(product_id) == reconciliation
        assert await restarted_manager.healthy()
    finally:
        await restarted_manager.close()


@pytest.mark.asyncio
async def test_postgres_movement_journal_rejects_updates_and_deletes(
    postgres_store,
) -> None:
    store, manager, _settings = postgres_store
    product = await store.create_product(product_request("SKU-APPEND-ONLY"))
    product_id = product["product_id"]
    movement = await store.receive_stock(
        product_id,
        InventoryReceiptRequest(
            quantity=3,
            idempotency_key="append-only-receipt",
            reference="append-only-test",
        ),
    )

    statements = (
        (
            """
            UPDATE inventory_movements
            SET reference = 'rewritten'
            WHERE movement_id = %s
            """,
            (movement["receipt_id"],),
        ),
        (
            "DELETE FROM inventory_movements WHERE movement_id = %s",
            (movement["receipt_id"],),
        ),
        ("TRUNCATE inventory_movements", ()),
    )
    for statement, parameters in statements:
        with pytest.raises(psycopg.Error, match="append-only"):
            async with manager.connection() as connection:
                await connection.execute(statement, parameters)

    movements = await store.list_movements(product_id)
    assert len(movements) == 1
    assert movements[0]["reference"] == "append-only-test"


@pytest.mark.asyncio
async def test_inventory_api_uses_postgres_repository_with_existing_http_contract(
    postgres_schema_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from importlib import import_module

    import httpx

    inventory_module = import_module("app.inventory.main")
    original_store = inventory_module.inventory_store
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("PERSISTENCE_BACKEND", "postgres")
    monkeypatch.setenv("DATABASE_URL", postgres_schema_url)
    monkeypatch.setenv("DATABASE_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("DATABASE_POOL_MAX_SIZE", "3")
    monkeypatch.setattr(inventory_module, "seed_demo_data", False)

    try:
        async with inventory_module.lifespan(inventory_module.app):
            assert isinstance(
                inventory_module.inventory_store,
                PostgresInventoryStore,
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=inventory_module.app),
                base_url="http://inventory.test",
            ) as client:
                health = await client.get("/healthz")
                assert health.status_code == 200
                assert health.json() == {
                    "status": "ok",
                    "service": "inventory-api",
                }

                created = await client.post(
                    "/products",
                    json={
                        "sku": "API-POSTGRES-001",
                        "name": "API PostgreSQL product",
                        "category": "integration",
                        "unit_price": "21.50",
                    },
                )
                assert created.status_code == 201
                product = created.json()
                assert {
                    "product_id",
                    "sku",
                    "name",
                    "category",
                    "unit_price",
                    "is_active",
                    "created_at",
                    "updated_at",
                }.issubset(product)

                product_id = product["product_id"]
                receipt = await client.post(
                    f"/inventory/{product_id}/receipts",
                    json={
                        "quantity": 8,
                        "idempotency_key": "api-postgres-receipt",
                        "reference": "API-PO-001",
                    },
                )
                assert receipt.status_code == 201
                inventory = await client.get(f"/inventory/{product_id}")
                assert inventory.status_code == 200
                assert inventory.json()["on_hand"] == 8
                assert inventory.json()["available_quantity"] == 8
    finally:
        inventory_module.inventory_store = original_store


@pytest.mark.asyncio
async def test_demo_bootstrap_is_explicit_idempotent_and_reconciled(
    postgres_store,
) -> None:
    store, _manager, _settings = postgres_store

    assert await store.list_products() == []
    await store.seed_demo_data()

    product = await store.get_product(7)
    assert product["sku"] == "DEMO-007"
    assert product["unit_price"] == Decimal("99.90")

    snapshot = await store.inventory_snapshot(7)
    assert (snapshot["on_hand"], snapshot["reserved"]) == (42, 0)

    first_movements = await store.list_movements(7)
    assert len(first_movements) == 1
    assert first_movements[0]["movement_type"] == "opening_balance"
    assert first_movements[0]["quantity_delta_on_hand"] == 42

    await store.seed_demo_data()
    assert await store.inventory_snapshot(7) == snapshot
    assert await store.list_movements(7) == first_movements

    assert await store.reconcile(7) == {
        "on_hand": 42,
        "reserved": 0,
        "journal_on_hand": 42,
        "journal_reserved": 0,
    }



def _start_inventory_api_process(database_url: str) -> tuple[subprocess.Popen, str]:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    environment = os.environ.copy()
    environment.update(
        {
            "ENVIRONMENT": "development",
            "PERSISTENCE_BACKEND": "postgres",
            "DATABASE_URL": database_url,
            "DATABASE_POOL_MIN_SIZE": "1",
            "DATABASE_POOL_MAX_SIZE": "4",
            "INVENTORY_SEED_DEMO_DATA": "false",
            "SERVICE_NAME": "inventory-api-concurrency-test",
            "LOG_LEVEL": "WARNING",
            "OTEL_TRACES_SAMPLER_ARG": "0.0",
        }
    )
    environment.pop("EVENT_COLLECTOR_URL", None)
    environment.pop("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None)
    environment.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.inventory.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=os.path.dirname(os.path.dirname(__file__)),
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return process, f"http://127.0.0.1:{port}"


async def _wait_for_inventory_api(
    process: subprocess.Popen,
    base_url: str,
    *,
    timeout_seconds: float = 20,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    last_error: Exception | None = None
    async with httpx.AsyncClient(timeout=1) as client:
        while asyncio.get_running_loop().time() < deadline:
            if process.poll() is not None:
                raise AssertionError(
                    f"Inventory API process exited during startup: {process.returncode}"
                )
            try:
                response = await client.get(f"{base_url}/healthz")
                if response.status_code == 200:
                    return
                last_error = AssertionError(
                    f"Inventory API readiness returned HTTP {response.status_code}"
                )
            except httpx.HTTPError as exc:
                last_error = exc
            await asyncio.sleep(0.1)

    raise AssertionError(
        f"Inventory API did not become ready at {base_url}: {last_error}"
    )


async def _stop_inventory_api_processes(
    processes: list[subprocess.Popen],
) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()

    for process in processes:
        if process.poll() is None:
            try:
                await asyncio.to_thread(process.wait, timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=5)


@pytest.mark.asyncio
async def test_inventory_operations_are_safe_across_two_api_processes(
    postgres_schema_url: str,
    postgres_store,
) -> None:
    """Exercise real HTTP requests handled by two independent Uvicorn processes."""
    store, _manager, _settings = postgres_store
    product = await store.create_product(product_request("SKU-MULTI-PROCESS"))
    product_id = product["product_id"]
    await store.receive_stock(
        product_id,
        InventoryReceiptRequest(
            quantity=10,
            idempotency_key="multi-process-opening",
            reference="opening-stock",
        ),
    )

    processes: list[subprocess.Popen] = []
    clients: list[httpx.AsyncClient] = []
    try:
        for _ in range(2):
            process, base_url = _start_inventory_api_process(postgres_schema_url)
            processes.append(process)
            clients.append(httpx.AsyncClient(base_url=base_url, timeout=10))

        await asyncio.gather(
            *(
                _wait_for_inventory_api(process, str(client.base_url))
                for process, client in zip(processes, clients, strict=True)
            )
        )

        # Two independent API processes compete for more stock than is available.
        reservation_responses = await asyncio.gather(
            clients[0].post(
                f"/inventory/{product_id}/reservations",
                json={"order_id": 7001, "saga_id": "oversell-a", "quantity": 7},
            ),
            clients[1].post(
                f"/inventory/{product_id}/reservations",
                json={"order_id": 7002, "saga_id": "oversell-b", "quantity": 7},
            ),
        )
        assert sorted(response.status_code for response in reservation_responses) == [
            200,
            409,
        ]
        snapshot = await store.inventory_snapshot(product_id)
        assert (snapshot["on_hand"], snapshot["reserved"]) == (10, 7)

        # Simultaneous retries of one receipt must return the same durable result.
        receipt_payload = {
            "quantity": 3,
            "idempotency_key": "multi-process-receipt",
            "reference": "PO-MULTI-PROCESS",
        }
        receipt_responses = await asyncio.gather(
            clients[0].post(
                f"/inventory/{product_id}/receipts", json=receipt_payload
            ),
            clients[1].post(
                f"/inventory/{product_id}/receipts", json=receipt_payload
            ),
        )
        assert [response.status_code for response in receipt_responses] == [201, 201]
        receipt_ids = {
            response.json()["receipt_id"] for response in receipt_responses
        }
        assert len(receipt_ids) == 1
        assert (await store.inventory_snapshot(product_id))["on_hand"] == 13

        conflicting_receipt = await clients[1].post(
            f"/inventory/{product_id}/receipts",
            json={
                **receipt_payload,
                "quantity": 4,
            },
        )
        assert conflicting_receipt.status_code == 409
        assert (await store.inventory_snapshot(product_id))["on_hand"] == 13

        # Adjustment idempotency is also shared by the independent processes.
        adjustment_payload = {
            "quantity_delta": -1,
            "reason": "concurrent cycle count",
            "idempotency_key": "multi-process-adjustment",
        }
        adjustment_responses = await asyncio.gather(
            clients[0].post(
                f"/inventory/{product_id}/adjustments",
                json=adjustment_payload,
            ),
            clients[1].post(
                f"/inventory/{product_id}/adjustments",
                json=adjustment_payload,
            ),
        )
        assert [response.status_code for response in adjustment_responses] == [
            201,
            201,
        ]
        movement_ids = {
            response.json()["movement_id"] for response in adjustment_responses
        }
        assert len(movement_ids) == 1
        conflicting_adjustment = await clients[0].post(
            f"/inventory/{product_id}/adjustments",
            json={**adjustment_payload, "quantity_delta": -2},
        )
        assert conflicting_adjustment.status_code == 409

        # Replaying the same reservation identity returns one reservation and
        # does not increment reserved stock or append another movement.
        reservation_payload = {
            "order_id": 7003,
            "saga_id": "multi-process-same-reservation",
            "quantity": 2,
        }
        duplicate_reservations = await asyncio.gather(
            clients[0].post(
                f"/inventory/{product_id}/reservations",
                json=reservation_payload,
            ),
            clients[1].post(
                f"/inventory/{product_id}/reservations",
                json=reservation_payload,
            ),
        )
        assert [response.status_code for response in duplicate_reservations] == [
            200,
            200,
        ]
        reservation_ids = {
            response.json()["reservation_id"]
            for response in duplicate_reservations
        }
        assert len(reservation_ids) == 1

        conflicting_reservation = await clients[1].post(
            f"/inventory/{product_id}/reservations",
            json={**reservation_payload, "quantity": 3},
        )
        assert conflicting_reservation.status_code == 409

        # Release and commit race for one reservation. The reservation row lock
        # permits exactly one terminal transition; the loser observes that state.
        reservation_id = next(iter(reservation_ids))
        release_response, commit_response = await asyncio.gather(
            clients[0].delete(f"/inventory/reservations/{reservation_id}"),
            clients[1].post(
                f"/inventory/reservations/{reservation_id}/commit"
            ),
        )
        assert release_response.status_code == 200
        final_status = release_response.json()["status"]
        if final_status == "released":
            assert commit_response.status_code == 409
        else:
            assert final_status == "committed"
            assert commit_response.status_code == 200
            assert commit_response.json()["status"] == "committed"

        # Simulate a response timeout while the database transaction is
        # still completing. Retrying the same key on the other process must
        # either replay the committed response or safely perform the rolled-
        # back operation, never crediting stock twice.
        async with _manager.connection() as connection:
            await connection.execute(
                """
                CREATE FUNCTION delay_timeout_receipt_idempotency()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $timeout$
                BEGIN
                    IF NEW.idempotency_key = 'multi-process-timeout-retry' THEN
                        PERFORM pg_sleep(0.4);
                    END IF;
                    RETURN NEW;
                END;
                $timeout$
                """
            )
            await connection.execute(
                """
                CREATE TRIGGER delay_timeout_receipt_idempotency
                BEFORE INSERT ON idempotency_records
                FOR EACH ROW
                EXECUTE FUNCTION delay_timeout_receipt_idempotency()
                """
            )

        before_timeout_retry = await store.inventory_snapshot(product_id)
        async with httpx.AsyncClient(
            base_url=str(clients[0].base_url), timeout=0.1
        ) as impatient_client:
            with pytest.raises(httpx.TimeoutException):
                await impatient_client.post(
                    f"/inventory/{product_id}/receipts",
                    json={
                        "quantity": 2,
                        "idempotency_key": "multi-process-timeout-retry",
                        "reference": "timeout-retry",
                    },
                )

        # Allow the original request either to commit or to roll back before
        # retrying from the second API process.
        await asyncio.sleep(0.6)
        retried_receipt = await clients[1].post(
            f"/inventory/{product_id}/receipts",
            json={
                "quantity": 2,
                "idempotency_key": "multi-process-timeout-retry",
                "reference": "timeout-retry",
            },
        )
        assert retried_receipt.status_code == 201
        after_timeout_retry = await store.inventory_snapshot(product_id)
        assert after_timeout_retry["on_hand"] == before_timeout_retry["on_hand"] + 2

        movements = await store.list_movements(product_id, limit=100)
        assert len(movements) == 7
        timeout_movements = [
            movement
            for movement in movements
            if movement["idempotency_key"] == "multi-process-timeout-retry"
        ]
        assert len(timeout_movements) == 1
        assert timeout_movements[0]["movement_id"] == retried_receipt.json()[
            "receipt_id"
        ]
        reconciliation = await store.reconcile(product_id)
        final_snapshot = await store.inventory_snapshot(product_id)
        assert reconciliation == {
            "on_hand": final_snapshot["on_hand"],
            "reserved": final_snapshot["reserved"],
            "journal_on_hand": final_snapshot["on_hand"],
            "journal_reserved": final_snapshot["reserved"],
        }
        assert final_snapshot["reserved"] == 7
    finally:
        await asyncio.gather(*(client.aclose() for client in clients))
        await _stop_inventory_api_processes(processes)


@pytest.mark.asyncio
async def test_inventory_writes_roll_back_all_related_rows_on_late_failure(
    postgres_store,
) -> None:
    store, manager, _settings = postgres_store
    receipt_product = await store.create_product(
        product_request("SKU-ROLLBACK-RECEIPT")
    )
    adjustment_product = await store.create_product(
        product_request("SKU-ROLLBACK-ADJUSTMENT")
    )
    reservation_product = await store.create_product(
        product_request("SKU-ROLLBACK-RESERVATION")
    )
    transition_product = await store.create_product(
        product_request("SKU-ROLLBACK-TRANSITION")
    )

    await store.receive_stock(
        reservation_product["product_id"],
        InventoryReceiptRequest(
            quantity=5,
            idempotency_key="rollback-reservation-opening",
            reference="opening",
        ),
    )
    transition_product_id = transition_product["product_id"]
    await store.receive_stock(
        transition_product_id,
        InventoryReceiptRequest(
            quantity=10,
            idempotency_key="rollback-transition-opening",
            reference="opening",
        ),
    )
    reservation = await store.reserve(
        transition_product_id,
        InventoryReservationRequest(
            order_id=8001,
            saga_id="rollback-transition",
            quantity=3,
        ),
    )

    # Fail late, after each operation has changed its projection and/or
    # reservation row but before its movement can be committed.
    async with manager.connection() as connection:
        await connection.execute(
            """
            CREATE FUNCTION reject_inventory_movement_insert()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                RAISE EXCEPTION 'injected movement failure';
            END;
            $$
            """
        )
        await connection.execute(
            """
            CREATE TRIGGER reject_inventory_movement_insert
            BEFORE INSERT ON inventory_movements
            FOR EACH ROW
            EXECUTE FUNCTION reject_inventory_movement_insert()
            """
        )

    with pytest.raises(psycopg.Error, match="injected movement failure"):
        await store.receive_stock(
            receipt_product["product_id"],
            InventoryReceiptRequest(
                quantity=4,
                idempotency_key="rollback-receipt",
                reference="must-not-commit",
            ),
        )

    with pytest.raises(psycopg.Error, match="injected movement failure"):
        await store.adjust_stock(
            adjustment_product["product_id"],
            InventoryAdjustmentRequest(
                quantity_delta=4,
                reason="must-not-commit",
                idempotency_key="rollback-adjustment",
            ),
        )

    with pytest.raises(psycopg.Error, match="injected movement failure"):
        await store.reserve(
            reservation_product["product_id"],
            InventoryReservationRequest(
                order_id=8002,
                saga_id="rollback-reservation",
                quantity=2,
            ),
        )

    with pytest.raises(psycopg.Error, match="injected movement failure"):
        await store.release(reservation["reservation_id"])

    with pytest.raises(psycopg.Error, match="injected movement failure"):
        await store.commit(reservation["reservation_id"])

    assert (await store.inventory_snapshot(receipt_product["product_id"]))[
        "on_hand"
    ] == 0
    assert (await store.inventory_snapshot(adjustment_product["product_id"]))[
        "on_hand"
    ] == 0
    assert (await store.inventory_snapshot(reservation_product["product_id"])) == {
        "product_id": reservation_product["product_id"],
        "on_hand": 5,
        "reserved": 0,
        "available": True,
        "available_quantity": 5,
        "quantity": 5,
    }
    assert (
        await store.inventory_snapshot(transition_product_id)
    )["on_hand"] == 10
    assert (
        await store.inventory_snapshot(transition_product_id)
    )["reserved"] == 3

    assert await store.list_movements(receipt_product["product_id"]) == []
    assert await store.list_movements(adjustment_product["product_id"]) == []
    assert len(await store.list_movements(reservation_product["product_id"])) == 1
    assert len(await store.list_movements(transition_product_id)) == 2

    async with manager.connection() as connection:
        for product_id, key, scope in (
            (
                receipt_product["product_id"],
                "rollback-receipt",
                f"receipt:{receipt_product['product_id']}",
            ),
            (
                adjustment_product["product_id"],
                "rollback-adjustment",
                f"adjustment:{adjustment_product['product_id']}",
            ),
        ):
            cursor = await connection.execute(
                """
                SELECT count(*) AS record_count
                FROM idempotency_records
                WHERE scope = %s AND idempotency_key = %s
                """,
                (scope, key),
            )
            assert (await cursor.fetchone())["record_count"] == 0

        cursor = await connection.execute(
            """
            SELECT status
            FROM reservations
            WHERE reservation_id = %s
            """,
            (reservation["reservation_id"],),
        )
        assert (await cursor.fetchone())["status"] == "reserved"

        cursor = await connection.execute(
            """
            SELECT count(*) AS reservation_count
            FROM reservations
            WHERE saga_id = 'rollback-reservation'
            """
        )
        assert (await cursor.fetchone())["reservation_count"] == 0


@pytest.mark.asyncio
async def test_database_constraints_reject_negative_and_over_reserved_balances(
    postgres_store,
) -> None:
    store, manager, _settings = postgres_store
    product = await store.create_product(product_request("SKU-DB-CONSTRAINTS"))
    product_id = product["product_id"]
    await store.receive_stock(
        product_id,
        InventoryReceiptRequest(
            quantity=5,
            idempotency_key="constraints-opening",
            reference="opening",
        ),
    )

    with pytest.raises(psycopg.errors.CheckViolation):
        async with manager.connection() as connection:
            await connection.execute(
                """
                UPDATE inventory_balances
                SET on_hand = -1
                WHERE product_id = %s
                """,
                (product_id,),
            )

    with pytest.raises(psycopg.errors.CheckViolation):
        async with manager.connection() as connection:
            await connection.execute(
                """
                UPDATE inventory_balances
                SET reserved = on_hand + 1
                WHERE product_id = %s
                """,
                (product_id,),
            )

    assert await store.inventory_snapshot(product_id) == {
        "product_id": product_id,
        "on_hand": 5,
        "reserved": 0,
        "available": True,
        "available_quantity": 5,
        "quantity": 5,
    }
