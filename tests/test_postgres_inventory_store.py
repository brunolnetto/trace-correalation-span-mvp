from __future__ import annotations

import os
from decimal import Decimal
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

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
