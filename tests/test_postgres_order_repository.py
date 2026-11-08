from __future__ import annotations

import os
from decimal import Decimal
from itertools import count
from uuid import UUID, uuid4

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from app.inventory.postgres_store import PostgresInventoryStore
from app.inventory.store import InventoryReservationRequest, ProductCreate
from app.order.repository import PostgresOrderRepository
from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager
from app.persistence.migrations import MIGRATIONS_DIR, apply_migrations


@pytest.fixture
def postgres_order_schema_url() -> str:
    database_url = os.getenv("POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("POSTGRES_TEST_URL is required for PostgreSQL integration tests")

    schema = f"test_orders_{uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    isolated_url = make_conninfo(
        database_url,
        options=f"-c search_path={schema}",
    )
    try:
        with psycopg.connect(isolated_url) as connection:
            apply_migrations(connection, MIGRATIONS_DIR)
        yield isolated_url
    finally:
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


def order_settings(database_url: str) -> DatabaseSettings:
    return DatabaseSettings(
        backend="postgres",
        database_url=database_url,
        connect_timeout_seconds=5,
        pool_min_size=1,
        pool_max_size=4,
    )


def outcome(
    *,
    order_id: int,
    requested_product_id: int,
    product_id: int | None,
    status: str,
    saga_id: str,
) -> dict:
    return {
        "order_id": order_id,
        "requested_product_id": requested_product_id,
        "product_id": product_id,
        "quantity": 2,
        "unit_price": Decimal("12.50") if product_id else None,
        "total_amount": Decimal("25.00") if product_id else None,
        "saga_id": saga_id,
        "reservation_id": None,
        "payment_id": None,
        "status": status,
        "failure_category": None if status == "pending" else status,
        "correlation_id": f"correlation-{order_id}",
    }


@pytest.mark.asyncio
async def test_order_repository_create_update_and_read_survive_pool_restart(
    postgres_order_schema_url: str,
) -> None:
    settings = order_settings(postgres_order_schema_url)
    manager = DatabaseManager()
    await manager.open(settings)
    try:
        inventory = PostgresInventoryStore(manager)
        product = await inventory.create_product(
            ProductCreate(
                sku="ORDER-REPO-001",
                name="Order repository fixture",
                category="integration",
                unit_price=Decimal("12.50"),
            )
        )
        repository = PostgresOrderRepository(manager)

        pending = outcome(
            order_id=700,
            requested_product_id=product["product_id"],
            product_id=product["product_id"],
            status="pending",
            saga_id="saga-order-700",
        )
        first = await repository.save_outcome(pending)
        assert first["status"] == "pending"

        completed = {
            **pending,
            "status": "processed",
            "failure_category": None,
            "payment_id": "payment-700",
        }
        updated = await repository.save_outcome(completed)
        assert updated["status"] == "processed"
        assert updated["payment_id"] == "payment-700"

        missing_product = outcome(
            order_id=701,
            requested_product_id=987654,
            product_id=None,
            status="product_not_found",
            saga_id="saga-order-701",
        )
        failed = await repository.save_outcome(missing_product)
        assert failed["product_id"] is None
        assert failed["requested_product_id"] == 987654
        assert failed["unit_price"] is None
    finally:
        await manager.close()

    restarted_manager = DatabaseManager()
    await restarted_manager.open(settings)
    try:
        restarted_repository = PostgresOrderRepository(restarted_manager)
        persisted = await restarted_repository.get(700)
        assert persisted is not None
        assert persisted["status"] == "processed"
        assert persisted["payment_id"] == "payment-700"
        assert persisted["total_amount"] == Decimal("25.00")

        persisted_failure = await restarted_repository.get(701)
        assert persisted_failure is not None
        assert persisted_failure["status"] == "product_not_found"
        assert persisted_failure["product_id"] is None
        assert persisted_failure["requested_product_id"] == 987654
    finally:
        await restarted_manager.close()


@pytest.mark.asyncio
async def test_order_api_persists_success_and_catalog_failure_without_contract_changes(
    postgres_order_schema_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from importlib import import_module

    from app.order import workflow

    order_module = import_module("app.order.main")
    fixed_saga_ids = iter(
        [
            UUID("00000000-0000-4000-8000-000000000001"),
            UUID("00000000-0000-4000-8000-000000000002"),
        ]
    )
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("PERSISTENCE_BACKEND", "postgres")
    monkeypatch.setenv("DATABASE_URL", postgres_order_schema_url)
    monkeypatch.setenv("DATABASE_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("DATABASE_POOL_MAX_SIZE", "4")
    monkeypatch.setattr(order_module, "uuid4", lambda: next(fixed_saga_ids))
    await workflow.payment_gateway.reset()

    reservation_id: str | None = None

    def inventory_handler(request: httpx.Request) -> httpx.Response:
        nonlocal reservation_id
        if request.method == "GET" and request.url.path == "/products/999":
            return httpx.Response(404, json={"detail": "not found"})
        if request.method == "GET" and request.url.path == "/products/7":
            return httpx.Response(
                200,
                json={
                    "product_id": 7,
                    "sku": "DEMO-007",
                    "unit_price": "99.90",
                    "is_active": True,
                },
            )
        if request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "reservation_id": reservation_id,
                    "status": "reserved",
                },
            )
        if request.method == "DELETE":
            return httpx.Response(
                200,
                json={
                    "reservation_id": reservation_id,
                    "status": "released",
                },
            )
        return httpx.Response(404)

    def inventory_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(inventory_handler),
            base_url="http://inventory.test",
        )

    monkeypatch.setattr(workflow, "create_inventory_client", inventory_client)

    try:
        async with order_module.lifespan(order_module.app):
            inventory = PostgresInventoryStore(order_module.database_manager)
            await inventory.seed_demo_data()
            reservation = await inventory.reserve(
                7,
                InventoryReservationRequest(
                    order_id=124,
                    saga_id="00000000-0000-4000-8000-000000000002",
                    quantity=2,
                ),
            )
            reservation_id = reservation["reservation_id"]

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=order_module.app),
                base_url="http://order.test",
            ) as client:
                not_found = await client.post(
                    "/orders/123/process",
                    json={"product_id": 999, "quantity": 2},
                )
                assert not_found.status_code == 404
                assert not_found.json() == {"detail": "Product not found."}

                completed = await client.post(
                    "/orders/124/process",
                    json={"product_id": 7, "quantity": 2},
                )
                assert completed.status_code == 201
                response_body = completed.json()
                assert set(response_body) == {
                    "order_id",
                    "status",
                    "saga_id",
                    "correlation_id",
                    "reservation_id",
                    "payment_id",
                    "unit_price",
                    "total_amount",
                }
                assert response_body["status"] == "processed"
                assert response_body["unit_price"] == "99.90"
                assert response_body["total_amount"] == "199.80"
                assert response_body["reservation_id"] == reservation_id

            repository = PostgresOrderRepository(order_module.database_manager)
            failed = await repository.get(123)
            processed = await repository.get(124)
            assert failed is not None and failed["status"] == "product_not_found"
            assert failed["product_id"] is None
            assert failed["requested_product_id"] == 999
            assert processed is not None and processed["status"] == "processed"
            assert processed["reservation_id"] == reservation_id
            assert processed["payment_id"] == response_body["payment_id"]
            assert workflow._orders == {}
    finally:
        await workflow.payment_gateway.reset()
