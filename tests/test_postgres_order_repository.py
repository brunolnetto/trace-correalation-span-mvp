from __future__ import annotations

import json
import os
from collections.abc import Iterator
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
import psycopg
import pytest
from psycopg import sql
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from app.inventory.postgres_store import PostgresInventoryStore
from app.inventory.store import InventoryReservationRequest, ProductCreate
from app.order.repository import PostgresOrderRepository
from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager
from app.persistence.migrations import MIGRATIONS_DIR, apply_migrations


@pytest.fixture
def postgres_order_schema_url() -> Iterator[str]:
    database_url = os.getenv("POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("POSTGRES_TEST_URL is required for PostgreSQL integration tests")

    schema = f"test_orders_{uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    parsed_url = urlsplit(database_url)
    query = parse_qsl(parsed_url.query, keep_blank_values=True)
    query.append(("options", f"-c search_path={schema}"))
    isolated_url = urlunsplit(
        parsed_url._replace(query=urlencode(query))
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
            UUID("00000000-0000-4000-8000-000000000003"),
            UUID("00000000-0000-4000-8000-000000000004"),
        ]
    )
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("PERSISTENCE_BACKEND", "postgres")
    monkeypatch.setenv("DATABASE_URL", postgres_order_schema_url)
    monkeypatch.setenv("DATABASE_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("DATABASE_POOL_MAX_SIZE", "4")
    monkeypatch.setattr(order_module, "uuid4", lambda: next(fixed_saga_ids))
    await workflow.payment_gateway.reset()

    reservation_ids: dict[int, str] = {}
    failed_release_reservations: set[str] = set()

    def inventory_handler(request: httpx.Request) -> httpx.Response:
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
            order_id = json.loads(request.content)["order_id"]
            return httpx.Response(
                200,
                json={
                    "reservation_id": reservation_ids[order_id],
                    "status": "reserved",
                },
            )
        if request.method == "DELETE":
            reservation_id = request.url.path.rsplit("/", 1)[-1]
            if reservation_id in failed_release_reservations:
                return httpx.Response(
                    503,
                    json={"detail": "intentional compensation failure"},
                )
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
            assert await order_module.database_manager.healthy()
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
            reservation_ids[124] = reservation["reservation_id"]
            failed_reservation = await inventory.reserve(
                7,
                InventoryReservationRequest(
                    order_id=125,
                    saga_id="00000000-0000-4000-8000-000000000003",
                    quantity=11,
                ),
            )
            reservation_ids[125] = failed_reservation["reservation_id"]
            incomplete_reservation = await inventory.reserve(
                7,
                InventoryReservationRequest(
                    order_id=126,
                    saga_id="00000000-0000-4000-8000-000000000004",
                    quantity=11,
                ),
            )
            reservation_ids[126] = incomplete_reservation["reservation_id"]
            failed_release_reservations.add(incomplete_reservation["reservation_id"])

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
                assert response_body["reservation_id"] == reservation_ids[124]

                declined = await client.post(
                    "/orders/125/process",
                    json={"product_id": 7, "quantity": 11},
                )
                assert declined.status_code == 402
                assert declined.json() == {
                    "detail": (
                        "Payment declined; completed actions were compensated."
                    )
                }

                incomplete = await client.post(
                    "/orders/126/process",
                    json={"product_id": 7, "quantity": 11},
                )
                assert incomplete.status_code == 500
                assert incomplete.json()["detail"]["incomplete_compensations"] == [
                    "reserve_inventory"
                ]

            repository = PostgresOrderRepository(order_module.database_manager)
            failed = await repository.get(123)
            processed = await repository.get(124)
            payment_declined = await repository.get(125)
            assert failed is not None and failed["status"] == "product_not_found"
            assert failed["product_id"] is None
            assert failed["requested_product_id"] == 999
            assert processed is not None and processed["status"] == "processed"
            assert processed["reservation_id"] == reservation_ids[124]
            assert processed["payment_id"] == response_body["payment_id"]
            assert payment_declined is not None
            assert payment_declined["status"] == "payment_declined"
            assert payment_declined["reservation_id"] == reservation_ids[125]
            assert payment_declined["payment_id"] is None

            compensation_incomplete = await repository.get(126)
            assert compensation_incomplete is not None
            assert compensation_incomplete["status"] == "compensation_incomplete"
            assert compensation_incomplete["failure_category"] == "payment_declined"
            assert (
                compensation_incomplete["reservation_id"]
                == reservation_ids[126]
            )
            assert workflow._orders == {}
    finally:
        await workflow.payment_gateway.reset()
