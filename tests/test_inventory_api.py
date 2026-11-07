from __future__ import annotations

from importlib import import_module

import httpx
import pytest

from app import observability
from app.inventory.store import InventoryStore

configure_observability = observability.configure_observability


def skip_observability_setup(app, service_name):
    return None


observability.configure_observability = skip_observability_setup
try:
    inventory_module = import_module("app.inventory.main")
finally:
    observability.configure_observability = configure_observability

inventory_app = inventory_module.app


@pytest.fixture(autouse=True)
def isolated_inventory(monkeypatch):
    monkeypatch.setattr(inventory_module, "inventory_store", InventoryStore())


async def request(method: str, path: str, **kwargs):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=inventory_app),
        base_url="http://inventory.test",
    ) as client:
        return await client.request(method, path, **kwargs)


async def create_product(*, quantity: int = 0) -> int:
    response = await request(
        "POST",
        "/products",
        json={
            "sku": "API-001",
            "name": "API test product",
            "category": "test",
            "unit_price": "19.90",
        },
    )
    assert response.status_code == 201
    product_id = response.json()["product_id"]
    if quantity:
        receipt = await request(
            "POST",
            f"/inventory/{product_id}/receipts",
            json={
                "quantity": quantity,
                "idempotency_key": "api-opening-receipt",
                "reference": "TEST-PO-001",
            },
        )
        assert receipt.status_code == 201
    return product_id


@pytest.mark.asyncio
async def test_catalog_create_list_update_and_duplicate_sku():
    response = await request(
        "POST",
        "/products",
        json={
            "sku": "api-001",
            "name": "API test product",
            "category": "test",
            "unit_price": "19.90",
        },
    )
    assert response.status_code == 201
    product_id = response.json()["product_id"]
    assert response.json()["sku"] == "API-001"

    duplicate = await request(
        "POST",
        "/products",
        json={
            "sku": "API-001",
            "name": "Duplicate product",
            "category": "test",
            "unit_price": "1.00",
        },
    )
    assert duplicate.status_code == 409

    updated = await request(
        "PATCH", f"/products/{product_id}", json={"is_active": False}
    )
    assert updated.status_code == 200
    assert updated.json()["is_active"] is False

    active = await request("GET", "/products")
    all_products = await request("GET", "/products?include_inactive=true")
    assert active.json() == []
    assert len(all_products.json()) == 1


@pytest.mark.asyncio
async def test_receipt_reservation_release_and_inventory_projection():
    product_id = await create_product(quantity=10)

    receipt = await request(
        "POST",
        f"/inventory/{product_id}/receipts",
        json={
            "quantity": 4,
            "idempotency_key": "supplier-delivery-001",
            "reference": "PO-001",
        },
    )
    assert receipt.status_code == 201

    repeated = await request(
        "POST",
        f"/inventory/{product_id}/receipts",
        json={
            "quantity": 4,
            "idempotency_key": "supplier-delivery-001",
            "reference": "PO-001",
        },
    )
    assert repeated.json()["receipt_id"] == receipt.json()["receipt_id"]

    reservation = await request(
        "POST",
        f"/inventory/{product_id}/reservations",
        json={"order_id": 123, "saga_id": "api-saga", "quantity": 6},
    )
    assert reservation.status_code == 200

    snapshot = await request("GET", f"/inventory/{product_id}")
    assert snapshot.json()["on_hand"] == 14
    assert snapshot.json()["reserved"] == 6
    assert snapshot.json()["available_quantity"] == 8

    released = await request(
        "DELETE",
        f"/inventory/reservations/{reservation.json()['reservation_id']}",
    )
    assert released.json()["status"] == "released"
    snapshot = await request("GET", f"/inventory/{product_id}")
    assert snapshot.json()["available_quantity"] == 14


@pytest.mark.asyncio
async def test_unknown_product_and_insufficient_stock_are_rejected():
    missing = await request("GET", "/inventory/999")
    assert missing.status_code == 404

    product_id = await create_product(quantity=3)
    rejected = await request(
        "POST",
        f"/inventory/{product_id}/reservations",
        json={"order_id": 1, "saga_id": "too-large", "quantity": 4},
    )
    assert rejected.status_code == 409

    snapshot = await request("GET", f"/inventory/{product_id}")
    assert snapshot.json()["available_quantity"] == 3


@pytest.mark.asyncio
async def test_inventory_commit_reduces_on_hand_and_excludes_reserved():
    product_id = await create_product(quantity=10)
    reservation = await request(
        "POST",
        f"/inventory/{product_id}/reservations",
        json={"order_id": 1, "saga_id": "commit-saga", "quantity": 4},
    )
    committed = await request(
        "POST",
        f"/inventory/reservations/{reservation.json()['reservation_id']}/commit",
    )

    assert committed.json()["status"] == "committed"
    snapshot = await request("GET", f"/inventory/{product_id}")
    assert (snapshot.json()["on_hand"], snapshot.json()["reserved"]) == (6, 0)
