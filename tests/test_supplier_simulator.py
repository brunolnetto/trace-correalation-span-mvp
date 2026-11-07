from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
import pytest

from app.supplier_simulator.store import (
    ReorderPolicy,
    SupplierSimulation,
)


class InventoryDouble:
    def __init__(self) -> None:
        self.products: dict[int, dict] = {}
        self.next_product_id = 1
        self.balances: dict[int, dict[str, int]] = {}
        self.receipts: dict[str, dict] = {}
        self.receipt_calls = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.strip("/").split("/")
        if request.method == "POST" and parts == ["products"]:
            payload = json.loads(request.content)
            if any(item["sku"] == payload["sku"] for item in self.products.values()):
                return httpx.Response(409, json={"detail": "duplicate SKU"})
            product_id = self.next_product_id
            self.next_product_id += 1
            self.products[product_id] = {"product_id": product_id, **payload}
            self.balances[product_id] = {"on_hand": 0, "reserved": 0}
            return httpx.Response(201, json=self.products[product_id])
        if request.method == "GET" and parts == ["products"]:
            return httpx.Response(200, json=list(self.products.values()))
        if request.method == "GET" and len(parts) == 2 and parts[0] == "products":
            product = self.products.get(int(parts[1]))
            return httpx.Response(200, json=product) if product else httpx.Response(404)
        if request.method == "GET" and len(parts) == 2 and parts[0] == "inventory":
            product_id = int(parts[1])
            balance = self.balances.get(product_id)
            if balance is None:
                return httpx.Response(404)
            available = balance["on_hand"] - balance["reserved"]
            return httpx.Response(
                200,
                json={
                    "product_id": product_id,
                    **balance,
                    "available_quantity": available,
                    "quantity": available,
                },
            )
        if (
            request.method == "POST"
            and len(parts) == 3
            and parts[0] == "inventory"
            and parts[2] == "receipts"
        ):
            self.receipt_calls += 1
            product_id = int(parts[1])
            payload = __import__("json").loads(request.content)
            key = f"{product_id}:{payload['idempotency_key']}"
            if key not in self.receipts:
                self.balances[product_id]["on_hand"] += payload["quantity"]
                self.receipts[key] = {
                    "receipt_id": f"receipt-{len(self.receipts) + 1}",
                    "product_id": product_id,
                    "quantity": payload["quantity"],
                    "reference": payload.get("reference"),
                }
            return httpx.Response(201, json=self.receipts[key])
        return httpx.Response(404)


@pytest.fixture
def inventory_double():
    return InventoryDouble()


@pytest.fixture
async def client(inventory_double):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(inventory_double.handler),
        base_url="http://inventory.test",
    ) as value:
        yield value


@pytest.mark.asyncio
async def test_replenishment_uses_available_stock_and_avoids_duplicate_open_orders(
    client, inventory_double
):
    simulation = SupplierSimulation()
    await simulation.set_policy(
        ReorderPolicy(
            product_id=1,
            reorder_point=8,
            target_stock=30,
            lead_time_seconds=60,
        )
    )
    # on_hand=20 and reserved=15 means only five are available.
    inventory_double.products[1] = {"product_id": 1, "sku": "SKU-1"}
    inventory_double.balances[1] = {"on_hand": 20, "reserved": 15}

    first = await simulation.evaluate_replenishment(client, "correlation-1")
    assert first[0]["decision"] == "purchase_order_created"
    order = first[0]["purchase_order"]
    assert order["inventory_position_at_order"] == 5
    assert order["quantity"] == 25

    second = await simulation.evaluate_replenishment(client, "correlation-2")
    assert second[0]["decision"] == "open_purchase_order_exists"
    assert second[0]["on_order"] == 25
    assert len(await simulation.list_purchase_orders()) == 1


@pytest.mark.asyncio
async def test_supplier_lead_time_receipt_and_idempotent_replay(
    client, inventory_double
):
    simulation = SupplierSimulation(
        initial_time=datetime(2026, 1, 1, tzinfo=UTC)
    )
    await simulation.set_policy(
        ReorderPolicy(
            product_id=1,
            reorder_point=5,
            target_stock=12,
            lead_time_seconds=60,
        )
    )
    inventory_double.products[1] = {"product_id": 1, "sku": "SKU-1"}
    inventory_double.balances[1] = {"on_hand": 5, "reserved": 0}

    decision = (await simulation.evaluate_replenishment(client, "corr"))[0]
    purchase_order_id = decision["purchase_order"]["purchase_order_id"]
    ordered = await simulation.order_purchase_order(purchase_order_id)
    assert ordered["status"] == "ordered"
    assert ordered["due_at"] == "2026-01-01T00:01:00+00:00"

    with pytest.raises(ValueError, match="lead time"):
        await simulation.receive_purchase_order(purchase_order_id, client, "corr")

    await simulation.advance_clock(60)
    received = await simulation.receive_purchase_order(
        purchase_order_id, client, "corr"
    )
    replayed = await simulation.receive_purchase_order(
        purchase_order_id, client, "corr"
    )

    assert received["status"] == "received"
    assert replayed["receipt_id"] == received["receipt_id"]
    assert inventory_double.receipt_calls == 1
    assert inventory_double.balances[1]["on_hand"] == 12


@pytest.mark.asyncio
async def test_supplier_catalog_onboarding_creates_product_and_reorder_policy(client):
    simulation = SupplierSimulation()
    first = await simulation.onboard_product("BEANS-250", client, "corr")
    replayed = await simulation.onboard_product("BEANS-250", client, "corr")

    assert first == replayed
    assert first["product"]["sku"] == "COFFEE-BEANS-250G"
    assert first["reorder_policy"]["reorder_point"] == 12
    assert first["reorder_policy"]["target_stock"] == 48
    assert len(await simulation.list_policies()) == 1
    assert len(await simulation.list_purchase_orders()) == 0


@pytest.mark.asyncio
async def test_onboarding_recovers_catalog_write_after_supplier_simulator_restart(
    client
):
    first_simulation = SupplierSimulation()
    first = await first_simulation.onboard_product("FILTER-100", client, "corr")
    restarted_simulation = SupplierSimulation()
    recovered = await restarted_simulation.onboard_product(
        "FILTER-100", client, "corr"
    )

    assert recovered["product"]["product_id"] == first["product"]["product_id"]
    assert recovered["reorder_policy"]["product_id"] == first["product"]["product_id"]


@pytest.mark.asyncio
async def test_purchase_order_cannot_be_cancelled_after_ordering(
    client, inventory_double
):
    simulation = SupplierSimulation()
    await simulation.set_policy(
        ReorderPolicy(
            product_id=1,
            reorder_point=0,
            target_stock=1,
            lead_time_seconds=10,
        )
    )
    inventory_double.products[1] = {"product_id": 1, "sku": "SKU-1"}
    inventory_double.balances[1] = {"on_hand": 0, "reserved": 0}

    decision = (await simulation.evaluate_replenishment(client, "corr"))[0]
    purchase_order_id = decision["purchase_order"]["purchase_order_id"]
    await simulation.order_purchase_order(purchase_order_id)
    with pytest.raises(ValueError, match="Only created"):
        await simulation.cancel_purchase_order(purchase_order_id)


@pytest.mark.asyncio
async def test_policy_must_target_above_reorder_point():
    simulation = SupplierSimulation()
    with pytest.raises(ValueError, match="greater than reorder_point"):
        await simulation.set_policy(
            ReorderPolicy(
                product_id=1,
                reorder_point=10,
                target_stock=10,
                lead_time_seconds=0,
            )
        )
