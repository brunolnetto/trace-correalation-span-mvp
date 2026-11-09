from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field

SUPPLIER_CATALOG: dict[str, dict[str, Any]] = {
    "BEANS-250": {
        "supplier_sku": "BEANS-250",
        "sku": "COFFEE-BEANS-250G",
        "name": "Whole bean coffee 250g",
        "category": "grocery",
        "unit_price": "24.90",
        "reorder_point": 12,
        "target_stock": 48,
        "lead_time_seconds": 3_600,
    },
    "FILTER-100": {
        "supplier_sku": "FILTER-100",
        "sku": "COFFEE-FILTER-100",
        "name": "Paper coffee filters 100 pack",
        "category": "grocery",
        "unit_price": "12.50",
        "reorder_point": 20,
        "target_stock": 80,
        "lead_time_seconds": 1_800,
    },
    "MUG-350": {
        "supplier_sku": "MUG-350",
        "sku": "CERAMIC-MUG-350",
        "name": "Ceramic coffee mug 350ml",
        "category": "kitchen",
        "unit_price": "32.00",
        "reorder_point": 8,
        "target_stock": 32,
        "lead_time_seconds": 7_200,
    },
}


class ReorderPolicy(BaseModel):
    product_id: int = Field(gt=0)
    reorder_point: int = Field(ge=0)
    target_stock: int = Field(gt=0)
    lead_time_seconds: int = Field(ge=0, le=31_536_000)


class AdvanceClockRequest(BaseModel):
    seconds: int = Field(gt=0, le=31_536_000)


class SupplierSimulation:
    """Process-local supplier simulator; Inventory API remains stock authority.

    Reorder position is available stock plus outstanding purchase-order units:
    on_hand - reserved + on_order. A rule fires when that position is at or
    below its reorder point, and orders enough to reach target_stock.
    """

    def __init__(self, *, initial_time: datetime | None = None) -> None:
        self._now = initial_time or datetime.now(UTC)
        if self._now.tzinfo is None:
            raise ValueError("initial_time must be timezone-aware")
        self._policies: dict[int, dict[str, Any]] = {}
        self._onboarded: dict[str, dict[str, Any]] = {}
        self._purchase_orders: dict[str, dict[str, Any]] = {}
        self._next_policy_version = 1
        self._lock = asyncio.Lock()

    def _now_iso(self) -> str:
        return self._now.isoformat()

    async def clock(self) -> dict[str, Any]:
        async with self._lock:
            return {"now": self._now_iso()}

    async def advance_clock(self, seconds: int) -> dict[str, Any]:
        if seconds <= 0:
            raise ValueError("Clock advance must be positive")
        async with self._lock:
            self._now += timedelta(seconds=seconds)
            return {"now": self._now_iso(), "advanced_seconds": seconds}

    async def list_offerings(self) -> list[dict[str, Any]]:
        return [item.copy() for item in SUPPLIER_CATALOG.values()]

    async def onboard_product(
        self,
        supplier_sku: str,
        client: httpx.AsyncClient,
        correlation_id: str,
    ) -> dict[str, Any]:
        offering = SUPPLIER_CATALOG.get(supplier_sku)
        if offering is None:
            raise KeyError(f"Supplier SKU {supplier_sku} does not exist")

        async with self._lock:
            existing = self._onboarded.get(supplier_sku)
            if existing is not None:
                return existing.copy()

            product_payload = {
                "sku": offering["sku"],
                "name": offering["name"],
                "category": offering["category"],
                "unit_price": offering["unit_price"],
            }
            response = await client.post(
                "/products",
                json=product_payload,
                headers={"X-Correlation-ID": correlation_id},
            )
            if response.status_code == 409:
                # The supplier simulator may have restarted after the catalog
                # write. Recover by SKU instead of duplicating the product.
                products_response = await client.get(
                    "/products",
                    headers={"X-Correlation-ID": correlation_id},
                )
                products_response.raise_for_status()
                product = next(
                    (
                        item
                        for item in products_response.json()
                        if item["sku"] == offering["sku"]
                    ),
                    None,
                )
                if product is None:
                    response.raise_for_status()
                    raise RuntimeError(
                        "Catalog rejected onboarding without a matching SKU"
                    )
            else:
                response.raise_for_status()
                product = response.json()

            product_id = int(product["product_id"])
            policy = {
                "product_id": product_id,
                "reorder_point": offering["reorder_point"],
                "target_stock": offering["target_stock"],
                "lead_time_seconds": offering["lead_time_seconds"],
                "version": self._next_policy_version,
                "updated_at": self._now_iso(),
            }
            self._next_policy_version += 1
            self._policies[product_id] = policy
            result = {
                "supplier_sku": supplier_sku,
                "product": product,
                "reorder_policy": policy.copy(),
                "onboarded_at": self._now_iso(),
            }
            self._onboarded[supplier_sku] = result
            return result.copy()

    async def set_policy(self, policy: ReorderPolicy) -> dict[str, Any]:
        async with self._lock:
            existing = self._policies.get(policy.product_id)
            version = (
                existing["version"] + 1
                if existing is not None
                else self._next_policy_version
            )
            if existing is None:
                self._next_policy_version += 1
            record = {
                **policy.model_dump(),
                "version": version,
                "updated_at": self._now_iso(),
            }
            if record["target_stock"] <= record["reorder_point"]:
                raise ValueError("target_stock must be greater than reorder_point")
            self._policies[policy.product_id] = record
            return record.copy()

    async def list_policies(self) -> list[dict[str, Any]]:
        async with self._lock:
            return [item.copy() for item in self._policies.values()]

    async def evaluate_replenishment(
        self,
        client: httpx.AsyncClient,
        correlation_id: str,
        *,
        product_id: int | None = None,
    ) -> list[dict[str, Any]]:
        async with self._lock:
            policies = [
                item.copy()
                for item in self._policies.values()
                if product_id is None or item["product_id"] == product_id
            ]
            decisions: list[dict[str, Any]] = []
            for policy in policies:
                current_product_id = policy["product_id"]
                response = await client.get(
                    f"/inventory/{current_product_id}",
                    headers={"X-Correlation-ID": correlation_id},
                )
                response.raise_for_status()
                inventory = response.json()
                active_orders = [
                    order
                    for order in self._purchase_orders.values()
                    if order["product_id"] == current_product_id
                    and order["status"] in {"created", "ordered", "receiving"}
                ]
                on_order = sum(order["quantity"] for order in active_orders)
                available = int(inventory["on_hand"]) - int(inventory["reserved"])
                inventory_position = available + on_order
                active_order_ids = [
                    order["purchase_order_id"] for order in active_orders
                ]

                if active_orders:
                    decisions.append(
                        {
                            "product_id": current_product_id,
                            "decision": "open_purchase_order_exists",
                            "on_hand": inventory["on_hand"],
                            "reserved": inventory["reserved"],
                            "available_quantity": available,
                            "on_order": on_order,
                            "inventory_position": inventory_position,
                            "purchase_order_ids": active_order_ids,
                        }
                    )
                    continue

                if inventory_position > policy["reorder_point"]:
                    decisions.append(
                        {
                            "product_id": current_product_id,
                            "decision": "no_reorder",
                            "on_hand": inventory["on_hand"],
                            "reserved": inventory["reserved"],
                            "available_quantity": available,
                            "on_order": on_order,
                            "inventory_position": inventory_position,
                            "purchase_order_ids": [],
                        }
                    )
                    continue

                quantity = policy["target_stock"] - inventory_position
                if quantity <= 0:
                    decisions.append(
                        {
                            "product_id": current_product_id,
                            "decision": "no_reorder",
                            "on_hand": inventory["on_hand"],
                            "reserved": inventory["reserved"],
                            "available_quantity": available,
                            "on_order": on_order,
                            "inventory_position": inventory_position,
                            "purchase_order_ids": [],
                        }
                    )
                    continue

                purchase_order_id = str(uuid4())
                order = {
                    "purchase_order_id": purchase_order_id,
                    "product_id": current_product_id,
                    "quantity": quantity,
                    "status": "created",
                    "reorder_point": policy["reorder_point"],
                    "target_stock": policy["target_stock"],
                    "inventory_position_at_order": inventory_position,
                    "on_hand_at_order": int(inventory["on_hand"]),
                    "reserved_at_order": int(inventory["reserved"]),
                    "on_order_at_order": on_order,
                    "lead_time_seconds": policy["lead_time_seconds"],
                    "created_at": self._now_iso(),
                    "ordered_at": None,
                    "due_at": None,
                    "receiving_started_at": None,
                    "received_at": None,
                    "receipt_id": None,
                    "policy_version": policy["version"],
                }
                self._purchase_orders[purchase_order_id] = order
                decisions.append(
                    {
                        "product_id": current_product_id,
                        "decision": "purchase_order_created",
                        "on_hand": inventory["on_hand"],
                        "reserved": inventory["reserved"],
                        "available_quantity": available,
                        "on_order": on_order,
                        "inventory_position": inventory_position,
                        "purchase_order": order.copy(),
                    }
                )
            return decisions

    async def list_purchase_orders(self) -> list[dict[str, Any]]:
        async with self._lock:
            return [item.copy() for item in self._purchase_orders.values()]

    async def get_purchase_order(self, purchase_order_id: str) -> dict[str, Any]:
        async with self._lock:
            order = self._purchase_orders.get(purchase_order_id)
            if order is None:
                raise KeyError(f"Purchase order {purchase_order_id} does not exist")
            return order.copy()

    async def order_purchase_order(self, purchase_order_id: str) -> dict[str, Any]:
        async with self._lock:
            order = self._purchase_orders.get(purchase_order_id)
            if order is None:
                raise KeyError(f"Purchase order {purchase_order_id} does not exist")
            if order["status"] == "ordered":
                return order.copy()
            if order["status"] != "created":
                raise ValueError("Only created purchase orders can be ordered")
            now = self._now
            order["status"] = "ordered"
            order["ordered_at"] = now.isoformat()
            order["due_at"] = (
                now + timedelta(seconds=order["lead_time_seconds"])
            ).isoformat()
            return order.copy()

    async def receive_purchase_order(
        self,
        purchase_order_id: str,
        client: httpx.AsyncClient,
        correlation_id: str,
    ) -> dict[str, Any]:
        async with self._lock:
            order = self._purchase_orders.get(purchase_order_id)
            if order is None:
                raise KeyError(f"Purchase order {purchase_order_id} does not exist")
            if order["status"] == "received":
                return order.copy()
            if order["status"] == "ordered":
                due_at = datetime.fromisoformat(order["due_at"])
                if self._now < due_at:
                    raise ValueError("Supplier lead time has not elapsed")
                order["status"] = "receiving"
                order["receiving_started_at"] = self._now_iso()
            elif order["status"] != "receiving":
                raise ValueError(
                    "Only ordered or receiving purchase orders can be received"
                )
            product_id = order["product_id"]
            quantity = order["quantity"]

        # The stable Inventory idempotency key makes this safe to retry after
        # the stock commit succeeds but the response or supplier checkpoint is lost.
        response = await client.post(
            f"/inventory/{product_id}/receipts",
            json={
                "quantity": quantity,
                "idempotency_key": f"supplier-po:{purchase_order_id}",
                "reference": purchase_order_id,
            },
            headers={"X-Correlation-ID": correlation_id},
        )
        response.raise_for_status()
        receipt = response.json()

        async with self._lock:
            order = self._purchase_orders[purchase_order_id]
            if order["status"] == "received":
                return order.copy()
            if order["status"] != "receiving":
                raise RuntimeError("Purchase order left receiving state unexpectedly")
            order["status"] = "received"
            order["received_at"] = self._now_iso()
            order["receipt_id"] = receipt["receipt_id"]
            return order.copy()

    async def cancel_purchase_order(
        self, purchase_order_id: str
    ) -> dict[str, Any]:
        async with self._lock:
            order = self._purchase_orders.get(purchase_order_id)
            if order is None:
                raise KeyError(f"Purchase order {purchase_order_id} does not exist")
            if order["status"] == "cancelled":
                return order.copy()
            if order["status"] != "created":
                raise ValueError("Only created purchase orders can be cancelled")
            order["status"] = "cancelled"
            order["cancelled_at"] = self._now_iso()
            return order.copy()

    async def reset(self) -> None:
        async with self._lock:
            self._policies.clear()
            self._onboarded.clear()
            self._purchase_orders.clear()
            self._next_policy_version = 1
