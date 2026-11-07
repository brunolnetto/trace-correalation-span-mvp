from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import BaseModel, Field

DEMO_PRODUCT_ID = 7
DEMO_STOCK = 42


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProductCreate(BaseModel):
    sku: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    name: str = Field(min_length=1, max_length=120)
    category: str = Field(min_length=1, max_length=80)
    unit_price: Decimal = Field(gt=0, max_digits=12, decimal_places=2)


class ProductUpdate(BaseModel):
    sku: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    )
    name: str | None = Field(default=None, min_length=1, max_length=120)
    category: str | None = Field(default=None, min_length=1, max_length=80)
    unit_price: Decimal | None = Field(
        default=None, gt=0, max_digits=12, decimal_places=2
    )
    is_active: bool | None = None


class InventoryReservationRequest(BaseModel):
    order_id: int
    saga_id: str = Field(min_length=1, max_length=128)
    quantity: int = Field(gt=0)


class InventoryReceiptRequest(BaseModel):
    quantity: int = Field(gt=0)
    idempotency_key: str = Field(min_length=1, max_length=128)
    reference: str | None = Field(default=None, max_length=128)


class InventoryAdjustmentRequest(BaseModel):
    quantity_delta: int = Field(ne=0)
    reason: str = Field(min_length=1, max_length=256)
    idempotency_key: str = Field(min_length=1, max_length=128)


class InventoryStore:
    """Single-process catalog, inventory balances, reservations and movement ledger.

    The asyncio lock protects operations only within this process. It is not a
    substitute for transactional storage when the API is replicated.
    """

    def __init__(self, *, seed_demo_data: bool = False) -> None:
        self._products: dict[int, dict[str, Any]] = {}
        self._sku_index: dict[str, int] = {}
        self._balances: dict[int, dict[str, int]] = {}
        self._reservations: dict[str, dict[str, Any]] = {}
        self._receipts: dict[str, dict[str, Any]] = {}
        self._adjustments: dict[str, dict[str, Any]] = {}
        self._movements: list[dict[str, Any]] = []
        self._next_product_id = 1
        self._lock = asyncio.Lock()

        if seed_demo_data:
            self._seed_demo_product()

    def _seed_demo_product(self) -> None:
        product = {
            "product_id": DEMO_PRODUCT_ID,
            "sku": "DEMO-007",
            "name": "Demo product",
            "category": "demo",
            "unit_price": Decimal("99.90"),
            "is_active": True,
            "created_at": utc_now(),
            "updated_at": utc_now(),
        }
        self._products[DEMO_PRODUCT_ID] = product
        self._sku_index[product["sku"]] = DEMO_PRODUCT_ID
        self._balances[DEMO_PRODUCT_ID] = {
            "on_hand": DEMO_STOCK,
            "reserved": 0,
        }
        self._next_product_id = DEMO_PRODUCT_ID + 1
        self._append_movement_locked(
            product_id=DEMO_PRODUCT_ID,
            movement_type="opening_balance",
            quantity_delta_on_hand=DEMO_STOCK,
            quantity_delta_reserved=0,
            reference="demo-seed",
        )

    def _append_movement_locked(
        self,
        *,
        product_id: int,
        movement_type: str,
        quantity_delta_on_hand: int,
        quantity_delta_reserved: int,
        reference: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        movement = {
            "movement_id": str(uuid4()),
            "product_id": product_id,
            "movement_type": movement_type,
            "quantity_delta_on_hand": quantity_delta_on_hand,
            "quantity_delta_reserved": quantity_delta_reserved,
            "reference": reference,
            "idempotency_key": idempotency_key,
            "occurred_at": utc_now(),
        }
        self._movements.append(movement)
        return movement.copy()

    def _require_product_locked(self, product_id: int) -> dict[str, Any]:
        product = self._products.get(product_id)
        if product is None:
            raise KeyError(f"Product {product_id} does not exist")
        return product

    def _snapshot_locked(self, product_id: int) -> dict[str, Any]:
        self._require_product_locked(product_id)
        balance = self._balances[product_id]
        available = balance["on_hand"] - balance["reserved"]
        return {
            "product_id": product_id,
            "on_hand": balance["on_hand"],
            "reserved": balance["reserved"],
            "available": available,
            "quantity": available,
            "is_available": available > 0,
        }

    async def create_product(self, request: ProductCreate) -> dict[str, Any]:
        sku = request.sku.strip().upper()
        name = request.name.strip()
        category = request.category.strip()
        if not sku or not name or not category:
            raise ValueError("Product fields cannot contain only whitespace")

        async with self._lock:
            if sku in self._sku_index:
                raise ValueError(f"SKU {sku} already exists")

            product_id = self._next_product_id
            self._next_product_id += 1
            now = utc_now()
            product = {
                "product_id": product_id,
                "sku": sku,
                "name": name,
                "category": category,
                "unit_price": request.unit_price,
                "is_active": True,
                "created_at": now,
                "updated_at": now,
            }
            self._products[product_id] = product
            self._sku_index[sku] = product_id
            self._balances[product_id] = {"on_hand": 0, "reserved": 0}
            return product.copy()

    async def list_products(
        self, *, include_inactive: bool = False
    ) -> list[dict[str, Any]]:
        async with self._lock:
            products = [
                product.copy()
                for product in self._products.values()
                if include_inactive or product["is_active"]
            ]
            return sorted(products, key=lambda product: product["product_id"])

    async def get_product(self, product_id: int) -> dict[str, Any]:
        async with self._lock:
            return self._require_product_locked(product_id).copy()

    async def update_product(
        self, product_id: int, request: ProductUpdate
    ) -> dict[str, Any]:
        changes = request.model_dump(exclude_unset=True)
        if any(value is None for value in changes.values()):
            raise ValueError("Product fields cannot be null")

        async with self._lock:
            product = self._require_product_locked(product_id)
            if "sku" in changes:
                sku = changes["sku"].strip().upper()
                if not sku:
                    raise ValueError("SKU cannot contain only whitespace")
                owner = self._sku_index.get(sku)
                if owner is not None and owner != product_id:
                    raise ValueError(f"SKU {sku} already exists")
                self._sku_index.pop(product["sku"], None)
                self._sku_index[sku] = product_id
                changes["sku"] = sku

            for field in ("name", "category"):
                if field in changes:
                    changes[field] = changes[field].strip()
                    if not changes[field]:
                        raise ValueError(f"{field} cannot contain only whitespace")

            product.update(changes)
            product["updated_at"] = utc_now()
            return product.copy()

    async def inventory_snapshot(self, product_id: int) -> dict[str, Any]:
        async with self._lock:
            return self._snapshot_locked(product_id)

    async def available_quantity(self, product_id: int) -> int:
        snapshot = await self.inventory_snapshot(product_id)
        return snapshot["available"]

    async def receive_stock(
        self, product_id: int, request: InventoryReceiptRequest
    ) -> dict[str, Any]:
        receipt_key = f"{product_id}:{request.idempotency_key}"
        async with self._lock:
            self._require_product_locked(product_id)
            existing = self._receipts.get(receipt_key)
            if existing is not None:
                same_request = (
                    existing["quantity"] == request.quantity
                    and existing["reference"] == request.reference
                )
                if not same_request:
                    raise ValueError("Receipt idempotency key was reused")
                return existing.copy()

            self._balances[product_id]["on_hand"] += request.quantity
            movement = self._append_movement_locked(
                product_id=product_id,
                movement_type="receipt",
                quantity_delta_on_hand=request.quantity,
                quantity_delta_reserved=0,
                reference=request.reference,
                idempotency_key=request.idempotency_key,
            )
            receipt = {
                "receipt_id": movement["movement_id"],
                "product_id": product_id,
                "quantity": request.quantity,
                "reference": request.reference,
                "idempotency_key": request.idempotency_key,
                "status": "received",
            }
            self._receipts[receipt_key] = receipt
            return receipt.copy()

    async def adjust_stock(
        self, product_id: int, request: InventoryAdjustmentRequest
    ) -> dict[str, Any]:
        adjustment_key = f"{product_id}:{request.idempotency_key}"
        async with self._lock:
            self._require_product_locked(product_id)
            existing = self._adjustments.get(adjustment_key)
            if existing is not None:
                same_request = (
                    existing["quantity_delta_on_hand"] == request.quantity_delta
                    and existing["reference"] == request.reason
                )
                if not same_request:
                    raise ValueError("Adjustment idempotency key was reused")
                return existing.copy()

            balance = self._balances[product_id]
            adjusted_on_hand = balance["on_hand"] + request.quantity_delta
            if adjusted_on_hand < 0:
                raise ValueError("Adjustment cannot make on-hand stock negative")
            if adjusted_on_hand < balance["reserved"]:
                raise ValueError(
                    "Adjustment cannot reduce on-hand below reserved quantity"
                )

            balance["on_hand"] = adjusted_on_hand
            movement = self._append_movement_locked(
                product_id=product_id,
                movement_type="adjustment",
                quantity_delta_on_hand=request.quantity_delta,
                quantity_delta_reserved=0,
                reference=request.reason,
                idempotency_key=request.idempotency_key,
            )
            self._adjustments[adjustment_key] = movement
            return movement.copy()

    async def reserve(
        self, product_id: int, request: InventoryReservationRequest
    ) -> dict[str, Any]:
        reservation_id = str(
            uuid5(
                NAMESPACE_URL,
                f"inventory-reservation:{request.saga_id}:{product_id}",
            )
        )

        async with self._lock:
            product = self._require_product_locked(product_id)
            existing = self._reservations.get(reservation_id)
            if existing is not None:
                same_request = (
                    existing["order_id"] == request.order_id
                    and existing["product_id"] == product_id
                    and existing["quantity"] == request.quantity
                )
                if not same_request or existing["status"] != "reserved":
                    raise ValueError("Reservation idempotency key is already consumed")
                return existing.copy()

            if not product["is_active"]:
                raise ValueError("Product is inactive")

            balance = self._balances[product_id]
            available = balance["on_hand"] - balance["reserved"]
            if request.quantity > available:
                raise ValueError("Insufficient inventory")

            balance["reserved"] += request.quantity
            reservation = {
                "reservation_id": reservation_id,
                "order_id": request.order_id,
                "product_id": product_id,
                "saga_id": request.saga_id,
                "quantity": request.quantity,
                "status": "reserved",
            }
            self._reservations[reservation_id] = reservation
            self._append_movement_locked(
                product_id=product_id,
                movement_type="reservation_created",
                quantity_delta_on_hand=0,
                quantity_delta_reserved=request.quantity,
                reference=reservation_id,
            )
            return reservation.copy()

    async def release(self, reservation_id: str) -> dict[str, Any]:
        async with self._lock:
            reservation = self._reservations.get(reservation_id)
            if reservation is None:
                return {
                    "reservation_id": reservation_id,
                    "status": "not_found",
                }

            if reservation["status"] == "reserved":
                product_id = reservation["product_id"]
                self._balances[product_id]["reserved"] -= reservation["quantity"]
                reservation["status"] = "released"
                self._append_movement_locked(
                    product_id=product_id,
                    movement_type="reservation_released",
                    quantity_delta_on_hand=0,
                    quantity_delta_reserved=-reservation["quantity"],
                    reference=reservation_id,
                )

            return reservation.copy()

    async def commit(self, reservation_id: str) -> dict[str, Any]:
        async with self._lock:
            reservation = self._reservations.get(reservation_id)
            if reservation is None:
                raise KeyError(f"Reservation {reservation_id} does not exist")
            if reservation["status"] == "committed":
                return reservation.copy()
            if reservation["status"] != "reserved":
                raise ValueError("Only active reservations can be committed")

            product_id = reservation["product_id"]
            quantity = reservation["quantity"]
            balance = self._balances[product_id]
            if balance["on_hand"] < quantity or balance["reserved"] < quantity:
                raise ValueError("Inventory balance is inconsistent")

            balance["on_hand"] -= quantity
            balance["reserved"] -= quantity
            reservation["status"] = "committed"
            self._append_movement_locked(
                product_id=product_id,
                movement_type="reservation_committed",
                quantity_delta_on_hand=-quantity,
                quantity_delta_reserved=-quantity,
                reference=reservation_id,
            )
            return reservation.copy()

    async def list_movements(
        self, product_id: int, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        async with self._lock:
            self._require_product_locked(product_id)
            return [
                movement.copy()
                for movement in reversed(self._movements)
                if movement["product_id"] == product_id
            ][:limit]

    async def reset(self) -> None:
        """Clear all process-local state; intended for deterministic tests."""
        async with self._lock:
            self._products.clear()
            self._sku_index.clear()
            self._balances.clear()
            self._reservations.clear()
            self._receipts.clear()
            self._adjustments.clear()
            self._movements.clear()
            self._next_product_id = 1
