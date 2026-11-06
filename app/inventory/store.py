from __future__ import annotations

import asyncio
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, Field

DEFAULT_STOCK = 42


class InventoryReservationRequest(BaseModel):
    order_id: int
    saga_id: str
    quantity: int = Field(gt=0)


class InventoryStore:
    """Process-local inventory and reservation state for the MVP."""

    def __init__(self) -> None:
        self._available: dict[int, int] = {}
        self._reservations: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def available_quantity(self, product_id: int) -> int:
        async with self._lock:
            return self._available.get(product_id, DEFAULT_STOCK)

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

            available = self._available.get(product_id, DEFAULT_STOCK)
            if request.quantity > available:
                raise ValueError("Insufficient inventory")

            self._available[product_id] = available - request.quantity
            reservation = {
                "reservation_id": reservation_id,
                "order_id": request.order_id,
                "product_id": product_id,
                "saga_id": request.saga_id,
                "quantity": request.quantity,
                "status": "reserved",
            }
            self._reservations[reservation_id] = reservation
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
                self._available[product_id] = (
                    self._available.get(product_id, DEFAULT_STOCK)
                    + reservation["quantity"]
                )
                reservation["status"] = "released"

            return reservation.copy()

    async def reset(self) -> None:
        """Reset process-local state; intended for deterministic tests."""
        async with self._lock:
            self._available.clear()
            self._reservations.clear()
