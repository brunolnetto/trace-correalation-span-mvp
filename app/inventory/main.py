from __future__ import annotations

import asyncio
import logging
import os
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from opentelemetry import trace

from app.observability import configure_observability

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("inventory-api")

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


inventory_store = InventoryStore()

app = FastAPI(
    title="Inventory API",
    description="Inventory service demonstrating downstream trace context.",
    version="0.1.0",
)
configure_observability(app, os.getenv("SERVICE_NAME", "inventory-api"))


@app.get("/healthz", tags=["health"])
async def healthcheck():
    return {"status": "ok", "service": "inventory-api"}


@app.get("/inventory/{product_id}", tags=["inventory"])
async def get_inventory(product_id: int):
    with tracer.start_as_current_span("inventory.lookup") as span:
        span.set_attribute("inventory.product_id", product_id)
        quantity = await inventory_store.available_quantity(product_id)
        logger.info(
            "inventory.lookup.completed",
            extra={
                "product_id": product_id,
                "available_quantity": quantity,
            },
        )
        return {
            "product_id": product_id,
            "available": quantity > 0,
            "quantity": quantity,
        }


@app.post("/inventory/{product_id}/reservations", tags=["inventory"])
async def reserve_inventory(
    product_id: int, request: InventoryReservationRequest
):
    with tracer.start_as_current_span("inventory.reserve") as span:
        span.set_attributes(
            {
                "inventory.product_id": product_id,
                "inventory.quantity": request.quantity,
                "saga.id": request.saga_id,
            }
        )
        try:
            reservation = await inventory_store.reserve(product_id, request)
        except ValueError as exc:
            logger.warning(
                "inventory.reservation.rejected",
                extra={
                    "product_id": product_id,
                    "order_id": request.order_id,
                    "saga_id": request.saga_id,
                    "reason": str(exc),
                },
            )
            raise HTTPException(status_code=409, detail=str(exc)) from None

        logger.info(
            "inventory.reservation.created",
            extra={
                "product_id": product_id,
                "order_id": request.order_id,
                "saga_id": request.saga_id,
                "reservation_id": reservation["reservation_id"],
                "quantity": request.quantity,
            },
        )
        return reservation


@app.delete(
    "/inventory/reservations/{reservation_id}",
    tags=["inventory"],
)
async def release_inventory(reservation_id: str):
    with tracer.start_as_current_span("inventory.release") as span:
        span.set_attribute("inventory.reservation_id", reservation_id)
        reservation = await inventory_store.release(reservation_id)
        logger.info(
            "inventory.reservation.released",
            extra={
                "reservation_id": reservation_id,
                "status": reservation["status"],
            },
        )
        return reservation
