from __future__ import annotations

import logging
import os

from fastapi import FastAPI, HTTPException
from opentelemetry import trace

from app.inventory.store import InventoryReservationRequest, InventoryStore
from app.observability import configure_observability

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("inventory-api")
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
