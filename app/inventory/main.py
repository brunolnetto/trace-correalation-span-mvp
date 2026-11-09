from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Security, status
from opentelemetry import trace

from app.inventory.postgres_store import PostgresInventoryStore
from app.inventory.store import (
    InventoryAdjustmentRequest,
    InventoryReceiptRequest,
    InventoryReservationRequest,
    InventoryStore,
    ProductCreate,
    ProductUpdate,
)
from app.observability import configure_observability
from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager
from app.persistence.ports import InventoryStorePort
from app.security import authenticate_request

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("inventory-api")
seed_demo_data = os.getenv("INVENTORY_SEED_DEMO_DATA", "false").lower() in {
    "1",
    "true",
    "yes",
}
inventory_store: InventoryStorePort = InventoryStore(
    seed_demo_data=seed_demo_data
)
database_manager = DatabaseManager(pool_name="inventory-api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global inventory_store

    settings = DatabaseSettings.from_env()
    await database_manager.open(settings)
    try:
        if settings.backend == "postgres":
            postgres_store = PostgresInventoryStore(database_manager)
            if seed_demo_data:
                await postgres_store.seed_demo_data()
            inventory_store = postgres_store
        else:
            inventory_store = InventoryStore(seed_demo_data=seed_demo_data)

        app.state.inventory_store = inventory_store
        yield
    finally:
        await database_manager.close()


app = FastAPI(
    title="Inventory API",
    description="Product catalog and inventory lifecycle with correlated traces.",
    version="0.2.0",
    lifespan=lifespan,
)
configure_observability(app, os.getenv("SERVICE_NAME", "inventory-api"))


@app.get("/healthz", tags=["health"])
async def healthcheck():
    if not await database_manager.healthy():
        raise HTTPException(
            status_code=503,
            detail="Database unavailable",
        )
    return {"status": "ok", "service": "inventory-api"}


@app.post("/products", status_code=status.HTTP_201_CREATED, tags=["catalog"], dependencies=[Security(authenticate_request, scopes=["catalog:write"])])
async def create_product(request: ProductCreate):
    try:
        product = await inventory_store.create_product(request)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None

    logger.info(
        "product.created",
        extra={
            "product_id": product["product_id"],
            "sku": product["sku"],
            "category": product["category"],
        },
    )
    return product


@app.get("/products", tags=["catalog"], dependencies=[Security(authenticate_request, scopes=["catalog:read"])])
async def list_products(include_inactive: bool = False):
    return await inventory_store.list_products(include_inactive=include_inactive)


@app.get("/products/{product_id}", tags=["catalog"], dependencies=[Security(authenticate_request, scopes=["catalog:read"])])
async def get_product(product_id: int):
    try:
        return await inventory_store.get_product(product_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Product not found") from None


@app.patch("/products/{product_id}", tags=["catalog"], dependencies=[Security(authenticate_request, scopes=["catalog:write"])])
async def update_product(product_id: int, request: ProductUpdate):
    try:
        product = await inventory_store.update_product(product_id, request)
    except KeyError:
        raise HTTPException(status_code=404, detail="Product not found") from None
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None

    logger.info(
        "product.updated",
        extra={
            "product_id": product_id,
            "is_active": product["is_active"],
        },
    )
    return product


@app.get("/inventory/{product_id}", tags=["inventory"], dependencies=[Security(authenticate_request, scopes=["inventory:read"])])
async def get_inventory(product_id: int):
    with tracer.start_as_current_span("inventory.lookup") as span:
        span.set_attribute("inventory.product_id", product_id)
        try:
            inventory = await inventory_store.inventory_snapshot(product_id)
        except KeyError:
            raise HTTPException(
                status_code=404, detail="Product not found"
            ) from None

        logger.info(
            "inventory.lookup.completed",
            extra={
                "product_id": product_id,
                "on_hand": inventory["on_hand"],
                "reserved": inventory["reserved"],
                "available_quantity": inventory["available_quantity"],
            },
        )
        return inventory


@app.post(
    "/inventory/{product_id}/receipts",
    status_code=status.HTTP_201_CREATED,
    tags=["inventory"],
    dependencies=[Security(authenticate_request, scopes=["inventory:receive"])],
)
async def receive_inventory(product_id: int, request: InventoryReceiptRequest):
    with tracer.start_as_current_span("inventory.receive") as span:
        span.set_attribute("inventory.product_id", product_id)
        span.set_attribute("inventory.quantity", request.quantity)
        try:
            receipt = await inventory_store.receive_stock(product_id, request)
        except KeyError:
            raise HTTPException(
                status_code=404, detail="Product not found"
            ) from None
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None

        logger.info(
            "inventory.stock.received",
            extra={
                "product_id": product_id,
                "receipt_id": receipt["receipt_id"],
                "quantity": request.quantity,
                "idempotency_key": request.idempotency_key,
                "movement_type": "receipt",
                "reference": request.reference,
            },
        )
        return receipt


@app.post(
    "/inventory/{product_id}/adjustments",
    status_code=status.HTTP_201_CREATED,
    tags=["inventory"],
    dependencies=[Security(authenticate_request, scopes=["inventory:adjust"])],
)
async def adjust_inventory(
    product_id: int, request: InventoryAdjustmentRequest
):
    with tracer.start_as_current_span("inventory.adjust") as span:
        span.set_attribute("inventory.product_id", product_id)
        span.set_attribute("inventory.quantity_delta", request.quantity_delta)
        try:
            movement = await inventory_store.adjust_stock(product_id, request)
        except KeyError:
            raise HTTPException(
                status_code=404, detail="Product not found"
            ) from None
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None

        logger.info(
            "inventory.stock.adjusted",
            extra={
                "product_id": product_id,
                "movement_id": movement["movement_id"],
                "quantity_delta": request.quantity_delta,
                "reason": request.reason,
                "idempotency_key": request.idempotency_key,
                "movement_type": "adjustment",
            },
        )
        return movement


@app.get("/inventory/{product_id}/movements", tags=["inventory"], dependencies=[Security(authenticate_request, scopes=["inventory:read"])])
async def list_inventory_movements(
    product_id: int,
    limit: int = Query(default=100, ge=1, le=500),
):
    try:
        return await inventory_store.list_movements(product_id, limit=limit)
    except KeyError:
        raise HTTPException(status_code=404, detail="Product not found") from None


@app.post("/inventory/{product_id}/reservations", tags=["inventory"], dependencies=[Security(authenticate_request, scopes=["inventory:reserve"])])
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
        except KeyError:
            raise HTTPException(status_code=404, detail="Product not found") from None
        except ValueError as exc:
            logger.warning(
                "inventory.reservation.rejected",
                extra={
                    "product_id": product_id,
                    "order_id": request.order_id,
                    "saga_id": request.saga_id,
                    "reason": str(exc),
                    "quantity": request.quantity,
                    "movement_type": "reservation_rejected",
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
                "movement_type": "reservation_created",
            },
        )
        return reservation


@app.post(
    "/inventory/reservations/{reservation_id}/commit",
    tags=["inventory"],
    dependencies=[Security(authenticate_request, scopes=["inventory:fulfill"])],
)
async def commit_inventory_reservation(reservation_id: str):
    with tracer.start_as_current_span("inventory.commit") as span:
        span.set_attribute("inventory.reservation_id", reservation_id)
        try:
            reservation = await inventory_store.commit(reservation_id)
        except KeyError:
            raise HTTPException(
                status_code=404, detail="Reservation not found"
            ) from None
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None

        logger.info(
            "inventory.reservation.committed",
            extra={
                "reservation_id": reservation_id,
                "product_id": reservation.get("product_id"),
                "order_id": reservation.get("order_id"),
                "saga_id": reservation.get("saga_id"),
                "quantity": reservation.get("quantity"),
                "status": reservation["status"],
                "movement_type": "reservation_committed",
            },
        )
        return reservation


@app.delete(
    "/inventory/reservations/{reservation_id}",
    tags=["inventory"],
    dependencies=[Security(authenticate_request, scopes=["inventory:reserve"])],
)
async def release_inventory(reservation_id: str):
    with tracer.start_as_current_span("inventory.release") as span:
        span.set_attribute("inventory.reservation_id", reservation_id)
        reservation = await inventory_store.release(reservation_id)
        logger.info(
            "inventory.reservation.released",
            extra={
                "reservation_id": reservation_id,
                "product_id": reservation.get("product_id"),
                "order_id": reservation.get("order_id"),
                "saga_id": reservation.get("saga_id"),
                "quantity": reservation.get("quantity"),
                "status": reservation["status"],
                "movement_type": (
                    "reservation_released"
                    if reservation["status"] == "released"
                    else reservation["status"]
                ),
            },
        )
        return reservation
