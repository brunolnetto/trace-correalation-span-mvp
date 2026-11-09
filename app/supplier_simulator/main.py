from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query

from app.observability import configure_observability, correlation_id_context
from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager
from app.supplier_simulator.postgres_store import PostgresSupplierSimulation
from app.supplier_simulator.store import (
    AdvanceClockRequest,
    ReorderPolicy,
    SupplierSimulation,
)

logger = logging.getLogger(__name__)
app = FastAPI(
    title="Supplier Simulation API",
    description="Deterministic supplier catalog and inventory replenishment simulator.",
    version="0.2.0",
    lifespan=lifespan,
)
configure_observability(app, os.getenv("SERVICE_NAME", "supplier-simulator"))

INVENTORY_API_URL = os.getenv(
    "INVENTORY_API_URL", "http://localhost:8001"
)
simulation: SupplierSimulation | PostgresSupplierSimulation = SupplierSimulation()
database_manager = DatabaseManager(pool_name="supplier-simulator")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global simulation

    settings = DatabaseSettings.from_env()
    await database_manager.open(settings)
    try:
        if settings.backend == "postgres":
            persistent_simulation = PostgresSupplierSimulation(database_manager)
            await persistent_simulation.open()
            simulation = persistent_simulation
        else:
            simulation = SupplierSimulation()

        app.state.supplier_simulation = simulation
        yield
    finally:
        await database_manager.close()


def inventory_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=INVENTORY_API_URL, timeout=5.0)


def correlation_id() -> str:
    return correlation_id_context.get() or ""


def map_inventory_error(exc: httpx.HTTPError) -> HTTPException:
    if isinstance(exc, httpx.HTTPStatusError):
        return HTTPException(
            status_code=502,
            detail={
                "message": "Inventory API rejected the supplier operation.",
                "inventory_status": exc.response.status_code,
            },
        )
    return HTTPException(
        status_code=503,
        detail="Inventory API is temporarily unavailable.",
    )


@app.get("/healthz", tags=["health"])
async def healthcheck():
    if not await database_manager.healthy():
        raise HTTPException(status_code=503, detail="Database unavailable")
    return {"status": "ok", "service": "supplier-simulator"}


@app.get("/supplier-catalog", tags=["supplier"])
async def list_supplier_offerings():
    return await simulation.list_offerings()


@app.post("/supplier-catalog/{supplier_sku}/onboard", tags=["supplier"])
async def onboard_supplier_product(supplier_sku: str):
    try:
        async with inventory_client() as client:
            result = await simulation.onboard_product(
                supplier_sku, client, correlation_id()
            )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    except (httpx.HTTPError, RuntimeError) as exc:
        if isinstance(exc, httpx.HTTPError):
            raise map_inventory_error(exc) from None
        raise HTTPException(status_code=502, detail=str(exc)) from None

    logger.info(
        "supplier.product.onboarded",
        extra={
            "supplier_sku": supplier_sku,
            "product_id": result["product"]["product_id"],
            "policy_version": result["reorder_policy"]["version"],
        },
    )
    return result


@app.get("/reorder-policies", tags=["replenishment"])
async def list_reorder_policies():
    return await simulation.list_policies()


@app.put("/reorder-policies", tags=["replenishment"])
async def upsert_reorder_policy(policy: ReorderPolicy):
    try:
        async with inventory_client() as client:
            response = await client.get(
                f"/products/{policy.product_id}",
                headers={"X-Correlation-ID": correlation_id()},
            )
            response.raise_for_status()
        result = await simulation.set_policy(policy)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Product not found.") from None
        raise map_inventory_error(exc) from None
    except httpx.HTTPError as exc:
        raise map_inventory_error(exc) from None
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None

    logger.info(
        "supplier.reorder_policy.updated",
        extra={
            "product_id": policy.product_id,
            "reorder_point": policy.reorder_point,
            "target_stock": policy.target_stock,
            "policy_version": result["version"],
        },
    )
    return result


@app.post("/replenishments/evaluate", tags=["replenishment"])
async def evaluate_replenishment(
    product_id: int | None = Query(default=None, gt=0)
):
    try:
        async with inventory_client() as client:
            decisions = await simulation.evaluate_replenishment(
                client, correlation_id(), product_id=product_id
            )
    except httpx.HTTPError as exc:
        raise map_inventory_error(exc) from None

    for decision in decisions:
        logger.info(
            "supplier.replenishment.evaluated",
            extra={
                "product_id": decision["product_id"],
                "decision": decision["decision"],
                "inventory_position": decision["inventory_position"],
                "purchase_order_id": (
                    decision.get("purchase_order", {}).get("purchase_order_id")
                ),
                "quantity": decision.get("purchase_order", {}).get("quantity"),
                "reorder_point": decision.get(
                    "purchase_order", {}
                ).get("reorder_point"),
                "target_stock": decision.get(
                    "purchase_order", {}
                ).get("target_stock"),
                "lead_time_seconds": decision.get(
                    "purchase_order", {}
                ).get("lead_time_seconds"),
                "policy_version": decision.get(
                    "purchase_order", {}
                ).get("policy_version"),
            },
        )
    return {"decisions": decisions}


@app.get("/purchase-orders", tags=["supplier"])
async def list_purchase_orders():
    return await simulation.list_purchase_orders()


@app.get("/purchase-orders/{purchase_order_id}", tags=["supplier"])
async def get_purchase_order(purchase_order_id: str):
    try:
        return await simulation.get_purchase_order(purchase_order_id)
    except KeyError:
        raise HTTPException(
            status_code=404, detail="Purchase order not found."
        ) from None


@app.get(
    "/purchase-orders/{purchase_order_id}/transitions",
    tags=["supplier"],
)
async def list_purchase_order_transitions(purchase_order_id: str):
    try:
        return await simulation.list_purchase_order_transitions(purchase_order_id)
    except KeyError:
        raise HTTPException(
            status_code=404, detail="Purchase order not found."
        ) from None


@app.post(
    "/purchase-orders/{purchase_order_id}/order",
    tags=["supplier"],
)
async def order_purchase_order(purchase_order_id: str):
    try:
        result = await simulation.order_purchase_order(
            purchase_order_id, correlation_id()
        )
    except KeyError:
        raise HTTPException(
            status_code=404, detail="Purchase order not found."
        ) from None
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None

    logger.info(
        "supplier.purchase_order.ordered",
        extra={
            "purchase_order_id": purchase_order_id,
            "product_id": result["product_id"],
            "quantity": result["quantity"],
            "status": result["status"],
            "due_at": result["due_at"],
            "lead_time_seconds": result["lead_time_seconds"],
        },
    )
    return result


@app.post(
    "/purchase-orders/{purchase_order_id}/receive",
    tags=["supplier"],
)
async def receive_purchase_order(purchase_order_id: str):
    try:
        async with inventory_client() as client:
            result = await simulation.receive_purchase_order(
                purchase_order_id, client, correlation_id()
            )
    except KeyError:
        raise HTTPException(
            status_code=404, detail="Purchase order not found."
        ) from None
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except httpx.HTTPError as exc:
        raise map_inventory_error(exc) from None

    logger.info(
        "supplier.purchase_order.received",
        extra={
            "purchase_order_id": purchase_order_id,
            "product_id": result["product_id"],
            "quantity": result["quantity"],
            "status": result["status"],
            "due_at": result["due_at"],
            "received_at": result["received_at"],
            "receipt_id": result["receipt_id"],
        },
    )
    return result


@app.post(
    "/purchase-orders/{purchase_order_id}/cancel",
    tags=["supplier"],
)
async def cancel_purchase_order(purchase_order_id: str):
    try:
        result = await simulation.cancel_purchase_order(
            purchase_order_id, correlation_id()
        )
    except KeyError:
        raise HTTPException(
            status_code=404, detail="Purchase order not found."
        ) from None
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None

    logger.info(
        "supplier.purchase_order.cancelled",
        extra={
            "purchase_order_id": purchase_order_id,
            "product_id": result["product_id"],
            "quantity": result["quantity"],
            "status": result["status"],
        },
    )
    return result


@app.get("/clock", tags=["simulation"])
async def get_simulation_clock():
    return await simulation.clock()


@app.post("/clock/advance", tags=["simulation"])
async def advance_simulation_clock(request: AdvanceClockRequest):
    try:
        return await simulation.advance_clock(request.seconds)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
