from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from decimal import Decimal
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException
from opentelemetry import trace

from app.observability import configure_observability, correlation_id_context
from app.order import workflow
from app.order.repository import PostgresOrderRepository
from app.order.workflow import (
    PaymentDeclined,
    ProcessOrderRequest,
    create_order_processing_saga,
)
from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("order-api")

database_manager = DatabaseManager(pool_name="order-api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = DatabaseSettings.from_env()
    await database_manager.open(settings)
    try:
        if settings.backend == "postgres":
            workflow.configure_order_repository(
                PostgresOrderRepository(database_manager)
            )
        else:
            workflow.configure_order_repository(None)
        yield
    finally:
        workflow.configure_order_repository(None)
        await database_manager.close()


app = FastAPI(
    title="Order API",
    description="Order service demonstrating correlation IDs and distributed traces.",
    version="0.1.0",
    lifespan=lifespan,
)
configure_observability(app, os.getenv("SERVICE_NAME", "order-api"))

INVENTORY_API_URL = os.getenv(
    "INVENTORY_API_URL", "http://localhost:8001"
)


def classify_order_failure(exc: Exception) -> str:
    if isinstance(exc, PaymentDeclined):
        return "payment_declined"
    if isinstance(exc, httpx.HTTPStatusError):
        if exc.response.status_code == 404:
            return "product_not_found"
        if exc.response.status_code == 409:
            return "inventory_rejected"
        return "inventory_service_error"
    if isinstance(exc, httpx.HTTPError):
        return "inventory_unavailable"
    if isinstance(exc, TimeoutError):
        return "timeout"
    return "processing_error"


@app.get("/healthz", tags=["health"])
async def healthcheck():
    if not await database_manager.healthy():
        raise HTTPException(
            status_code=503,
            detail="Database unavailable",
        )
    return {"status": "ok", "service": "order-api"}


@app.get("/orders/{order_id}", tags=["orders"])
async def get_order(order_id: int):
    logger.info("order.lookup.started", extra={"order_id": order_id})

    try:
        with tracer.start_as_current_span("order.load_inventory") as span:
            span.set_attribute("order.id", order_id)

            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(
                    f"{INVENTORY_API_URL}/inventory/{order_id}",
                    headers={
                        "X-Correlation-ID": correlation_id_context.get() or ""
                    },
                )
                response.raise_for_status()
                inventory = response.json()
    except httpx.HTTPError:
        logger.exception(
            "order.inventory_lookup.failed",
            extra={"order_id": order_id},
        )
        raise HTTPException(
            status_code=503,
            detail="Inventory service is temporarily unavailable",
        ) from None

    logger.info("order.lookup.completed", extra={"order_id": order_id})
    return {
        "order_id": order_id,
        "status": "processing",
        "inventory": inventory,
    }


async def persist_order_outcome(record: dict) -> None:
    try:
        await workflow.record_order_outcome(record)
    except Exception:
        logger.exception(
            "order.persistence.failed",
            extra={
                "order_id": record.get("order_id"),
                "status": record.get("status"),
            },
        )
        raise HTTPException(
            status_code=503,
            detail="Order persistence is temporarily unavailable.",
        ) from None


@app.post("/orders/{order_id}/process", status_code=201, tags=["orders"])
async def process_order(order_id: int, request: ProcessOrderRequest):
    """Run the order saga, persist its outcome, and compensate on failure."""
    correlation_id = correlation_id_context.get() or ""
    saga_id = str(uuid4())

    try:
        async with workflow.create_inventory_client() as client:
            response = await client.get(
                f"/products/{request.product_id}",
                headers={"X-Correlation-ID": correlation_id},
            )
            response.raise_for_status()
            product = response.json()
    except httpx.HTTPStatusError as exc:
        failure_category = (
            "product_not_found"
            if exc.response.status_code == 404
            else "product_catalog_rejected"
        )
        await persist_order_outcome(
            {
                "order_id": order_id,
                "requested_product_id": request.product_id,
                "product_id": None,
                "quantity": request.quantity,
                "unit_price": None,
                "total_amount": None,
                "saga_id": saga_id,
                "reservation_id": None,
                "payment_id": None,
                "status": failure_category,
                "failure_category": failure_category,
                "correlation_id": correlation_id,
            }
        )
        logger.warning(
            "order.process.preflight_rejected",
            extra={
                "order_id": order_id,
                "product_id": request.product_id,
                "quantity": request.quantity,
                "failure_category": failure_category,
            },
        )
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404, detail="Product not found."
            ) from None
        raise HTTPException(
            status_code=502, detail="Product catalog rejected the lookup."
        ) from None
    except httpx.HTTPError:
        failure_category = "product_catalog_unavailable"
        await persist_order_outcome(
            {
                "order_id": order_id,
                "requested_product_id": request.product_id,
                "product_id": None,
                "quantity": request.quantity,
                "unit_price": None,
                "total_amount": None,
                "saga_id": saga_id,
                "reservation_id": None,
                "payment_id": None,
                "status": failure_category,
                "failure_category": failure_category,
                "correlation_id": correlation_id,
            }
        )
        logger.exception(
            "order.process.preflight_rejected",
            extra={
                "order_id": order_id,
                "product_id": request.product_id,
                "quantity": request.quantity,
                "failure_category": failure_category,
            },
        )
        raise HTTPException(
            status_code=503, detail="Product catalog is temporarily unavailable."
        ) from None

    unit_price = Decimal(str(product["unit_price"])).quantize(Decimal("0.01"))
    amount = unit_price * request.quantity
    if not product["is_active"]:
        await persist_order_outcome(
            {
                "order_id": order_id,
                "requested_product_id": request.product_id,
                "product_id": request.product_id,
                "quantity": request.quantity,
                "unit_price": unit_price,
                "total_amount": amount,
                "saga_id": saga_id,
                "reservation_id": None,
                "payment_id": None,
                "status": "product_inactive",
                "failure_category": "product_inactive",
                "correlation_id": correlation_id,
            }
        )
        logger.warning(
            "order.process.preflight_rejected",
            extra={
                "order_id": order_id,
                "product_id": request.product_id,
                "quantity": request.quantity,
                "failure_category": "product_inactive",
            },
        )
        raise HTTPException(status_code=409, detail="Product is inactive.")

    await persist_order_outcome(
        {
            "order_id": order_id,
            "requested_product_id": request.product_id,
            "product_id": request.product_id,
            "quantity": request.quantity,
            "unit_price": unit_price,
            "total_amount": amount,
            "saga_id": saga_id,
            "reservation_id": None,
            "payment_id": None,
            "status": "pending",
            "failure_category": None,
            "correlation_id": correlation_id,
        }
    )

    saga, compensation_tracker = create_order_processing_saga()

    current_span = trace.get_current_span()
    if current_span.is_recording():
        current_span.set_attribute("saga.id", saga_id)
        current_span.set_attribute("saga.name", saga.saga_name)

    logger.info(
        "order.process.started",
        extra={
            "order_id": order_id,
            "product_id": request.product_id,
            "quantity": request.quantity,
            "unit_price": str(unit_price),
            "total_amount": str(amount),
            "saga_id": saga_id,
        },
    )

    saga_context = {
        "order_id": order_id,
        "product_id": request.product_id,
        "quantity": request.quantity,
        "unit_price": unit_price,
        "amount": amount,
        "correlation_id": correlation_id,
    }

    try:
        result = await saga.run(saga_context, saga_id=saga_id)
    except Exception as exc:
        incomplete_compensations = compensation_tracker.incomplete_steps
        compensation_context = compensation_tracker.last_context
        failure_category = classify_order_failure(exc)
        outcome_status = (
            "compensation_incomplete"
            if incomplete_compensations
            else failure_category
        )
        await persist_order_outcome(
            {
                "order_id": order_id,
                "requested_product_id": request.product_id,
                "product_id": request.product_id,
                "quantity": request.quantity,
                "unit_price": unit_price,
                "total_amount": amount,
                "saga_id": saga_id,
                "reservation_id": compensation_context.get(
                    "reservation_id", saga_context.get("reservation_id")
                ),
                "payment_id": compensation_context.get(
                    "payment_id", saga_context.get("payment_id")
                ),
                "status": outcome_status,
                "failure_category": failure_category,
                "correlation_id": correlation_id,
            }
        )
        logger.exception(
            "order.process.failed",
            extra={
                "order_id": order_id,
                "product_id": request.product_id,
                "quantity": request.quantity,
                "unit_price": str(unit_price),
                "total_amount": str(amount),
                "saga_id": saga_id,
                "incomplete_compensations": incomplete_compensations,
                "failure_category": failure_category,
            },
        )

        if incomplete_compensations:
            logger.error(
                "order.compensation.incomplete",
                extra={
                    "order_id": order_id,
                    "saga_id": saga_id,
                    "incomplete_compensations": incomplete_compensations,
                },
            )
            raise HTTPException(
                status_code=500,
                detail={
                    "message": (
                        "Order processing failed and compensation is incomplete."
                    ),
                    "saga_id": saga_id,
                    "incomplete_compensations": incomplete_compensations,
                },
            ) from None

        if isinstance(exc, PaymentDeclined):
            raise HTTPException(
                status_code=402,
                detail="Payment declined; completed actions were compensated.",
            ) from None

        if isinstance(exc, httpx.HTTPStatusError):
            inventory_status = exc.response.status_code
            if inventory_status in {404, 409}:
                status_code = inventory_status
                detail = (
                    "Product not found."
                    if inventory_status == 404
                    else "Inventory service rejected the reservation."
                )
            else:
                status_code = 502
                detail = "Inventory service failed the reservation."
            raise HTTPException(status_code=status_code, detail=detail) from None

        if isinstance(exc, httpx.HTTPError):
            raise HTTPException(
                status_code=503,
                detail="Inventory service is temporarily unavailable.",
            ) from None

        if isinstance(exc, TimeoutError):
            raise HTTPException(
                status_code=504,
                detail="Order processing timed out.",
            ) from None

        raise HTTPException(
            status_code=500,
            detail="Order processing failed; completed actions were compensated.",
        ) from None

    logger.info(
        "order.process.completed",
        extra={
            "order_id": order_id,
            "product_id": request.product_id,
            "quantity": request.quantity,
            "unit_price": str(unit_price),
            "saga_id": saga_id,
            "payment_id": result["payment_id"],
            "reservation_id": result["reservation_id"],
            "total_amount": str(amount),
        },
    )
    return {
        "order_id": order_id,
        "status": result["order_status"],
        "saga_id": saga_id,
        "correlation_id": correlation_id,
        "reservation_id": result["reservation_id"],
        "payment_id": result["payment_id"],
        "unit_price": str(unit_price),
        "total_amount": str(amount),
    }
