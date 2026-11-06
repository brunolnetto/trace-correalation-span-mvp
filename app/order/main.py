from __future__ import annotations

import logging
import os
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException
from opentelemetry import trace

from app.observability import configure_observability, correlation_id_context
from app.order.workflow import (
    PaymentDeclined,
    ProcessOrderRequest,
    create_order_processing_saga,
)

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("order-api")

app = FastAPI(
    title="Order API",
    description="Order service demonstrating correlation IDs and distributed traces.",
    version="0.1.0",
)
configure_observability(app, os.getenv("SERVICE_NAME", "order-api"))

INVENTORY_API_URL = os.getenv(
    "INVENTORY_API_URL", "http://localhost:8001"
)


@app.get("/healthz", tags=["health"])
async def healthcheck():
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


@app.post("/orders/{order_id}/process", status_code=201, tags=["orders"])
async def process_order(order_id: int, request: ProcessOrderRequest):
    """Run the order saga and compensate completed actions on failure."""
    correlation_id = correlation_id_context.get() or ""
    saga_id = str(uuid4())
    saga, compensation_tracker = create_order_processing_saga()

    current_span = trace.get_current_span()
    if current_span.is_recording():
        current_span.set_attribute("saga.id", saga_id)
        current_span.set_attribute("saga.name", saga.saga_name)

    logger.info(
        "order.process.started",
        extra={
            "order_id": order_id,
            "saga_id": saga_id,
        },
    )

    try:
        result = await saga.run(
            {
                "order_id": order_id,
                "product_id": request.product_id,
                "quantity": request.quantity,
                "amount": request.amount,
                "correlation_id": correlation_id,
            },
            saga_id=saga_id,
        )
    except Exception as exc:
        incomplete_compensations = compensation_tracker.incomplete_steps
        logger.exception(
            "order.process.failed",
            extra={
                "order_id": order_id,
                "saga_id": saga_id,
                "incomplete_compensations": incomplete_compensations,
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
                    "message": "Order processing failed and compensation is incomplete.",
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
            status_code = (
                409
                if exc.response.status_code == 409
                else 502
            )
            raise HTTPException(
                status_code=status_code,
                detail="Inventory service rejected or failed the reservation.",
            ) from None

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
            "saga_id": saga_id,
            "payment_id": result["payment_id"],
            "reservation_id": result["reservation_id"],
        },
    )
    return {
        "order_id": order_id,
        "status": result["order_status"],
        "saga_id": saga_id,
        "correlation_id": correlation_id,
        "reservation_id": result["reservation_id"],
        "payment_id": result["payment_id"],
    }
