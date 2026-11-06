from __future__ import annotations

import logging
import os

import httpx
from fastapi import FastAPI, HTTPException
from opentelemetry import trace

from app.observability import configure_observability, correlation_id_context

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
