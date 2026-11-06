from __future__ import annotations

import logging
import os

from fastapi import FastAPI
from opentelemetry import trace

from app.observability import configure_observability

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("inventory-api")

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
        logger.info(
            "inventory.lookup.started",
            extra={"product_id": product_id},
        )
        result = {
            "product_id": product_id,
            "available": True,
            "quantity": 42,
        }
        logger.info(
            "inventory.lookup.completed",
            extra={
                "product_id": product_id,
                "available_quantity": result["quantity"],
            },
        )
        return result
