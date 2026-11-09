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
from app.order.durable import (
    CompensationIncomplete,
    DurableOrderExecutor,
    DurableOrderFailure,
    OrderExecutionBusy,
    OrderExecutionConflict,
    PostgresOrderSagaRepository,
    PostgresPaymentGateway,
)
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
durable_order_executor: DurableOrderExecutor | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global durable_order_executor

    settings = DatabaseSettings.from_env()
    await database_manager.open(settings)
    try:
        if settings.backend == "postgres":
            workflow.configure_order_repository(
                PostgresOrderRepository(database_manager)
            )
            payment_adapter = PostgresPaymentGateway(
                database_manager,
                decline_above=Decimal(str(workflow.PAYMENT_DECLINE_ABOVE)),
            )
            durable_order_executor = DurableOrderExecutor(
                PostgresOrderSagaRepository(database_manager),
                payment_adapter,
            )
            recovery = await durable_order_executor.recover_pending(
                lambda adapter: create_order_processing_saga(
                    payment_adapter=adapter
                )
            )
            logger.info("order.saga.startup_recovery.completed", extra=recovery)
        else:
            workflow.configure_order_repository(None)
            durable_order_executor = None
        yield
    finally:
        durable_order_executor = None
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
    """Run or resume the durable Order Saga, compensating deterministic failures."""
    correlation_id = correlation_id_context.get() or ""
    existing = (
        await durable_order_executor.get_by_order(order_id)
        if durable_order_executor is not None
        else None
    )

    if existing is not None:
        saga_context = dict(existing["context"])
        if (
            int(saga_context["product_id"]) != request.product_id
            or int(saga_context["quantity"]) != request.quantity
        ):
            raise HTTPException(
                status_code=409,
                detail="Order ID is already bound to a different request.",
            )
        saga_id = existing["saga_id"]
        unit_price = Decimal(str(saga_context["unit_price"]))
        amount = Decimal(str(saga_context["amount"]))
        execution_record = existing
    else:
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
                status_code=503,
                detail="Product catalog is temporarily unavailable.",
            ) from None

        unit_price = Decimal(str(product["unit_price"])).quantize(
            Decimal("0.01")
        )
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

        saga_context = {
            "order_id": order_id,
            "product_id": request.product_id,
            "quantity": request.quantity,
            "unit_price": unit_price,
            "amount": amount,
            "correlation_id": correlation_id,
            "saga_id": saga_id,
        }
        execution_record = None

        if durable_order_executor is not None:
            try:
                execution_record = await durable_order_executor.start_or_load(
                    saga_context
                )
            except OrderExecutionConflict as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from None
            saga_context = dict(execution_record["context"])
            saga_id = execution_record["saga_id"]
            unit_price = Decimal(str(saga_context["unit_price"]))
            amount = Decimal(str(saga_context["amount"]))
        else:
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

    payment_adapter = (
        durable_order_executor.payment_gateway
        if durable_order_executor is not None
        else None
    )
    saga, compensation_tracker = create_order_processing_saga(
        payment_adapter=payment_adapter
    )

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
            "recovery": existing is not None,
        },
    )

    try:
        if durable_order_executor is not None:
            result = await durable_order_executor.execute(
                saga,
                compensation_tracker,
                execution_record,
            )
        else:
            result = await saga.run(saga_context, saga_id=saga_id)
    except OrderExecutionBusy:
        raise HTTPException(
            status_code=409,
            detail="This order is currently being recovered by another worker.",
        ) from None
    except OrderExecutionConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    except Exception as exc:
        incomplete_compensations = compensation_tracker.incomplete_steps
        if isinstance(exc, CompensationIncomplete):
            incomplete_compensations = exc.steps
        failure_category = (
            exc.failure_category
            if isinstance(exc, DurableOrderFailure)
            else classify_order_failure(exc)
        )
        outcome_status = (
            "compensation_incomplete"
            if incomplete_compensations
            else failure_category
        )
        failure_context = dict(saga_context)
        if durable_order_executor is not None:
            try:
                latest_execution = await durable_order_executor.get_by_order(
                    order_id
                )
            except Exception:
                latest_execution = None
                logger.exception(
                    "order.saga.failure_context_load_failed",
                    extra={"order_id": order_id, "saga_id": saga_id},
                )
            if latest_execution is not None:
                failure_context.update(latest_execution["context"])

        # Sagaz's in-memory listener and the durable coordinator both expose
        # the latest compensation context. Preserve completed side-effect IDs
        # in the final order projection for either execution mode.
        failure_context.update(compensation_tracker.last_context)

        try:
            await persist_order_outcome(
                {
                    "order_id": order_id,
                    "requested_product_id": request.product_id,
                    "product_id": request.product_id,
                    "quantity": request.quantity,
                    "unit_price": unit_price,
                    "total_amount": amount,
                    "saga_id": saga_id,
                    "reservation_id": failure_context.get("reservation_id"),
                    "payment_id": failure_context.get("payment_id"),
                    "status": outcome_status,
                    "failure_category": failure_category,
                    "correlation_id": failure_context.get(
                        "correlation_id", correlation_id
                    ),
                }
            )
        except HTTPException:
            if durable_order_executor is None:
                raise

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

        if failure_category == "payment_declined" or isinstance(
            exc, PaymentDeclined
        ):
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

        if isinstance(exc, DurableOrderFailure):
            if failure_category == "inventory_rejected":
                raise HTTPException(
                    status_code=409,
                    detail="Inventory service rejected the reservation.",
                ) from None
            if failure_category == "product_not_found":
                raise HTTPException(
                    status_code=404,
                    detail="Product not found.",
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
        "status": result.get("order_status", "processed"),
        "saga_id": saga_id,
        "correlation_id": saga_context.get("correlation_id", correlation_id),
        "reservation_id": result.get("reservation_id"),
        "payment_id": result.get("payment_id"),
        "unit_price": str(unit_price),
        "total_amount": str(amount),
    }


@app.get("/orders/{order_id}/recovery", tags=["orders"])
async def get_order_recovery(order_id: int):
    """Expose durable Saga progress and reconciliation requirements."""
    if durable_order_executor is None:
        raise HTTPException(
            status_code=404,
            detail="Durable order recovery is not enabled.",
        )
    view = await durable_order_executor.recovery_view(order_id)
    if view is None:
        raise HTTPException(status_code=404, detail="Order execution not found.")
    return view
