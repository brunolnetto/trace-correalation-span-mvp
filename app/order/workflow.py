from __future__ import annotations

import os
from decimal import Decimal
from threading import Lock
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx
from pydantic import BaseModel, ConfigDict, Field
from sagaz import Saga, SagaConfig, action, compensate
from sagaz.core.listeners import SagaListener

from app.persistence.ports import OrderRepository

INVENTORY_API_URL = os.getenv("INVENTORY_API_URL", "http://localhost:8001")
PAYMENT_DECLINE_ABOVE = float(os.getenv("PAYMENT_DECLINE_ABOVE", "1000"))


class ProcessOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    product_id: int = Field(gt=0)
    quantity: int = Field(default=1, gt=0)


class PaymentDeclined(Exception):
    """Raised when the demo payment gateway rejects a charge."""

    def __init__(self, message: str, *, payment_id: str | None = None) -> None:
        super().__init__(message)
        self.payment_id = payment_id


class PaymentGateway:
    """Process-local payment adapter used by this runnable MVP."""

    def __init__(self) -> None:
        self._payments: dict[str, dict[str, Any]] = {}
        self._lock = Lock()

    async def charge(self, ctx: dict[str, Any]) -> str:
        amount = float(ctx["amount"])
        if amount > PAYMENT_DECLINE_ABOVE:
            raise PaymentDeclined("Payment was declined by the demo gateway")

        payment_id = str(uuid5(NAMESPACE_URL, f"payment:{ctx['saga_id']}"))
        with self._lock:
            existing = self._payments.get(payment_id)
            if existing is not None:
                if existing["status"] == "charged":
                    return payment_id
                raise PaymentDeclined("This saga's payment was already refunded")

            self._payments[payment_id] = {
                "payment_id": payment_id,
                "order_id": ctx["order_id"],
                "amount": amount,
                "status": "charged",
            }
        return payment_id

    async def refund(self, payment_id: str) -> None:
        with self._lock:
            payment = self._payments.get(payment_id)
            if payment is not None and payment["status"] == "charged":
                payment["status"] = "refunded"

    async def status(self, payment_id: str) -> str | None:
        with self._lock:
            payment = self._payments.get(payment_id)
            return payment["status"] if payment is not None else None

    async def reset(self) -> None:
        """Reset process-local state; intended for deterministic tests."""
        with self._lock:
            self._payments.clear()


payment_gateway = PaymentGateway()
_orders: dict[int, dict[str, Any]] = {}
_order_lock = Lock()
_order_repository: OrderRepository | None = None


def configure_order_repository(repository: OrderRepository | None) -> None:
    """Select the durable repository, or the in-memory test double."""
    global _order_repository
    _order_repository = repository


async def record_order_outcome(record: dict[str, Any]) -> dict[str, Any]:
    """Store the latest accepted/rejected business outcome for an order ID."""
    if _order_repository is not None:
        return await _order_repository.save_outcome(record)

    with _order_lock:
        _orders[int(record["order_id"])] = dict(record)
        return dict(_orders[int(record["order_id"])])


async def persist_order(ctx: dict[str, Any]) -> dict[str, str]:
    """Persist the successful terminal result of the Order Saga."""
    unit_price = ctx.get("unit_price")
    if unit_price is None:
        unit_price = Decimal(str(ctx["amount"])) / int(ctx["quantity"])

    await record_order_outcome(
        {
            "order_id": int(ctx["order_id"]),
            "requested_product_id": int(ctx["product_id"]),
            "product_id": int(ctx["product_id"]),
            "quantity": int(ctx["quantity"]),
            "unit_price": unit_price,
            "total_amount": ctx["amount"],
            "saga_id": ctx["saga_id"],
            "reservation_id": ctx.get("reservation_id"),
            "payment_id": ctx.get("payment_id"),
            "status": "processed",
            "failure_category": None,
            "correlation_id": ctx.get("correlation_id"),
        }
    )
    return {"order_status": "processed"}


def create_inventory_client() -> httpx.AsyncClient:
    """Factory kept injectable so the workflow can be tested without Docker."""
    return httpx.AsyncClient(base_url=INVENTORY_API_URL, timeout=5.0)


class CompensationTracker(SagaListener):
    """Tracks compensation attempts through Sagaz's public lifecycle hooks.

    Sagaz currently emits start and completion callbacks but no failure callback.
    A started compensation with no completion is therefore an incomplete
    compensation and must be surfaced for reconciliation.
    """

    def __init__(self) -> None:
        self._started: set[str] = set()
        self._completed: set[str] = set()
        self._last_context: dict[str, Any] = {}

    async def on_compensation_start(
        self, saga_name: str, step_name: str, ctx: dict[str, Any]
    ) -> None:
        self._started.add(step_name)
        self._last_context = dict(ctx)

    async def on_compensation_complete(
        self, saga_name: str, step_name: str, ctx: dict[str, Any]
    ) -> None:
        self._completed.add(step_name)
        self._last_context = dict(ctx)

    def record_compensation_start(
        self, step_name: str, ctx: dict[str, Any]
    ) -> None:
        """Record a durable coordinator's compensation start."""
        self._started.add(step_name)
        self._last_context = dict(ctx)

    def record_compensation_complete(
        self, step_name: str, ctx: dict[str, Any]
    ) -> None:
        """Record a durable coordinator's compensation completion."""
        self._completed.add(step_name)
        self._last_context = dict(ctx)

    def record_compensation_failure(
        self, step_name: str, ctx: dict[str, Any]
    ) -> None:
        """Keep a failed compensation visible as incomplete."""
        self._started.add(step_name)
        self._last_context = dict(ctx)

    @property
    def last_context(self) -> dict[str, Any]:
        """Return the most recent context seen by a compensation callback."""
        return dict(self._last_context)

    @property
    def incomplete_steps(self) -> list[str]:
        return sorted(self._started - self._completed)


class OrderProcessingSaga(Saga):
    """Coordinates inventory reservation, payment and order confirmation."""

    saga_name = "order-processing"

    def __init__(
        self,
        *,
        config: SagaConfig | None = None,
        payment_adapter: Any | None = None,
    ) -> None:
        self.payment_adapter = payment_adapter or payment_gateway
        super().__init__(config=config)

    @action("reserve_inventory", timeout_seconds=10, max_retries=0)
    async def reserve_inventory(self, ctx: dict[str, Any]) -> dict[str, Any]:
        async with create_inventory_client() as client:
            response = await client.post(
                f"/inventory/{ctx['product_id']}/reservations",
                json={
                    "order_id": ctx["order_id"],
                    "saga_id": ctx["saga_id"],
                    "quantity": ctx["quantity"],
                },
                headers={"X-Correlation-ID": ctx["correlation_id"]},
            )
            response.raise_for_status()
        return response.json()

    @compensate("reserve_inventory", timeout_seconds=10, max_retries=0)
    async def release_inventory(self, ctx: dict[str, Any]) -> None:
        reservation_id = ctx.get("reservation_id")
        if not reservation_id:
            return

        async with create_inventory_client() as client:
            response = await client.delete(
                f"/inventory/reservations/{reservation_id}",
                headers={"X-Correlation-ID": ctx["correlation_id"]},
            )
            response.raise_for_status()

    @action(
        "charge_payment",
        depends_on=["reserve_inventory"],
        timeout_seconds=10,
        max_retries=0,
    )
    async def charge_payment(self, ctx: dict[str, Any]) -> dict[str, str]:
        payment_id = await self.payment_adapter.charge(ctx)
        return {"payment_id": payment_id}

    @compensate("charge_payment", timeout_seconds=10, max_retries=0)
    async def refund_payment(self, ctx: dict[str, Any]) -> None:
        payment_id = ctx.get("payment_id")
        if payment_id:
            await self.payment_adapter.refund(payment_id)

    @action(
        "confirm_order",
        depends_on=["charge_payment"],
        timeout_seconds=10,
        max_retries=0,
    )
    async def confirm_order(self, ctx: dict[str, Any]) -> dict[str, str]:
        return await persist_order(ctx)


def create_order_processing_saga(
    payment_adapter: Any | None = None,
) -> tuple[OrderProcessingSaga, CompensationTracker]:
    """Create an isolated saga instance and its per-execution listener."""
    tracker = CompensationTracker()
    config = SagaConfig(
        logging=tracker,
        metrics=False,
        tracing=False,
    )
    return OrderProcessingSaga(
        config=config,
        payment_adapter=payment_adapter,
    ), tracker
