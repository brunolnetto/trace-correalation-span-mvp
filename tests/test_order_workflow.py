from __future__ import annotations

import asyncio
from uuid import NAMESPACE_URL, uuid5

import httpx
import pytest

from app.order import workflow
from app.order.workflow import (
    PaymentDeclined,
    create_order_processing_saga,
    payment_gateway,
)


@pytest.fixture(autouse=True)
async def reset_workflow_state():
    await payment_gateway.reset()
    workflow._orders.clear()
    yield
    await payment_gateway.reset()
    workflow._orders.clear()


def install_inventory_mock(monkeypatch, handler):
    requests: list[httpx.Request] = []

    def recording_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    def create_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(recording_handler),
            base_url="http://inventory.test",
        )

    monkeypatch.setattr(workflow, "create_inventory_client", create_client)
    return requests


def successful_inventory_handler(request: httpx.Request) -> httpx.Response:
    if request.method == "POST":
        return httpx.Response(
            200,
            json={
                "reservation_id": "reservation-123",
                "status": "reserved",
            },
        )
    if request.method == "DELETE":
        return httpx.Response(
            200,
            json={
                "reservation_id": "reservation-123",
                "status": "released",
            },
        )
    return httpx.Response(404)


def initial_context(amount: float = 99.0) -> dict:
    return {
        "order_id": 123,
        "product_id": 7,
        "quantity": 2,
        "amount": amount,
        "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
    }


@pytest.mark.asyncio
async def test_order_saga_completes_and_propagates_correlation_id(monkeypatch):
    requests = install_inventory_mock(
        monkeypatch, successful_inventory_handler
    )
    saga, tracker = create_order_processing_saga()

    result = await saga.run(initial_context(), saga_id="saga-success")

    assert result["order_status"] == "processed"
    assert result["reservation_id"] == "reservation-123"
    assert result["payment_id"]
    assert result["saga_id"] == "saga-success"
    assert await payment_gateway.status(result["payment_id"]) == "charged"
    assert tracker.incomplete_steps == []

    assert [request.method for request in requests] == ["POST"]
    assert requests[0].headers["X-Correlation-ID"] == (
        "550e8400-e29b-41d4-a716-446655440000"
    )
    assert requests[0].url.path == "/inventory/7/reservations"
    assert requests[0].content == (
        b'{"order_id":123,"saga_id":"saga-success","quantity":2}'
    )


@pytest.mark.asyncio
async def test_payment_decline_compensates_inventory_reservation(monkeypatch):
    requests = install_inventory_mock(
        monkeypatch, successful_inventory_handler
    )
    saga, tracker = create_order_processing_saga()

    with pytest.raises(PaymentDeclined):
        await saga.run(initial_context(amount=1001.0), saga_id="saga-declined")

    assert [request.method for request in requests] == ["POST", "DELETE"]
    assert requests[1].url.path == (
        "/inventory/reservations/reservation-123"
    )
    assert tracker.incomplete_steps == []
    assert tracker.last_context["reservation_id"] == "reservation-123"
    assert workflow._orders == {}


@pytest.mark.asyncio
async def test_order_confirmation_failure_refunds_then_releases_inventory(
    monkeypatch,
):
    requests = install_inventory_mock(
        monkeypatch, successful_inventory_handler
    )

    async def fail_persist_order(ctx: dict) -> dict:
        raise RuntimeError("simulated order-store failure")

    monkeypatch.setattr(workflow, "persist_order", fail_persist_order)
    saga, tracker = create_order_processing_saga()

    with pytest.raises(RuntimeError, match="simulated order-store failure"):
        await saga.run(initial_context(), saga_id="saga-confirm-failure")

    assert [request.method for request in requests] == ["POST", "DELETE"]
    assert tracker.incomplete_steps == []

    payment_id = str(uuid5(NAMESPACE_URL, "payment:saga-confirm-failure"))
    assert await payment_gateway.status(payment_id) == "refunded"


@pytest.mark.asyncio
async def test_failed_compensation_is_reported_by_tracker(monkeypatch):
    def fail_release(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return successful_inventory_handler(request)
        return httpx.Response(503, json={"detail": "inventory unavailable"})

    install_inventory_mock(monkeypatch, fail_release)
    saga, tracker = create_order_processing_saga()

    with pytest.raises(PaymentDeclined):
        await saga.run(initial_context(amount=1001.0), saga_id="saga-release-fails")

    assert tracker.incomplete_steps == ["reserve_inventory"]


@pytest.mark.asyncio
async def test_concurrent_sagas_keep_correlation_ids_isolated(monkeypatch):
    requests = install_inventory_mock(
        monkeypatch, successful_inventory_handler
    )
    contexts = [
        {
            **initial_context(),
            "order_id": order_id,
            "correlation_id": correlation_id,
        }
        for order_id, correlation_id in (
            (201, "550e8400-e29b-41d4-a716-446655440201"),
            (202, "550e8400-e29b-41d4-a716-446655440202"),
        )
    ]

    async def run_one(index: int, ctx: dict):
        saga, _ = create_order_processing_saga()
        return await saga.run(ctx, saga_id=f"saga-concurrent-{index}")

    results = await asyncio.gather(
        *(run_one(index, ctx) for index, ctx in enumerate(contexts))
    )

    assert {result["order_id"] for result in results} == {201, 202}
    headers = {request.headers["X-Correlation-ID"] for request in requests}
    assert headers == {
        "550e8400-e29b-41d4-a716-446655440201",
        "550e8400-e29b-41d4-a716-446655440202",
    }
