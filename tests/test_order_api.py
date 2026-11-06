from __future__ import annotations

from importlib import import_module

import httpx
import pytest

import app.observability as observability
import app.order.workflow as workflow
from app.observability import CorrelationIdMiddleware
from app.order.workflow import payment_gateway

# Import the production routes without configuring a real OTLP exporter in
# this in-process ASGI test. Docker reproduction verifies the real provider.
configure_observability = observability.configure_observability


def skip_observability_setup(app, service_name):
    return None


observability.configure_observability = skip_observability_setup
try:
    order_app = import_module("app.order.main").app
finally:
    observability.configure_observability = configure_observability

order_app.add_middleware(CorrelationIdMiddleware)


@pytest.fixture(autouse=True)
async def reset_state():
    await payment_gateway.reset()
    workflow._orders.clear()
    yield
    await payment_gateway.reset()
    workflow._orders.clear()


def install_inventory_mock(monkeypatch, *, fail_release=False, reject_reservation=False):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            if reject_reservation:
                return httpx.Response(409, json={"detail": "insufficient inventory"})
            return httpx.Response(
                200,
                json={
                    "reservation_id": "reservation-api-test",
                    "status": "reserved",
                },
            )
        if request.method == "DELETE":
            if fail_release:
                return httpx.Response(503, json={"detail": "inventory unavailable"})
            return httpx.Response(
                200,
                json={
                    "reservation_id": "reservation-api-test",
                    "status": "released",
                },
            )
        return httpx.Response(404)

    def create_client() -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="http://inventory.test",
        )

    monkeypatch.setattr(workflow, "create_inventory_client", create_client)
    return requests


async def post_order(correlation_id: str, amount: float = 99.0):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=order_app),
        base_url="http://order.test",
    ) as client:
        return await client.post(
            "/orders/123/process",
            headers={"X-Correlation-ID": correlation_id},
            json={"product_id": 7, "quantity": 2, "amount": amount},
        )


@pytest.mark.asyncio
async def test_process_order_returns_saga_result_and_correlation_id(monkeypatch):
    requests = install_inventory_mock(monkeypatch)
    correlation_id = "550e8400-e29b-41d4-a716-446655440301"

    response = await post_order(correlation_id)

    assert response.status_code == 201
    assert response.headers["X-Correlation-ID"] == correlation_id
    body = response.json()
    assert body["status"] == "processed"
    assert body["correlation_id"] == correlation_id
    assert body["saga_id"]
    assert body["reservation_id"] == "reservation-api-test"
    assert body["payment_id"]
    assert [request.method for request in requests] == ["POST"]
    assert requests[0].headers["X-Correlation-ID"] == correlation_id


@pytest.mark.asyncio
async def test_payment_decline_returns_402_after_inventory_compensation(monkeypatch):
    requests = install_inventory_mock(monkeypatch)
    correlation_id = "550e8400-e29b-41d4-a716-446655440302"

    response = await post_order(correlation_id, amount=1001.0)

    assert response.status_code == 402
    assert response.headers["X-Correlation-ID"] == correlation_id
    assert response.json()["detail"] == (
        "Payment declined; completed actions were compensated."
    )
    assert [request.method for request in requests] == ["POST", "DELETE"]


@pytest.mark.asyncio
async def test_failed_compensation_returns_reconciliation_details(monkeypatch):
    requests = install_inventory_mock(monkeypatch, fail_release=True)

    response = await post_order(
        "550e8400-e29b-41d4-a716-446655440303",
        amount=1001.0,
    )

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["message"] == (
        "Order processing failed and compensation is incomplete."
    )
    assert detail["incomplete_compensations"] == ["reserve_inventory"]
    assert [request.method for request in requests] == ["POST", "DELETE"]


@pytest.mark.asyncio
async def test_inventory_conflict_returns_409_without_charging_payment(monkeypatch):
    requests = install_inventory_mock(monkeypatch, reject_reservation=True)

    response = await post_order(
        "550e8400-e29b-41d4-a716-446655440304"
    )

    assert response.status_code == 409
    assert [request.method for request in requests] == ["POST"]
    assert workflow._orders == {}
