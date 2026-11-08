from __future__ import annotations

from decimal import Decimal
from importlib import import_module

import httpx
import pytest

from app import observability
from app.observability import CorrelationIdMiddleware
from app.order import workflow
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


def install_inventory_mock(
    monkeypatch,
    *,
    fail_release=False,
    reject_reservation=False,
    product_not_found=False,
    product_inactive=False,
):
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            if product_not_found:
                return httpx.Response(404, json={"detail": "product not found"})
            return httpx.Response(
                200,
                json={
                    "product_id": 7,
                    "sku": "DEMO-007",
                    "unit_price": 99.9,
                    "is_active": not product_inactive,
                },
            )
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


async def post_order(correlation_id: str, quantity: int = 2):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=order_app),
        base_url="http://order.test",
    ) as client:
        return await client.post(
            "/orders/123/process",
            headers={"X-Correlation-ID": correlation_id},
            json={"product_id": 7, "quantity": quantity},
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
    assert body["unit_price"] == "99.90"
    assert body["total_amount"] == "199.80"
    assert [request.method for request in requests] == ["GET", "POST"]
    assert requests[0].headers["X-Correlation-ID"] == correlation_id
    persisted = workflow._orders[123]
    assert persisted["status"] == "processed"
    assert persisted["requested_product_id"] == 7
    assert persisted["unit_price"] == Decimal("99.90")


@pytest.mark.asyncio
async def test_payment_decline_returns_402_after_inventory_compensation(monkeypatch):
    requests = install_inventory_mock(monkeypatch)
    correlation_id = "550e8400-e29b-41d4-a716-446655440302"

    response = await post_order(correlation_id, quantity=11)

    assert response.status_code == 402
    assert response.headers["X-Correlation-ID"] == correlation_id
    assert response.json()["detail"] == (
        "Payment declined; completed actions were compensated."
    )
    assert [request.method for request in requests] == ["GET", "POST", "DELETE"]
    assert workflow._orders[123]["status"] == "payment_declined"
    assert workflow._orders[123]["reservation_id"] == "reservation-api-test"


@pytest.mark.asyncio
async def test_failed_compensation_returns_reconciliation_details(monkeypatch):
    requests = install_inventory_mock(monkeypatch, fail_release=True)

    response = await post_order(
        "550e8400-e29b-41d4-a716-446655440303",
        quantity=11,
    )

    assert response.status_code == 500
    assert workflow._orders[123]["status"] == "compensation_incomplete"
    detail = response.json()["detail"]
    assert detail["message"] == (
        "Order processing failed and compensation is incomplete."
    )
    assert detail["incomplete_compensations"] == ["reserve_inventory"]
    assert [request.method for request in requests] == ["GET", "POST", "DELETE"]


@pytest.mark.asyncio
async def test_inventory_conflict_returns_409_without_charging_payment(monkeypatch):
    requests = install_inventory_mock(monkeypatch, reject_reservation=True)

    response = await post_order(
        "550e8400-e29b-41d4-a716-446655440304"
    )

    assert response.status_code == 409
    assert [request.method for request in requests] == ["GET", "POST"]
    assert workflow._orders[123]["status"] == "inventory_rejected"


@pytest.mark.asyncio
async def test_inactive_product_returns_409_without_reserving_stock(monkeypatch):
    requests = install_inventory_mock(monkeypatch, product_inactive=True)

    response = await post_order(
        "550e8400-e29b-41d4-a716-446655440306"
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "Product is inactive."
    assert [request.method for request in requests] == ["GET"]
    assert workflow._orders[123]["status"] == "product_inactive"


@pytest.mark.asyncio
async def test_unknown_product_returns_404_without_charging_payment(monkeypatch):
    requests = install_inventory_mock(monkeypatch, product_not_found=True)

    response = await post_order(
        "550e8400-e29b-41d4-a716-446655440305"
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Product not found."
    assert [request.method for request in requests] == ["GET"]
    assert workflow._orders[123]["status"] == "product_not_found"
    assert workflow._orders[123]["product_id"] is None
    assert workflow._orders[123]["requested_product_id"] == 7


@pytest.mark.asyncio
async def test_client_cannot_override_catalog_derived_total(monkeypatch):
    requests = install_inventory_mock(monkeypatch)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=order_app),
        base_url="http://order.test",
    ) as client:
        response = await client.post(
            "/orders/123/process",
            json={"product_id": 7, "quantity": 2, "amount": 0.01},
        )

    assert response.status_code == 422
    assert requests == []
