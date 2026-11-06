import json
import logging
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI

from app.observability import (
    CorrelationIdMiddleware,
    JsonFormatter,
    correlation_id_context,
)


@pytest.fixture
def correlation_app():
    app = FastAPI()
    app.add_middleware(CorrelationIdMiddleware)

    @app.get("/")
    async def read_context():
        return {"correlation_id": correlation_id_context.get()}

    return app


@pytest.mark.asyncio
async def test_valid_correlation_id_is_preserved(correlation_app):
    expected = "550e8400-e29b-41d4-a716-446655440000"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=correlation_app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/", headers={"X-Correlation-ID": expected}
        )

    assert response.status_code == 200
    assert response.json()["correlation_id"] == expected
    assert response.headers["X-Correlation-ID"] == expected


@pytest.mark.asyncio
async def test_missing_correlation_id_is_generated(correlation_app):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=correlation_app),
        base_url="http://test",
    ) as client:
        response = await client.get("/")

    generated = response.headers["X-Correlation-ID"]
    assert str(UUID(generated)) == generated
    assert response.json()["correlation_id"] == generated


@pytest.mark.asyncio
async def test_invalid_correlation_id_is_replaced(correlation_app):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=correlation_app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/", headers={"X-Correlation-ID": "not-a-uuid"}
        )

    generated = response.headers["X-Correlation-ID"]
    assert str(UUID(generated)) == generated
    assert generated != "not-a-uuid"


@pytest.mark.asyncio
async def test_oversized_correlation_id_is_replaced(correlation_app):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=correlation_app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/", headers={"X-Correlation-ID": "a" * 100}
        )

    generated = response.headers["X-Correlation-ID"]
    assert str(UUID(generated)) == generated


def test_json_formatter_includes_correlation_and_extra_attributes():
    token = correlation_id_context.set(
        "550e8400-e29b-41d4-a716-446655440000"
    )
    try:
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="order.created",
            args=(),
            exc_info=None,
        )
        record.order_id = 123

        payload = json.loads(JsonFormatter("order-api").format(record))

        assert payload["service"] == "order-api"
        assert payload["message"] == "order.created"
        assert payload["correlation_id"] == (
            "550e8400-e29b-41d4-a716-446655440000"
        )
        assert payload["trace_id"] is None
        assert payload["span_id"] is None
        assert payload["attributes"] == {"order_id": 123}
    finally:
        correlation_id_context.reset(token)
