import json
import logging
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI

from app.observability import (
    CorrelationIdLogFilter,
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

    @app.get("/duplicate-header")
    async def duplicate_header():
        from fastapi.responses import Response

        return Response(
            content="ok",
            headers={"X-Correlation-ID": "stale-response-value"},
        )

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


def test_correlation_id_log_filter_adds_context_attribute():
    expected = "550e8400-e29b-41d4-a716-446655440000"
    token = correlation_id_context.set(expected)
    try:
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="order.process.failed",
            args=(),
            exc_info=None,
        )

        assert CorrelationIdLogFilter().filter(record) is True
        assert record.correlation_id == expected
    finally:
        correlation_id_context.reset(token)


def test_correlation_id_log_filter_handles_outside_request_context():
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="startup",
        args=(),
        exc_info=None,
    )

    assert CorrelationIdLogFilter().filter(record) is True
    assert record.correlation_id is None


def test_correlation_id_log_filter_preserves_explicit_background_context():
    explicit = "550e8400-e29b-41d4-a716-446655440001"
    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="outbox.delivery.failed",
        args=(),
        exc_info=None,
    )
    record.correlation_id = explicit

    assert CorrelationIdLogFilter().filter(record) is True
    assert record.correlation_id == explicit


@pytest.mark.asyncio
async def test_correlation_middleware_replaces_preexisting_response_header(
    correlation_app,
):
    expected = "550e8400-e29b-41d4-a716-446655440009"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=correlation_app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/duplicate-header",
            headers={"X-Correlation-ID": expected},
        )

    assert response.headers.get_list("X-Correlation-ID") == [expected]
