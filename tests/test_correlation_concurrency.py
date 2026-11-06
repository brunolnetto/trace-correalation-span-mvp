import asyncio

import httpx
import pytest
from fastapi import FastAPI

from app.observability import CorrelationIdMiddleware


@pytest.mark.asyncio
async def test_concurrent_requests_keep_correlation_context_isolated():
    app = FastAPI()
    app.add_middleware(CorrelationIdMiddleware)

    @app.get("/")
    async def read_context():
        # Yield so requests overlap and exercise ContextVar isolation.
        await asyncio.sleep(0.01)
        from app.observability import correlation_id_context

        return {"correlation_id": correlation_id_context.get()}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        ids = [
            "550e8400-e29b-41d4-a716-446655440001",
            "550e8400-e29b-41d4-a716-446655440002",
        ]
        responses = await asyncio.gather(
            *(
                client.get("/", headers={"X-Correlation-ID": value})
                for value in ids
            )
        )

    assert [response.json()["correlation_id"] for response in responses] == ids
    assert [response.headers["X-Correlation-ID"] for response in responses] == ids
