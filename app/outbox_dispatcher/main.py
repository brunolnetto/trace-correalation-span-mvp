from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from app.observability import configure_observability
from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager
from app.persistence.outbox import OutboxDispatcher, PostgresOutboxRepository

logger = logging.getLogger(__name__)
database = DatabaseManager(pool_name="outbox-dispatcher")
repository: PostgresOutboxRepository | None = None
dispatcher: OutboxDispatcher | None = None
dispatcher_task: asyncio.Task[None] | None = None


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    return value


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    return value


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global repository, dispatcher, dispatcher_task

    settings = DatabaseSettings.from_env()
    if settings.backend != "postgres":
        raise RuntimeError("Outbox dispatcher requires PostgreSQL persistence")
    collector_url = os.getenv("EVENT_COLLECTOR_URL")
    if not collector_url:
        raise RuntimeError("EVENT_COLLECTOR_URL is required by the outbox dispatcher")

    await database.open(settings)
    stop = asyncio.Event()
    try:
        repository = PostgresOutboxRepository(database)
        dispatcher = OutboxDispatcher(
            repository,
            collector_url=collector_url,
            batch_size=_int_env("OUTBOX_BATCH_SIZE", 50),
            lease_seconds=_int_env("OUTBOX_LEASE_SECONDS", 30),
            max_attempts=_int_env("OUTBOX_MAX_ATTEMPTS", 12),
            base_backoff_seconds=_float_env("OUTBOX_BASE_BACKOFF_SECONDS", 1.0),
            max_backoff_seconds=_float_env("OUTBOX_MAX_BACKOFF_SECONDS", 300.0),
            poll_interval_seconds=_float_env("OUTBOX_POLL_INTERVAL_SECONDS", 0.5),
            request_timeout_seconds=_float_env(
                "OUTBOX_REQUEST_TIMEOUT_SECONDS", 5.0
            ),
        )
        dispatcher_task = asyncio.create_task(
            dispatcher.run(stop),
            name="postgres-outbox-dispatcher",
        )
        app.state.outbox_repository = repository
        app.state.outbox_dispatcher = dispatcher
        yield
    finally:
        stop.set()
        if dispatcher_task is not None:
            try:
                await asyncio.wait_for(dispatcher_task, timeout=10.0)
            except TimeoutError:
                dispatcher_task.cancel()
                await asyncio.gather(dispatcher_task, return_exceptions=True)
            finally:
                dispatcher_task = None
        dispatcher = None
        repository = None
        await database.close()


app = FastAPI(
    title="Transactional Outbox Dispatcher",
    description=(
        "At-least-once PostgreSQL outbox delivery to the append-only Event Collector."
    ),
    version="0.1.0",
    lifespan=lifespan,
)
configure_observability(app, os.getenv("SERVICE_NAME", "outbox-dispatcher"))


@app.get("/healthz", tags=["health"])
async def healthcheck():
    task_healthy = dispatcher_task is not None and not dispatcher_task.done()
    if not await database.healthy() or not task_healthy:
        raise HTTPException(
            status_code=503,
            detail="Outbox dispatcher is not ready.",
        )
    return {"status": "ok", "service": "outbox-dispatcher"}


@app.get("/outbox/stats", tags=["operations"])
async def outbox_stats():
    if repository is None:
        raise HTTPException(status_code=503, detail="Outbox repository is unavailable.")
    return await repository.stats()


@app.get("/outbox/blocked", tags=["operations"])
async def blocked_outbox_events(
    limit: int = Query(default=100, ge=1, le=1000)
):
    if repository is None:
        raise HTTPException(status_code=503, detail="Outbox repository is unavailable.")
    return {"events": await repository.list_blocked(limit=limit)}
