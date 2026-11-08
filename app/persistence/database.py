from __future__ import annotations

import logging
from contextlib import AbstractAsyncContextManager
from typing import Any

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from app.persistence.config import DatabaseSettings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Own the application-scoped PostgreSQL pool.

    Pool creation and shutdown belong to the ASGI lifespan. The memory backend
    deliberately has no pool, allowing existing unit tests to use InventoryStore.
    """

    def __init__(self, *, pool_name: str = "application") -> None:
        self._pool_name = pool_name
        self._settings: DatabaseSettings | None = None
        self._pool: AsyncConnectionPool[AsyncConnection] | None = None

    @property
    def pool(self) -> AsyncConnectionPool[AsyncConnection] | None:
        return self._pool

    async def open(self, settings: DatabaseSettings) -> None:
        if self._pool is not None:
            raise RuntimeError("Database pool is already open")

        self._settings = settings
        if settings.backend == "memory":
            return

        if not settings.database_url:
            raise ValueError("DATABASE_URL is required for PostgreSQL persistence")

        pool = AsyncConnectionPool(
            conninfo=settings.database_url,
            min_size=settings.pool_min_size,
            max_size=settings.pool_max_size,
            timeout=settings.connect_timeout_seconds,
            kwargs={
                "connect_timeout": settings.connect_timeout_seconds,
                "row_factory": dict_row,
            },
            open=False,
            name=self._pool_name,
        )
        try:
            await pool.open(
                wait=True,
                timeout=settings.connect_timeout_seconds,
            )
            self._pool = pool
            if not await self.healthy():
                raise ConnectionError("PostgreSQL health check failed after pool startup")
        except Exception:
            self._pool = None
            await pool.close()
            raise

    async def close(self) -> None:
        pool, self._pool = self._pool, None
        try:
            if pool is not None:
                await pool.close()
        finally:
            self._settings = None

    def connection(
        self, *, timeout: float | None = None
    ) -> AbstractAsyncContextManager[AsyncConnection]:
        """Check out a connection with a transaction-scoped pool context.

        Psycopg commits when the context exits normally and rolls back when an
        exception escapes it. Inventory mutations keep all projection,
        reservation, journal, and idempotency writes inside this boundary.
        """
        if self._pool is None:
            raise RuntimeError("PostgreSQL connection pool is not open")
        effective_timeout = timeout
        if effective_timeout is None and self._settings is not None:
            effective_timeout = self._settings.connect_timeout_seconds
        return self._pool.connection(timeout=effective_timeout)

    async def healthy(self) -> bool:
        if self._settings is None or self._settings.backend == "memory":
            return True
        if self._pool is None or self._pool.closed:
            return False

        try:
            async with self._pool.connection(
                timeout=self._settings.connect_timeout_seconds
            ) as connection:
                cursor = await connection.execute("SELECT 1 AS healthy")
                row: dict[str, Any] | None = await cursor.fetchone()
                return bool(row and row["healthy"] == 1)
        except Exception:
            logger.exception("database.health_check.failed")
            return False
