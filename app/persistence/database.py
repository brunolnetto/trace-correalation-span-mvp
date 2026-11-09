from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar, Token
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
        self._bound_connection: ContextVar[AsyncConnection | None] = ContextVar(
            f"{pool_name}_bound_connection",
            default=None,
        )

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
                raise ConnectionError(
                    "PostgreSQL health check failed after pool startup"
                )
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

    def bind_connection(
        self, connection: AsyncConnection
    ) -> Token[AsyncConnection | None]:
        """Bind a checked-out session to the current async execution context."""
        return self._bound_connection.set(connection)

    def reset_connection(
        self, token: Token[AsyncConnection | None]
    ) -> None:
        """Restore the connection binding that was active before this scope."""
        self._bound_connection.reset(token)

    async def _set_transaction_context(
        self, connection: AsyncConnection
    ) -> None:
        """Expose request correlation and trace IDs to transactional triggers."""
        from opentelemetry import trace

        from app.observability import correlation_id_context

        span_context = trace.get_current_span().get_span_context()
        trace_id = (
            format(span_context.trace_id, "032x")
            if span_context.is_valid
            else ""
        )
        span_id = (
            format(span_context.span_id, "016x")
            if span_context.is_valid
            else ""
        )
        await connection.execute(
            """
            SELECT
                set_config('app.correlation_id', %s, true),
                set_config('app.trace_id', %s, true),
                set_config('app.span_id', %s, true)
            """,
            (correlation_id_context.get() or "", trace_id, span_id),
        )

    @asynccontextmanager
    async def _bound_transaction(
        self, connection: AsyncConnection
    ) -> AsyncIterator[AsyncConnection]:
        """Run one repository operation on the currently bound session."""
        async with connection.transaction():
            await self._set_transaction_context(connection)
            yield connection

    @asynccontextmanager
    async def _pooled_transaction(
        self,
        timeout: float | None,
    ) -> AsyncIterator[AsyncConnection]:
        pool = self._pool
        if pool is None:
            raise RuntimeError("PostgreSQL connection pool is not open")
        async with pool.connection(timeout=timeout) as connection:
            await self._set_transaction_context(connection)
            yield connection

    def connection(
        self, *, timeout: float | None = None
    ) -> AbstractAsyncContextManager[AsyncConnection]:
        """Use the Saga's lock-owning session or check out a pooled connection.

        Normal callers receive a transaction-scoped pool connection. A durable
        Saga binds its session-level advisory-lock connection to the current
        async context; its checkpoint and payment transactions then reuse that
        session instead of consuming a second pool slot. Psycopg commits when a
        transaction context exits normally and rolls it back on an exception.
        """
        bound = self._bound_connection.get()
        if bound is not None:
            return self._bound_transaction(bound)

        if self._pool is None:
            raise RuntimeError("PostgreSQL connection pool is not open")
        effective_timeout = timeout
        if effective_timeout is None and self._settings is not None:
            effective_timeout = self._settings.connect_timeout_seconds
        return self._pooled_transaction(effective_timeout)

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
