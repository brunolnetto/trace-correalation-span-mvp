from __future__ import annotations

import pytest

from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager


@pytest.mark.asyncio
async def test_memory_backend_has_no_pool_and_stays_healthy() -> None:
    manager = DatabaseManager()
    await manager.open(
        DatabaseSettings(
            backend="memory",
            database_url=None,
        )
    )

    assert manager.pool is None
    assert await manager.healthy()

    await manager.close()
    assert manager.pool is None
    assert await manager.healthy()


@pytest.mark.asyncio
async def test_postgres_backend_rejects_missing_url_before_pool_creation() -> None:
    manager = DatabaseManager()

    with pytest.raises(ValueError, match="DATABASE_URL is required"):
        await manager.open(
            DatabaseSettings(
                backend="postgres",
                database_url=None,
            )
        )

    assert manager.pool is None


@pytest.mark.asyncio
async def test_connection_requires_an_open_postgres_pool() -> None:
    manager = DatabaseManager()

    with pytest.raises(RuntimeError, match="pool is not open"):
        manager.connection()
