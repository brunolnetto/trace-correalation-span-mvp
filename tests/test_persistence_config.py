from __future__ import annotations

import pytest

from app.persistence.config import (
    DatabaseSettings,
    PersistenceConfigurationError,
)


def test_development_defaults_to_explicit_in_memory_test_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PERSISTENCE_BACKEND", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")

    settings = DatabaseSettings.from_env()

    assert settings.backend == "memory"
    assert settings.database_url is None


def test_production_requires_postgresql(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PERSISTENCE_BACKEND", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "production")

    with pytest.raises(PersistenceConfigurationError, match="DATABASE_URL"):
        DatabaseSettings.from_env()


def test_production_rejects_memory_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("PERSISTENCE_BACKEND", "memory")
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(PersistenceConfigurationError, match="Production requires"):
        DatabaseSettings.from_env()


def test_postgres_backend_requires_a_postgresql_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("PERSISTENCE_BACKEND", "postgres")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///tmp/app.db")

    with pytest.raises(PersistenceConfigurationError, match="PostgreSQL URL"):
        DatabaseSettings.from_env()


def test_postgres_backend_accepts_postgresql_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("PERSISTENCE_BACKEND", "postgres")
    monkeypatch.setenv("DATABASE_URL", "postgresql://app:secret@localhost:5432/app")

    settings = DatabaseSettings.from_env()

    assert settings.backend == "postgres"
    assert settings.database_url == "postgresql://app:secret@localhost:5432/app"


def test_pool_sizes_must_be_positive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_POOL_MAX_SIZE", "0")

    with pytest.raises(PersistenceConfigurationError, match="greater than zero"):
        DatabaseSettings.from_env()
