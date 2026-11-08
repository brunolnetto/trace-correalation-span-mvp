from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import urlsplit


class PersistenceConfigurationError(ValueError):
    """Raised when persistence settings are unsafe or incomplete."""


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    """Validated PostgreSQL connection settings.

    The application may use its in-memory test doubles outside production.
    Production defaults to PostgreSQL and never silently degrades to memory.
    """

    backend: str
    database_url: str | None
    connect_timeout_seconds: int = 5
    pool_min_size: int = 1
    pool_max_size: int = 10

    @classmethod
    def from_env(cls) -> DatabaseSettings:
        environment = os.getenv("ENVIRONMENT", "development").strip().lower()
        default_backend = "postgres" if environment == "production" else "memory"
        backend = os.getenv("PERSISTENCE_BACKEND", default_backend).strip().lower()
        database_url = os.getenv("DATABASE_URL")

        if backend not in {"memory", "postgres"}:
            raise PersistenceConfigurationError(
                "PERSISTENCE_BACKEND must be either 'memory' or 'postgres'"
            )
        if environment == "production" and backend != "postgres":
            raise PersistenceConfigurationError(
                "Production requires PERSISTENCE_BACKEND=postgres"
            )
        if backend == "postgres":
            if not database_url:
                raise PersistenceConfigurationError(
                    "DATABASE_URL is required when PostgreSQL persistence is enabled"
                )
            parsed = urlsplit(database_url)
            if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
                raise PersistenceConfigurationError(
                    "DATABASE_URL must be a PostgreSQL URL with a hostname"
                )

        pool_min_size = _positive_int_env("DATABASE_POOL_MIN_SIZE", 1)
        pool_max_size = _positive_int_env("DATABASE_POOL_MAX_SIZE", 10)
        if pool_min_size > pool_max_size:
            raise PersistenceConfigurationError(
                "DATABASE_POOL_MIN_SIZE cannot exceed DATABASE_POOL_MAX_SIZE"
            )

        return cls(
            backend=backend,
            database_url=database_url,
            connect_timeout_seconds=_positive_int_env(
                "DATABASE_CONNECT_TIMEOUT_SECONDS", 5
            ),
            pool_min_size=pool_min_size,
            pool_max_size=pool_max_size,
        )


def _positive_int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise PersistenceConfigurationError(f"{name} must be an integer") from exc
    if value < 1:
        raise PersistenceConfigurationError(f"{name} must be greater than zero")
    return value
