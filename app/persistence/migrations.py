from __future__ import annotations

import hashlib
import logging
import os
import re
from collections.abc import Mapping
from pathlib import Path

import psycopg
from psycopg import Connection

logger = logging.getLogger(__name__)
MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
ADVISORY_LOCK_ID = 210021
_VERSION_PATTERN = re.compile(r"^[0-9]+_[a-z0-9_]+$")


class MigrationChecksumMismatch(RuntimeError):
    """An already-applied migration was changed in place."""


def apply_migrations(
    connection: Connection,
    migrations_dir: Path = MIGRATIONS_DIR,
) -> list[str]:
    """Apply pending SQL migrations under one transaction-scoped advisory lock.

    A migration version is its filename without the .sql suffix. Applied files
    are immutable: any checksum difference aborts the transaction.
    """
    migration_files = sorted(migrations_dir.glob("*.sql"))
    applied: list[str] = []

    with connection.transaction():
        connection.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (ADVISORY_LOCK_ID,),
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR(128) PRIMARY KEY,
                checksum CHAR(64) NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )

        existing_rows = connection.execute(
            "SELECT version, checksum FROM schema_migrations"
        ).fetchall()
        existing = {
            (
                row["version"] if isinstance(row, Mapping) else row[0]
            ): (
                row["checksum"].strip()
                if isinstance(row, Mapping)
                else row[1].strip()
            )
            for row in existing_rows
        }

        for migration_file in migration_files:
            version = migration_file.stem
            if not _VERSION_PATTERN.fullmatch(version):
                raise ValueError(f"Invalid migration filename: {migration_file.name}")

            sql = migration_file.read_text(encoding="utf-8")
            checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
            previous_checksum = existing.get(version)
            if previous_checksum is not None:
                if previous_checksum != checksum:
                    raise MigrationChecksumMismatch(
                        f"Checksum mismatch for applied migration {version}: "
                        f"database={previous_checksum}, file={checksum}"
                    )
                logger.info("migration.already_applied", extra={"version": version})
                continue

            # prepare=False uses the simple query protocol, which permits a
            # migration file to contain multiple SQL statements and functions.
            connection.execute(sql, prepare=False)
            connection.execute(
                """
                INSERT INTO schema_migrations (version, checksum)
                VALUES (%s, %s)
                """,
                (version, checksum),
            )
            applied.append(version)
            logger.info("migration.applied", extra={"version": version})

    return applied


def main() -> int:
    database_url = os.getenv("DATABASE_URL", "")
    with psycopg.connect(database_url) as connection:
        applied = apply_migrations(connection)

    if applied:
        print(f"Applied migrations: {', '.join(applied)}")
    else:
        print("Database schema is up to date.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
