from __future__ import annotations

import os
import shutil
from pathlib import Path
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from app.persistence.migrations import (
    MIGRATIONS_DIR,
    MigrationChecksumMismatch,
    apply_migrations,
)


@pytest.fixture
def isolated_postgres_schema():
    database_url = os.getenv("POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("POSTGRES_TEST_URL is required for PostgreSQL migration tests")

    schema = f"test_migrations_{uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
        )

    isolated_url = make_conninfo(
        database_url,
        options=f"-c search_path={schema}",
    )
    try:
        yield isolated_url
    finally:
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


def test_migrations_apply_to_empty_schema_repeat_and_reject_checksum_drift(
    isolated_postgres_schema: str,
    tmp_path: Path,
) -> None:
    with psycopg.connect(isolated_postgres_schema, row_factory=dict_row) as connection:
        applied = apply_migrations(connection, MIGRATIONS_DIR)
        assert applied == [
            "0001_core_persistence",
            "0002_protect_movement_truncate",
        ]

        tables = {
            row["table_name"]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                """
            ).fetchall()
        }
        assert {
            "schema_migrations",
            "products",
            "inventory_balances",
            "reservations",
            "inventory_movements",
            "idempotency_records",
            "orders",
        }.issubset(tables)

        assert apply_migrations(connection, MIGRATIONS_DIR) == []
        migration_count = connection.execute(
            "SELECT count(*) AS count FROM schema_migrations"
        ).fetchone()["count"]
        assert migration_count == 2

    altered_migrations = tmp_path / "migrations"
    shutil.copytree(MIGRATIONS_DIR, altered_migrations)
    migration_file = altered_migrations / "0001_core_persistence.sql"
    migration_file.write_text(
        migration_file.read_text(encoding="utf-8")
        + "\n-- Deliberate checksum drift for the migration test.\n",
        encoding="utf-8",
    )

    with psycopg.connect(isolated_postgres_schema) as connection:
        with pytest.raises(MigrationChecksumMismatch, match="Checksum mismatch"):
            apply_migrations(connection, altered_migrations)
