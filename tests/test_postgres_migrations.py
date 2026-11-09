from __future__ import annotations

import os
import shutil
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

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

    parsed_url = urlsplit(database_url)
    query = parse_qsl(parsed_url.query, keep_blank_values=True)
    query.append(("options", f"-c search_path={schema}"))
    isolated_url = urlunsplit(
        parsed_url._replace(query=urlencode(query, quote_via=quote))
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
    with psycopg.connect(isolated_postgres_schema) as connection:
        applied = apply_migrations(connection, MIGRATIONS_DIR)
        assert applied == [
            "0001_core_persistence",
            "0002_protect_movement_truncate",
            "0003_persist_order_outcomes",
            "0004_persist_supplier_state",
            "0005_durable_order_saga",
            "0006_transactional_outbox",
        ]

        tables = {
            row[0]
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
            "supplier_offerings",
            "supplier_product_onboardings",
            "supplier_reorder_policies",
            "supplier_purchase_orders",
            "supplier_purchase_order_lines",
            "supplier_purchase_order_transitions",
            "supplier_simulation_clock",
            "order_saga_executions",
            "order_saga_steps",
            "order_payments",
            "outbox_events",
        }.issubset(tables)

        assert apply_migrations(connection, MIGRATIONS_DIR) == []
        migration_count = connection.execute(
            "SELECT count(*) AS count FROM schema_migrations"
        ).fetchone()[0]
        assert migration_count == 6

    altered_migrations = tmp_path / "migrations"
    shutil.copytree(MIGRATIONS_DIR, altered_migrations)
    migration_file = altered_migrations / "0001_core_persistence.sql"
    migration_file.write_text(
        migration_file.read_text(encoding="utf-8")
        + "\n-- Deliberate checksum drift for the migration test.\n",
        encoding="utf-8",
    )

    with (
        psycopg.connect(isolated_postgres_schema) as connection,
        pytest.raises(MigrationChecksumMismatch, match="Checksum mismatch"),
    ):
        apply_migrations(connection, altered_migrations)
