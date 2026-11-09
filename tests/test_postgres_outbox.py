from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterator
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from app.event_collector.store import RawEvent
from app.inventory.postgres_store import PostgresInventoryStore
from app.inventory.store import InventoryReceiptRequest, ProductCreate
from app.observability import correlation_id_context
from app.order.repository import PostgresOrderRepository
from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager
from app.persistence.migrations import MIGRATIONS_DIR, apply_migrations
from app.persistence.outbox import OutboxDispatcher, PostgresOutboxRepository
from app.supplier_simulator.postgres_store import PostgresSupplierSimulation
from app.supplier_simulator.store import ReorderPolicy


@pytest.fixture
def outbox_database_url() -> Iterator[str]:
    database_url = os.getenv("POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("POSTGRES_TEST_URL is required for outbox integration tests")

    schema = f"test_outbox_{uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    parsed_url = urlsplit(database_url)
    query = parse_qsl(parsed_url.query, keep_blank_values=True)
    query.append(("options", f"-c search_path={schema}"))
    isolated_url = urlunsplit(
        parsed_url._replace(query=urlencode(query, quote_via=quote))
    )
    try:
        with psycopg.connect(isolated_url) as connection:
            apply_migrations(connection, MIGRATIONS_DIR)
        yield isolated_url
    finally:
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


async def open_database(database_url: str) -> DatabaseManager:
    database = DatabaseManager(pool_name=f"outbox-test-{uuid4().hex[:8]}")
    await database.open(
        DatabaseSettings(
            backend="postgres",
            database_url=database_url,
            connect_timeout_seconds=5,
            pool_min_size=1,
            pool_max_size=8,
        )
    )
    return database


async def insert_outbox_event(
    database: DatabaseManager,
    *,
    name: str = "test.outbox.event",
    aggregate_id: str | None = None,
) -> str:
    async with database.connection() as connection:
        cursor = await connection.execute(
            """
            SELECT enqueue_outbox_event(
                'outbox-test',
                %s,
                'test-aggregate',
                %s,
                %s
            ) AS event_id
            """,
            (
                name,
                aggregate_id or str(uuid4()),
                Jsonb({"test": True}),
            ),
        )
        row = await cursor.fetchone()
        return str(row["event_id"])


async def outbox_rows(database: DatabaseManager) -> list[dict]:
    async with database.connection() as connection:
        cursor = await connection.execute(
            """
            SELECT outbox_id, event_id, event_name, correlation_id,
                   event_payload, status, attempts, last_error
            FROM outbox_events
            ORDER BY outbox_id
            """
        )
        return [dict(row) for row in await cursor.fetchall()]


@pytest.mark.asyncio
async def test_domain_rollback_also_rolls_back_outbox_event(
    outbox_database_url: str,
) -> None:
    database = await open_database(outbox_database_url)
    try:
        with pytest.raises(RuntimeError, match="simulate domain rollback"):
            async with database.connection() as connection:
                await connection.execute(
                    """
                    INSERT INTO products (sku, name, category, unit_price)
                    VALUES ('ROLLBACK-001', 'Rollback product', 'test', 1.00)
                    """
                )
                raise RuntimeError("simulate domain rollback")

        async with database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM products
                     WHERE sku = 'ROLLBACK-001') AS product_count,
                    (SELECT COUNT(*) FROM outbox_events
                     WHERE event_name = 'product.created'
                       AND event_payload->'attributes'->>'sku' = 'ROLLBACK-001')
                     AS outbox_count
                """
            )
            row = await cursor.fetchone()
        assert row["product_count"] == 0
        assert row["outbox_count"] == 0
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_outbox_event_identity_and_payload_are_immutable(
    outbox_database_url: str,
) -> None:
    database = await open_database(outbox_database_url)
    try:
        event_id = await insert_outbox_event(database)
        with pytest.raises(psycopg.Error, match="identity and payload are immutable"):
            async with database.connection() as connection:
                await connection.execute(
                    """
                    UPDATE outbox_events
                    SET event_payload = jsonb_set(
                        event_payload,
                        '{attributes,test}',
                        'false'::jsonb
                    )
                    WHERE event_id = %s
                    """,
                    (event_id,),
                )

        rows = await outbox_rows(database)
        assert len(rows) == 1
        assert str(rows[0]["event_id"]) == event_id
        assert rows[0]["status"] == "pending"
        assert rows[0]["event_payload"]["attributes"]["test"] is True
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_inventory_triggers_emit_correlated_events_once(
    outbox_database_url: str,
) -> None:
    database = await open_database(outbox_database_url)
    correlation_id = "550e8400-e29b-41d4-a716-446655440901"
    token = correlation_id_context.set(correlation_id)
    try:
        inventory = PostgresInventoryStore(database)
        product = await inventory.create_product(
            ProductCreate(
                sku="OUTBOX-INV-001",
                name="Transactional outbox inventory",
                category="test",
                unit_price=Decimal("19.90"),
            )
        )
        request = InventoryReceiptRequest(
            quantity=5,
            idempotency_key="outbox-inventory-receipt-001",
            reference="outbox-test",
        )
        first = await inventory.receive_stock(product["product_id"], request)
        replay = await inventory.receive_stock(product["product_id"], request)
        assert first["receipt_id"] == replay["receipt_id"]

        rows = await outbox_rows(database)
        relevant = [
            row for row in rows
            if row["event_name"] in {
                "product.created",
                "inventory.stock.received",
            }
        ]
        assert [row["event_name"] for row in relevant] == [
            "product.created",
            "inventory.stock.received",
        ]
        for row in relevant:
            event = RawEvent.model_validate(row["event_payload"])
            assert event.event_id == row["event_id"]
            assert event.correlation_id == correlation_id
            assert event.source["message"] == event.event_name
        assert relevant[1]["event_payload"]["attributes"]["receipt_id"] == (
            first["receipt_id"]
        )
    finally:
        correlation_id_context.reset(token)
        await database.close()


@pytest.mark.asyncio
async def test_order_and_supplier_events_are_written_with_domain_transitions(
    outbox_database_url: str,
) -> None:
    database = await open_database(outbox_database_url)
    correlation_id = "550e8400-e29b-41d4-a716-446655440902"
    token = correlation_id_context.set(correlation_id)
    try:
        inventory = PostgresInventoryStore(database)
        product = await inventory.create_product(
            ProductCreate(
                sku="OUTBOX-DOM-001",
                name="Transactional outbox domain product",
                category="test",
                unit_price=Decimal("8.25"),
            )
        )
        order_repository = PostgresOrderRepository(database)
        pending = {
            "order_id": 801,
            "requested_product_id": product["product_id"],
            "product_id": product["product_id"],
            "quantity": 2,
            "unit_price": Decimal("8.25"),
            "total_amount": Decimal("16.50"),
            "saga_id": "outbox-saga-801",
            "reservation_id": None,
            "payment_id": None,
            "status": "pending",
            "failure_category": None,
            "correlation_id": correlation_id,
        }
        await order_repository.save_outcome(pending)
        await order_repository.save_outcome(
            {**pending, "status": "processed"}
        )

        supplier = PostgresSupplierSimulation(database)
        await supplier.initialize()
        offering = await supplier._get_offering("BEANS-250")
        supplier_product = await inventory.create_product(
            ProductCreate(
                sku=offering["inventory_sku"],
                name=offering["name"],
                category=offering["category"],
                unit_price=offering["unit_price"],
            )
        )
        await supplier.save_onboarding("BEANS-250", supplier_product)
        await supplier.set_policy(
            ReorderPolicy(
                product_id=supplier_product["product_id"],
                reorder_point=10,
                target_stock=20,
                lead_time_seconds=3600,
            )
        )
        decision = await supplier.evaluate_product(
            supplier_product["product_id"],
            {
                "on_hand": 0,
                "reserved": 0,
                "available_quantity": 0,
            },
        )
        assert decision["decision"] == "purchase_order_created"
        purchase_order_id = decision["purchase_order"]["purchase_order_id"]
        await supplier.order_purchase_order(purchase_order_id)
        await supplier.advance_clock(3600)
        await supplier.begin_receipt(purchase_order_id)
        await supplier.complete_receipt(purchase_order_id, "receipt-outbox-801")

        rows = await outbox_rows(database)
        names = [row["event_name"] for row in rows]
        assert names.count("order.process.started") == 1
        assert names.count("order.process.completed") == 1
        assert names.count("supplier.product.onboarded") == 1
        assert names.count("supplier.reorder_policy.created") == 1
        assert names.count("supplier.reorder_policy.updated") == 1
        assert names.count("supplier.replenishment.evaluated") == 1
        assert names.count("supplier.purchase_order.created") == 1
        assert names.count("supplier.purchase_order.ordered") == 1
        assert names.count("supplier.purchase_order.receiving") == 1
        assert names.count("supplier.purchase_order.received") == 1

        for row in rows:
            if row["event_name"].startswith(("order.", "supplier.")):
                assert row["correlation_id"] == correlation_id
                RawEvent.model_validate(row["event_payload"])
    finally:
        correlation_id_context.reset(token)
        await database.close()


@pytest.mark.asyncio
async def test_pending_event_is_dispatched_after_fresh_database_pool_restart(
    outbox_database_url: str,
) -> None:
    database = await open_database(outbox_database_url)
    expected_id = await insert_outbox_event(database)
    await database.close()

    restarted_database = await open_database(outbox_database_url)
    try:
        repository = PostgresOutboxRepository(restarted_database)
        received: list[str] = []

        def collector(request: httpx.Request) -> httpx.Response:
            event = json.loads(request.content)["events"][0]
            received.append(event["event_id"])
            return httpx.Response(
                202,
                json={"accepted": 1, "duplicates": 0, "quarantined": 0},
            )

        dispatcher = OutboxDispatcher(
            repository,
            collector_url="http://collector.test",
            batch_size=10,
            transport=httpx.MockTransport(collector),
            worker_id="worker-after-restart",
        )
        result = await dispatcher.dispatch_once()
        assert result["delivered"] == 1
        assert received == [expected_id]

        rows = await outbox_rows(restarted_database)
        assert len(rows) == 1
        assert rows[0]["status"] == "delivered"
        assert rows[0]["attempts"] == 1
    finally:
        await restarted_database.close()


@pytest.mark.asyncio
async def test_multiple_dispatchers_claim_disjoint_batches_and_expired_leases_recover(
    outbox_database_url: str,
) -> None:
    database = await open_database(outbox_database_url)
    try:
        repository = PostgresOutboxRepository(database)
        expected = {
            await insert_outbox_event(database, aggregate_id=f"aggregate-{index}")
            for index in range(6)
        }
        first, second = await asyncio.gather(
            repository.claim_batch(
                lease_owner="worker-a",
                batch_size=4,
                lease_seconds=30,
            ),
            repository.claim_batch(
                lease_owner="worker-b",
                batch_size=4,
                lease_seconds=30,
            ),
        )
        first_ids = {str(row["event_id"]) for row in first}
        second_ids = {str(row["event_id"]) for row in second}
        assert first_ids.isdisjoint(second_ids)
        assert first_ids | second_ids == expected

        reclaimed_id = next(iter(first_ids))
        async with database.connection() as connection:
            await connection.execute(
                """
                UPDATE outbox_events
                SET lease_expires_at = now() - interval '1 second'
                WHERE event_id = %s
                """,
                (reclaimed_id,),
            )
        reclaimed = await repository.claim_batch(
            lease_owner="worker-c",
            batch_size=1,
            lease_seconds=30,
        )
        assert len(reclaimed) == 1
        assert str(reclaimed[0]["event_id"]) == reclaimed_id
        assert reclaimed[0]["attempts"] == 2
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_collector_ack_lost_after_persist_is_safely_redelivered(
    outbox_database_url: str,
) -> None:
    database = await open_database(outbox_database_url)
    token = correlation_id_context.set(
        "550e8400-e29b-41d4-a716-446655440903"
    )
    try:
        repository = PostgresOutboxRepository(database)
        expected_id = await insert_outbox_event(database)
        received_ids: set[str] = set()
        request_ids: list[str] = []
        request_correlations: list[str | None] = []
        event_correlations: list[str | None] = []

        def collector(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            event = payload["events"][0]
            event_id = event["event_id"]
            request_ids.append(event_id)
            request_correlations.append(request.headers.get("x-correlation-id"))
            event_correlations.append(event.get("correlation_id"))
            duplicate = event_id in received_ids
            received_ids.add(event_id)
            return httpx.Response(
                202,
                json={
                    "accepted": 0 if duplicate else 1,
                    "duplicates": 1 if duplicate else 0,
                    "quarantined": 0,
                },
            )

        dispatcher = OutboxDispatcher(
            repository,
            collector_url="http://collector.test",
            batch_size=1,
            base_backoff_seconds=0,
            max_backoff_seconds=0,
            transport=httpx.MockTransport(collector),
            worker_id="worker-crash-window",
        )
        async def lose_database_ack(**_kwargs: Any) -> bool:
            raise RuntimeError("simulated process loss before local acknowledgement")

        repository.mark_delivered = lose_database_ack  # type: ignore[method-assign]
        first = await dispatcher.dispatch_once()
        assert first["claimed"] == 1
        assert first["delivered"] == 0
        assert received_ids == {expected_id}
        await database.close()

        # Reopen a fresh pool as a new process would. The Collector has already
        # persisted the event, while PostgreSQL still has only an expired lease.
        database = await open_database(outbox_database_url)
        repository = PostgresOutboxRepository(database)
        async with database.connection() as connection:
            await connection.execute(
                """
                UPDATE outbox_events
                SET lease_expires_at = now() - interval '1 second'
                WHERE event_id = %s
                """,
                (expected_id,),
            )

        restarted_dispatcher = OutboxDispatcher(
            repository,
            collector_url="http://collector.test",
            batch_size=1,
            base_backoff_seconds=0,
            max_backoff_seconds=0,
            transport=httpx.MockTransport(collector),
            worker_id="worker-after-crash",
        )
        second = await restarted_dispatcher.dispatch_once()
        assert second["delivered"] == 1
        assert request_ids == [expected_id, expected_id]
        assert request_correlations == [
            "550e8400-e29b-41d4-a716-446655440903",
            "550e8400-e29b-41d4-a716-446655440903",
        ]
        assert event_correlations == request_correlations
        assert len(received_ids) == 1

        rows = await outbox_rows(database)
        assert len(rows) == 1
        assert rows[0]["status"] == "delivered"
        assert rows[0]["attempts"] == 2
    finally:
        correlation_id_context.reset(token)
        await database.close()


@pytest.mark.asyncio
async def test_retryable_outbox_event_blocks_after_attempt_limit(
    outbox_database_url: str,
) -> None:
    database = await open_database(outbox_database_url)
    try:
        repository = PostgresOutboxRepository(database)
        event_id = await insert_outbox_event(database, name="test.retry-ceiling")

        def unavailable(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, json={"detail": "collector unavailable"})

        dispatcher = OutboxDispatcher(
            repository,
            collector_url="http://collector.test",
            batch_size=1,
            max_attempts=2,
            base_backoff_seconds=0,
            max_backoff_seconds=0,
            transport=httpx.MockTransport(unavailable),
            worker_id="worker-retry-ceiling",
        )
        first = await dispatcher.dispatch_once()
        second = await dispatcher.dispatch_once()
        assert first["retried"] == 1
        assert second["blocked"] == 1

        rows = await outbox_rows(database)
        row = next(item for item in rows if str(item["event_id"]) == event_id)
        assert row["status"] == "blocked"
        assert row["attempts"] == 2
        assert row["last_error"] is not None
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_transient_errors_retry_and_permanent_rejections_are_blocked(
    outbox_database_url: str,
) -> None:
    database = await open_database(outbox_database_url)
    try:
        repository = PostgresOutboxRepository(database)
        transient_id = await insert_outbox_event(database, name="test.transient")
        blocked_id = await insert_outbox_event(database, name="test.permanent")

        def collector(request: httpx.Request) -> httpx.Response:
            event = json.loads(request.content)["events"][0]
            if event["event_name"] == "test.transient":
                return httpx.Response(503, json={"detail": "temporary outage"})
            return httpx.Response(422, json={"detail": "invalid event"})

        dispatcher = OutboxDispatcher(
            repository,
            collector_url="http://collector.test",
            batch_size=1,
            max_attempts=5,
            base_backoff_seconds=60,
            max_backoff_seconds=60,
            transport=httpx.MockTransport(collector),
            worker_id="worker-retry",
        )
        first = await dispatcher.dispatch_once()
        assert first["retried"] == 1
        rows = await outbox_rows(database)
        transient = next(row for row in rows if str(row["event_id"]) == transient_id)
        assert transient["status"] == "pending"
        assert transient["attempts"] == 1
        assert transient["last_error"] is not None

        # The transient event has a future available_at; make only the
        # permanent event eligible for the next bounded claim.
        second = await dispatcher.dispatch_once()
        assert second["blocked"] == 1
        rows = await outbox_rows(database)
        blocked = next(row for row in rows if str(row["event_id"]) == blocked_id)
        assert blocked["status"] == "blocked"
        assert blocked["attempts"] == 1
        assert blocked["last_error"] is not None

        stats = await repository.stats()
        assert stats["pending_count"] == 1
        assert stats["blocked_count"] == 1
        assert stats["oldest_pending_age_seconds"] >= 0
        blocked_records = await repository.list_blocked()
        assert [str(row["event_id"]) for row in blocked_records] == [blocked_id]
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_committed_domain_event_survives_producer_restart_before_dispatch(
    outbox_database_url: str,
) -> None:
    producer_database = await open_database(outbox_database_url)
    correlation_id = "550e8400-e29b-41d4-a716-446655440903"
    token = correlation_id_context.set(correlation_id)
    try:
        product = await PostgresInventoryStore(producer_database).create_product(
            ProductCreate(
                sku="OUTBOX-RESTART-001",
                name="Committed before dispatcher restart",
                category="test",
                unit_price=Decimal("14.50"),
            )
        )
        product_id = product["product_id"]
    finally:
        correlation_id_context.reset(token)
        await producer_database.close()

    # A new pool represents the producer process starting again. The outbox
    # event must already be committed even though no dispatch was attempted.
    dispatcher_database = await open_database(outbox_database_url)
    try:
        rows = await outbox_rows(dispatcher_database)
        created = next(
            row
            for row in rows
            if row["event_name"] == "product.created"
            and row["event_payload"]["attributes"]["product_id"] == product_id
        )
        assert created["status"] == "pending"
        persisted = RawEvent.model_validate(created["event_payload"])
        assert persisted.correlation_id == correlation_id
        assert persisted.event_id == created["event_id"]

        delivered_ids: list[str] = []

        def collector(request: httpx.Request) -> httpx.Response:
            event = json.loads(request.content)["events"][0]
            delivered_ids.append(event["event_id"])
            return httpx.Response(
                202,
                json={
                    "accepted": 1,
                    "duplicates": 0,
                    "quarantined": 0,
                },
            )

        dispatcher = OutboxDispatcher(
            PostgresOutboxRepository(dispatcher_database),
            collector_url="http://collector.test",
            batch_size=10,
            base_backoff_seconds=0,
            max_backoff_seconds=0,
            transport=httpx.MockTransport(collector),
            worker_id="worker-after-producer-restart",
        )
        result = await dispatcher.dispatch_once()
        assert result["delivered"] == 1
        assert delivered_ids == [str(persisted.event_id)]

        rows = await outbox_rows(dispatcher_database)
        delivered = next(row for row in rows if row["outbox_id"] == created["outbox_id"])
        assert delivered["status"] == "delivered"
    finally:
        await dispatcher_database.close()


@pytest.mark.asyncio
async def test_transport_failure_retries_same_event_and_correlation(
    outbox_database_url: str,
) -> None:
    database = await open_database(outbox_database_url)
    correlation_id = "550e8400-e29b-41d4-a716-446655440904"
    token = correlation_id_context.set(correlation_id)
    try:
        expected_id = await insert_outbox_event(
            database,
            name="test.transport-retry",
            aggregate_id="transport-retry-aggregate",
        )
    finally:
        correlation_id_context.reset(token)

    try:
        requests: list[dict[str, Any]] = []

        def collector(request: httpx.Request) -> httpx.Response:
            event = json.loads(request.content)["events"][0]
            requests.append(event)
            if len(requests) == 1:
                raise httpx.ConnectError(
                    "simulated connection failure before Collector persistence",
                    request=request,
                )
            return httpx.Response(
                202,
                json={
                    "accepted": 1,
                    "duplicates": 0,
                    "quarantined": 0,
                },
            )

        dispatcher = OutboxDispatcher(
            PostgresOutboxRepository(database),
            collector_url="http://collector.test",
            batch_size=1,
            base_backoff_seconds=0,
            max_backoff_seconds=0,
            transport=httpx.MockTransport(collector),
            worker_id="worker-transport-retry",
        )
        first = await dispatcher.dispatch_once()
        second = await dispatcher.dispatch_once()

        assert first["retried"] == 1
        assert second["delivered"] == 1
        assert len(requests) == 2
        assert requests[0] == requests[1]
        assert requests[0]["event_id"] == expected_id
        assert requests[0]["correlation_id"] == correlation_id

        rows = await outbox_rows(database)
        assert len(rows) == 1
        assert rows[0]["status"] == "delivered"
        assert rows[0]["attempts"] == 2
    finally:
        await database.close()
