from __future__ import annotations

import asyncio
import os
from decimal import Decimal
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row

from app.persistence.config import DatabaseSettings
from app.persistence.database import DatabaseManager
from app.persistence.migrations import MIGRATIONS_DIR, apply_migrations
from app.supplier_simulator.postgres_store import PostgresSupplierSimulation
from app.supplier_simulator.store import ReorderPolicy


@pytest.fixture
def postgres_schema_url():
    database_url = os.getenv("POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("POSTGRES_TEST_URL is required for PostgreSQL integration tests")

    schema = f"test_supplier_{uuid4().hex}"
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
        with psycopg.connect(isolated_url, row_factory=dict_row) as connection:
            apply_migrations(connection, MIGRATIONS_DIR)
        yield isolated_url
    finally:
        with psycopg.connect(database_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                )
            )


class InventoryHTTPDouble:
    """Inventory API contract double with durable-key receipt semantics."""

    def __init__(self) -> None:
        self.products: dict[int, dict] = {}
        self.balances: dict[int, dict[str, int]] = {}
        self.receipts: dict[str, dict] = {}
        self.receipt_calls = 0
        self.fail_next_receipt: str | None = None

    def add_product(
        self, product_id: int, sku: str, *, on_hand: int, reserved: int = 0
    ) -> None:
        self.products[product_id] = {
            "product_id": product_id,
            "sku": sku,
            "name": "Inventory test product",
            "category": "test",
            "unit_price": "10.00",
            "is_active": True,
        }
        self.balances[product_id] = {
            "on_hand": on_hand,
            "reserved": reserved,
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.strip("/").split("/")
        if request.method == "GET" and parts == ["products"]:
            return httpx.Response(200, json=list(self.products.values()))
        if request.method == "GET" and len(parts) == 2 and parts[0] == "products":
            product = self.products.get(int(parts[1]))
            return (
                httpx.Response(200, json=product)
                if product is not None
                else httpx.Response(404)
            )
        if request.method == "GET" and len(parts) == 2 and parts[0] == "inventory":
            product_id = int(parts[1])
            balance = self.balances.get(product_id)
            if balance is None:
                return httpx.Response(404)
            available = balance["on_hand"] - balance["reserved"]
            return httpx.Response(
                200,
                json={
                    "product_id": product_id,
                    **balance,
                    "available_quantity": available,
                },
            )
        if (
            request.method == "POST"
            and len(parts) == 3
            and parts[0] == "inventory"
            and parts[2] == "receipts"
        ):
            self.receipt_calls += 1
            product_id = int(parts[1])
            payload = __import__("json").loads(request.content)
            key = f"{product_id}:{payload['idempotency_key']}"
            if self.fail_next_receipt == "before_commit":
                self.fail_next_receipt = None
                raise httpx.ConnectError(
                    "simulated Inventory outage before commit", request=request
                )
            if key not in self.receipts:
                self.balances[product_id]["on_hand"] += payload["quantity"]
                self.receipts[key] = {
                    "receipt_id": f"receipt-{len(self.receipts) + 1}",
                    "product_id": product_id,
                    "quantity": payload["quantity"],
                    "reference": payload.get("reference"),
                }
            if self.fail_next_receipt == "after_commit":
                self.fail_next_receipt = None
                raise httpx.ReadTimeout(
                    "simulated response loss after Inventory commit",
                    request=request,
                )
            return httpx.Response(201, json=self.receipts[key])
        return httpx.Response(404, json={"detail": "not found"})


async def _open_store(database_url: str, name: str):
    manager = DatabaseManager(pool_name=name)
    await manager.open(
        DatabaseSettings(
            backend="postgres",
            database_url=database_url,
            connect_timeout_seconds=5,
            pool_min_size=1,
            pool_max_size=4,
        )
    )
    store = PostgresSupplierSimulation(manager)
    await store.open()
    return manager, store


async def _create_product(
    manager: DatabaseManager, sku: str
) -> int:
    async with manager.connection() as connection:
        cursor = await connection.execute(
            """
            INSERT INTO products (sku, name, category, unit_price)
            VALUES (%s, 'Supplier test product', 'test', %s)
            RETURNING product_id
            """,
            (sku, Decimal("10.00")),
        )
        row = await cursor.fetchone()
        await connection.execute(
            """
            INSERT INTO inventory_balances (product_id, on_hand, reserved)
            VALUES (%s, 0, 0)
            """,
            (row["product_id"],),
        )
        return row["product_id"]


@pytest.mark.asyncio
async def test_supplier_offerings_onboarding_and_policy_survive_restart(
    postgres_schema_url: str,
):
    manager, store = await _open_store(postgres_schema_url, "supplier-onboarding-a")
    inventory = InventoryHTTPDouble()
    product_id = await _create_product(manager, "COFFEE-BEANS-250G")
    inventory.add_product(product_id, "COFFEE-BEANS-250G", on_hand=0)
    transport = httpx.MockTransport(inventory.handler)

    try:
        offerings = await store.list_offerings()
        assert len(offerings) == 3
        assert {item["supplier_sku"] for item in offerings} == {
            "BEANS-250",
            "FILTER-100",
            "MUG-350",
        }

        # A 409 followed by SKU lookup models Inventory committing product
        # creation immediately before the supplier process loses its checkpoint.
        async with httpx.AsyncClient(
            transport=transport, base_url="http://inventory.test"
        ) as client:
            onboarded = await store.onboard_product("BEANS-250", client, "onboard-1")
        assert onboarded["product"]["product_id"] == product_id
        assert onboarded["reorder_policy"]["version"] == 1

        customized = await store.set_policy(
            ReorderPolicy(
                product_id=product_id,
                reorder_point=9,
                target_stock=40,
                lead_time_seconds=900,
            )
        )
        assert customized["version"] == 2
    finally:
        await manager.close()

    # A new pool/store represents a new Supplier Simulator process.
    restarted_manager, restarted_store = await _open_store(
        postgres_schema_url, "supplier-onboarding-b"
    )
    try:
        policies = await restarted_store.list_policies()
        assert len(policies) == 1
        assert policies[0] == customized
        async with httpx.AsyncClient(
            transport=transport, base_url="http://inventory.test"
        ) as client:
            replayed = await restarted_store.onboard_product(
                "BEANS-250", client, "onboard-2"
            )
        assert replayed["product"]["product_id"] == product_id
        assert replayed["reorder_policy"] == customized

        async with restarted_manager.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT product_id, onboarded_at
                FROM supplier_offerings
                WHERE supplier_sku = 'BEANS-250'
                """
            )
            mapping = await cursor.fetchone()
            assert mapping["product_id"] == product_id
            assert mapping["onboarded_at"] is not None
    finally:
        await restarted_manager.close()


@pytest.mark.asyncio
async def test_concurrent_replenishment_evaluations_create_one_active_purchase_order(
    postgres_schema_url: str,
):
    manager_a, store_a = await _open_store(postgres_schema_url, "supplier-evaluate-a")
    manager_b, store_b = await _open_store(postgres_schema_url, "supplier-evaluate-b")
    inventory = InventoryHTTPDouble()
    product_id = await _create_product(manager_a, "SKU-SUPPLIER-RACE")
    inventory.add_product(product_id, "SKU-SUPPLIER-RACE", on_hand=5)
    await store_a.set_policy(
        ReorderPolicy(
            product_id=product_id,
            reorder_point=5,
            target_stock=12,
            lead_time_seconds=60,
        )
    )

    try:
        async with (
            httpx.AsyncClient(
                transport=httpx.MockTransport(inventory.handler),
                base_url="http://inventory.test",
            ) as client_a,
            httpx.AsyncClient(
                transport=httpx.MockTransport(inventory.handler),
                base_url="http://inventory.test",
            ) as client_b,
        ):
            result_a, result_b = await asyncio.gather(
                store_a.evaluate_replenishment(client_a, "evaluate-a", product_id=product_id),
                store_b.evaluate_replenishment(client_b, "evaluate-b", product_id=product_id),
            )

        decisions = [result_a[0]["decision"], result_b[0]["decision"]]
        assert sorted(decisions) == [
            "open_purchase_order_exists",
            "purchase_order_created",
        ]
        purchase_orders = await store_a.list_purchase_orders()
        assert len(purchase_orders) == 1
        purchase_order = purchase_orders[0]
        assert purchase_order["quantity"] == 7
        assert purchase_order["status"] == "created"

        transitions = await store_b.list_purchase_order_transitions(
            purchase_order["purchase_order_id"]
        )
        assert [item["to_status"] for item in transitions] == ["created"]

        async with manager_a.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT count(*) AS line_count, sum(quantity) AS total_quantity
                FROM supplier_purchase_order_lines
                WHERE purchase_order_id = %s
                """,
                (purchase_order["purchase_order_id"],),
            )
            line = await cursor.fetchone()
            assert line == {"line_count": 1, "total_quantity": 7}
    finally:
        await manager_b.close()
        await manager_a.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_point", ["before_commit", "after_commit"])
async def test_receiving_po_recovers_across_supplier_restart_without_double_credit(
    postgres_schema_url: str,
    failure_point: str,
):
    manager, store = await _open_store(
        postgres_schema_url, f"supplier-receive-{failure_point}-a"
    )
    inventory = InventoryHTTPDouble()
    product_id = await _create_product(
        manager, f"SKU-SUPPLIER-RECEIVE-{failure_point.upper()}"
    )
    inventory.add_product(product_id, f"SKU-SUPPLIER-RECEIVE-{failure_point.upper()}", on_hand=5)
    await store.set_policy(
        ReorderPolicy(
            product_id=product_id,
            reorder_point=5,
            target_stock=12,
            lead_time_seconds=60,
        )
    )
    transport = httpx.MockTransport(inventory.handler)

    try:
        async with httpx.AsyncClient(
            transport=transport, base_url="http://inventory.test"
        ) as client:
            decision = (
                await store.evaluate_replenishment(
                    client, "replenishment", product_id=product_id
                )
            )[0]
            purchase_order_id = decision["purchase_order"]["purchase_order_id"]
            ordered = await store.order_purchase_order(
                purchase_order_id, "order-correlation"
            )
            assert ordered["status"] == "ordered"
            await store.advance_clock(60)
            inventory.fail_next_receipt = failure_point

            expected_error = (
                httpx.ConnectError
                if failure_point == "before_commit"
                else httpx.ReadTimeout
            )
            with pytest.raises(expected_error):
                await store.receive_purchase_order(
                    purchase_order_id, client, "receipt-correlation"
                )

            pending = await store.get_purchase_order(purchase_order_id)
            assert pending["status"] == "receiving"
            assert pending["receipt_id"] is None
            assert inventory.balances[product_id]["on_hand"] == (
                5 if failure_point == "before_commit" else 12
            )
            persisted_clock = await store.clock()
    finally:
        await manager.close()

    # Process restart: the row remains in receiving and simulated time is not reset.
    restarted_manager, restarted_store = await _open_store(
        postgres_schema_url, f"supplier-receive-{failure_point}-b"
    )
    try:
        assert await restarted_store.clock() == persisted_clock
        async with httpx.AsyncClient(
            transport=transport, base_url="http://inventory.test"
        ) as client:
            received = await restarted_store.receive_purchase_order(
                purchase_order_id, client, "receipt-retry-correlation"
            )

        assert received["status"] == "received"
        assert received["receipt_id"] == inventory.receipts[
            f"{product_id}:supplier-po:{purchase_order_id}"
        ]["receipt_id"]
        assert inventory.balances[product_id]["on_hand"] == 12
        assert inventory.receipt_calls == 2

        transitions = await restarted_store.list_purchase_order_transitions(
            purchase_order_id
        )
        assert [item["to_status"] for item in transitions] == [
            "created",
            "ordered",
            "receiving",
            "received",
        ]
        assert len({item["to_status"] for item in transitions}) == 4

        # The receipt is also idempotent after the PO has reached received.
        replay = await restarted_store.receive_purchase_order(
            purchase_order_id, client, "receipt-replay-correlation"
        )
        assert replay["receipt_id"] == received["receipt_id"]
        assert inventory.receipt_calls == 2
    finally:
        await restarted_manager.close()


@pytest.mark.asyncio
async def test_cancelled_purchase_order_history_is_durable_and_replenishment_can_retry(
    postgres_schema_url: str,
):
    manager, store = await _open_store(postgres_schema_url, "supplier-cancel")
    inventory = InventoryHTTPDouble()
    product_id = await _create_product(manager, "SKU-SUPPLIER-CANCEL")
    inventory.add_product(product_id, "SKU-SUPPLIER-CANCEL", on_hand=0)
    try:
        await store.set_policy(
            ReorderPolicy(
                product_id=product_id,
                reorder_point=0,
                target_stock=1,
                lead_time_seconds=0,
            )
        )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(inventory.handler),
            base_url="http://inventory.test",
        ) as client:
            first_decision = (
                await store.evaluate_replenishment(
                    client, "first-evaluation", product_id=product_id
                )
            )[0]
            first_id = first_decision["purchase_order"]["purchase_order_id"]
            cancelled = await store.cancel_purchase_order(first_id, "cancel-correlation")
            replayed_cancel = await store.cancel_purchase_order(first_id, "cancel-replay")
            assert cancelled["status"] == replayed_cancel["status"] == "cancelled"

            second_decision = (
                await store.evaluate_replenishment(
                    client, "second-evaluation", product_id=product_id
                )
            )[0]
            assert second_decision["decision"] == "purchase_order_created"
            assert second_decision["purchase_order"]["purchase_order_id"] != first_id

        history = await store.list_purchase_order_transitions(first_id)
        assert [item["to_status"] for item in history] == ["created", "cancelled"]
        orders = await store.list_purchase_orders()
        assert len(orders) == 2
        assert {order["status"] for order in orders} == {"created", "cancelled"}

        with pytest.raises(psycopg.Error, match="append-only"):
            async with manager.connection() as connection:
                await connection.execute(
                    """
                    UPDATE supplier_purchase_order_transitions
                    SET to_status = 'ordered'
                    WHERE purchase_order_id = %s
                    """,
                    (first_id,),
                )
    finally:
        await manager.close()
