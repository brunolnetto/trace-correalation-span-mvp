from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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
        if request.method == "POST" and parts == ["products"]:
            payload = json.loads(request.content)
            existing = next(
                (
                    item
                    for item in self.products.values()
                    if item["sku"] == payload["sku"]
                ),
                None,
            )
            if existing is not None:
                return httpx.Response(409, json={"detail": "duplicate SKU"})
            product_id = max(self.products, default=0) + 1
            product = {"product_id": product_id, **payload, "is_active": True}
            self.products[product_id] = product
            self.balances[product_id] = {"on_hand": 0, "reserved": 0}
            return httpx.Response(201, json=product)
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
            payload = json.loads(request.content)
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
                store_a.evaluate_replenishment(
                    client_a, "evaluate-a", product_id=product_id
                ),
                store_b.evaluate_replenishment(
                    client_b, "evaluate-b", product_id=product_id
                ),
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

        # The partial unique index remains a final guard even if a future
        # writer bypasses the policy-row locking protocol.
        with pytest.raises(psycopg.errors.UniqueViolation):
            async with manager_a.connection() as connection:
                await connection.execute(
                    """
                    INSERT INTO supplier_purchase_orders (
                        purchase_order_id, product_id, policy_version, status,
                        reorder_point, target_stock,
                        inventory_position_at_order, on_hand_at_order,
                        reserved_at_order, on_order_at_order,
                        lead_time_seconds, created_at, updated_at
                    )
                    VALUES (
                        %s, %s, 1, 'created', 5, 12, 5, 5, 0, 0,
                        60, now(), now()
                    )
                    """,
                    (uuid4(), product_id),
                )

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
    inventory.add_product(
        product_id,
        f"SKU-SUPPLIER-RECEIVE-{failure_point.upper()}",
        on_hand=5,
    )
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
            cancelled = await store.cancel_purchase_order(
                first_id, "cancel-correlation"
            )
            replayed_cancel = await store.cancel_purchase_order(
                first_id, "cancel-replay"
            )
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



def _start_inventory_http_server(state: dict):
    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, status_code: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path == f"/products/{state['product_id']}":
                self._send_json(200, state["product"])
                return
            if path == f"/inventory/{state['product_id']}":
                balance = state["balance"]
                self._send_json(
                    200,
                    {
                        "product_id": state["product_id"],
                        **balance,
                        "available_quantity": (
                            balance["on_hand"] - balance["reserved"]
                        ),
                    },
                )
                return
            self._send_json(404, {"detail": "not found"})

        def do_POST(self) -> None:
            path = self.path.split("?", 1)[0]
            expected = f"/inventory/{state['product_id']}/receipts"
            if path != expected:
                self._send_json(404, {"detail": "not found"})
                return

            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length))
            state["receipt_calls"] += 1
            failure = state["fail_next_receipt"]
            if failure == "before_commit":
                state["fail_next_receipt"] = None
                self._send_json(503, {"detail": "injected pre-commit outage"})
                return

            key = payload["idempotency_key"]
            if key not in state["receipts"]:
                state["balance"]["on_hand"] += payload["quantity"]
                state["receipts"][key] = {
                    "receipt_id": f"receipt-{len(state['receipts']) + 1}",
                    "product_id": state["product_id"],
                    "quantity": payload["quantity"],
                    "reference": payload.get("reference"),
                }

            if failure == "after_commit":
                state["fail_next_receipt"] = None
                self._send_json(503, {"detail": "injected lost response"})
                return
            self._send_json(201, state["receipts"][key])

        def log_message(self, _format: str, *_args) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}"


def _start_supplier_api_process(
    database_url: str, inventory_api_url: str
) -> tuple[subprocess.Popen, str]:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    environment = os.environ.copy()
    environment.update(
        {
            "ENVIRONMENT": "development",
            "PERSISTENCE_BACKEND": "postgres",
            "DATABASE_URL": database_url,
            "DATABASE_POOL_MIN_SIZE": "1",
            "DATABASE_POOL_MAX_SIZE": "4",
            "INVENTORY_API_URL": inventory_api_url,
            "SERVICE_NAME": "supplier-restart-integration-test",
            "LOG_LEVEL": "WARNING",
        }
    )
    for name in (
        "EVENT_COLLECTOR_URL",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ):
        environment.pop(name, None)

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.supplier_simulator.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=os.path.dirname(os.path.dirname(__file__)),
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return process, f"http://127.0.0.1:{port}"


async def _wait_for_supplier_process(
    process: subprocess.Popen,
    base_url: str,
    *,
    timeout_seconds: float = 20,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    last_error: Exception | None = None
    async with httpx.AsyncClient(timeout=1) as client:
        while asyncio.get_running_loop().time() < deadline:
            if process.poll() is not None:
                raise AssertionError(
                    "Supplier Simulator exited during startup: "
                    f"{process.returncode}"
                )
            try:
                response = await client.get(f"{base_url}/healthz")
                if response.status_code == 200:
                    return
                last_error = AssertionError(
                    f"Supplier health returned HTTP {response.status_code}"
                )
            except httpx.HTTPError as exc:
                last_error = exc
            await asyncio.sleep(0.1)
    raise AssertionError(
        f"Supplier Simulator did not become ready: {last_error}"
    )


async def _kill_supplier_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.kill()
    await asyncio.to_thread(process.wait, timeout=5)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_point", ["before_commit", "after_commit"])
async def test_supplier_api_process_restart_recovers_receipt_without_double_credit(
    postgres_schema_url: str,
    failure_point: str,
):
    product_sku = f"SKU-SUPPLIER-PROCESS-{failure_point.upper()}"
    with psycopg.connect(postgres_schema_url, row_factory=dict_row) as connection:
        cursor = connection.execute(
            """
            INSERT INTO products (sku, name, category, unit_price)
            VALUES (%s, 'Supplier process product', 'integration', %s)
            RETURNING product_id
            """,
            (product_sku, Decimal("10.00")),
        )
        product_id = cursor.fetchone()["product_id"]
        connection.execute(
            """
            INSERT INTO inventory_balances (product_id, on_hand, reserved)
            VALUES (%s, 0, 0)
            """,
            (product_id,),
        )

    inventory_state = {
        "product_id": product_id,
        "product": {
            "product_id": product_id,
            "sku": product_sku,
            "name": "Supplier process product",
            "category": "integration",
            "unit_price": "10.00",
            "is_active": True,
        },
        "balance": {"on_hand": 5, "reserved": 0},
        "receipts": {},
        "receipt_calls": 0,
        "fail_next_receipt": None,
    }
    inventory_server, inventory_thread, inventory_url = (
        _start_inventory_http_server(inventory_state)
    )
    process: subprocess.Popen | None = None
    try:
        process, supplier_url = _start_supplier_api_process(
            postgres_schema_url, inventory_url
        )
        await _wait_for_supplier_process(process, supplier_url)
        async with httpx.AsyncClient(base_url=supplier_url, timeout=10) as client:
            policy_response = await client.put(
                "/reorder-policies",
                json={
                    "product_id": product_id,
                    "reorder_point": 5,
                    "target_stock": 12,
                    "lead_time_seconds": 60,
                },
            )
            assert policy_response.status_code == 200

            evaluation = await client.post(
                "/replenishments/evaluate",
                params={"product_id": product_id},
            )
            assert evaluation.status_code == 200
            purchase_order_id = evaluation.json()["decisions"][0][
                "purchase_order"
            ]["purchase_order_id"]

            ordered = await client.post(
                f"/purchase-orders/{purchase_order_id}/order"
            )
            assert ordered.status_code == 200
            advanced = await client.post(
                "/clock/advance", json={"seconds": 60}
            )
            assert advanced.status_code == 200

            inventory_state["fail_next_receipt"] = failure_point
            failed_attempt = await client.post(
                f"/purchase-orders/{purchase_order_id}/receive"
            )
            assert failed_attempt.status_code == 502
            pending = await client.get(
                f"/purchase-orders/{purchase_order_id}"
            )
            assert pending.status_code == 200
            assert pending.json()["status"] == "receiving"
            assert pending.json()["receipt_id"] is None
            assert inventory_state["balance"]["on_hand"] == (
                5 if failure_point == "before_commit" else 12
            )

        await _kill_supplier_process(process)
        process = None

        # A fresh OS process must recover the persisted clock, PO state,
        # and receipt identity solely from PostgreSQL.
        process, restarted_url = _start_supplier_api_process(
            postgres_schema_url, inventory_url
        )
        await _wait_for_supplier_process(process, restarted_url)
        async with httpx.AsyncClient(
            base_url=restarted_url, timeout=10
        ) as restarted_client:
            clock_response = await restarted_client.get("/clock")
            assert clock_response.status_code == 200
            assert clock_response.json()["now"] == advanced.json()["now"]

            retried = await restarted_client.post(
                f"/purchase-orders/{purchase_order_id}/receive"
            )
            assert retried.status_code == 200
            assert retried.json()["status"] == "received"
            assert retried.json()["receipt_id"] == inventory_state["receipts"][
                f"supplier-po:{purchase_order_id}"
            ]["receipt_id"]

            history_response = await restarted_client.get(
                f"/purchase-orders/{purchase_order_id}/transitions"
            )
            assert history_response.status_code == 200
            assert [
                item["to_status"] for item in history_response.json()
            ] == ["created", "ordered", "receiving", "received"]

            # The next decision must use Inventory's post-receipt balance and
            # the persisted PO state, not the stale pre-receipt snapshot.
            post_receipt = await restarted_client.post(
                "/replenishments/evaluate",
                params={"product_id": product_id},
            )
            assert post_receipt.status_code == 200
            decision = post_receipt.json()["decisions"][0]
            assert decision["decision"] == "no_reorder"
            assert decision["inventory_position"] == 12
            assert decision["purchase_order_ids"] == []

        assert inventory_state["balance"] == {"on_hand": 12, "reserved": 0}
        assert inventory_state["receipt_calls"] == 2
    finally:
        if process is not None:
            await _kill_supplier_process(process)
        inventory_server.shutdown()
        inventory_server.server_close()
        inventory_thread.join(timeout=5)



@pytest.mark.asyncio
async def test_replenishment_retries_inventory_read_if_receipt_finalizes_mid_read(
    postgres_schema_url: str,
):
    manager, store = await _open_store(
        postgres_schema_url, "supplier-stale-inventory-read"
    )
    inventory = InventoryHTTPDouble()
    product_id = await _create_product(manager, "SKU-SUPPLIER-STALE-READ")
    inventory.add_product(product_id, "SKU-SUPPLIER-STALE-READ", on_hand=5)
    try:
        await store.set_policy(
            ReorderPolicy(
                product_id=product_id,
                reorder_point=5,
                target_stock=12,
                lead_time_seconds=60,
            )
        )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(inventory.handler),
            base_url="http://inventory.test",
        ) as client:
            decision = (
                await store.evaluate_replenishment(
                    client, "stale-read-create", product_id=product_id
                )
            )[0]
            purchase_order_id = decision["purchase_order"]["purchase_order_id"]
            await store.order_purchase_order(purchase_order_id, "stale-read-order")
            await store.advance_clock(60)
            received = await store.receive_purchase_order(
                purchase_order_id, client, "stale-read-receipt"
            )
            assert received["status"] == "received"

        # Force a stale revision marker, then simulate Inventory returning its
        # old balance just as the persisted receipt revision becomes visible.
        async with manager.connection() as connection:
            await connection.execute(
                """
                UPDATE supplier_reorder_policies
                SET inventory_revision = 0
                WHERE product_id = %s
                """,
                (product_id,),
            )

        read_count = 0

        def stale_inventory_handler(request: httpx.Request) -> httpx.Response:
            nonlocal read_count
            if request.method == "GET" and request.url.path == (
                f"/inventory/{product_id}"
            ):
                read_count += 1
                if read_count == 1:
                    with psycopg.connect(postgres_schema_url) as connection:
                        connection.execute(
                            """
                            UPDATE supplier_reorder_policies
                            SET inventory_revision = 1
                            WHERE product_id = %s
                            """,
                            (product_id,),
                        )
                    return httpx.Response(
                        200,
                        json={
                            "product_id": product_id,
                            "on_hand": 5,
                            "reserved": 0,
                            "available_quantity": 5,
                        },
                    )
                return httpx.Response(
                    200,
                    json={
                        "product_id": product_id,
                        "on_hand": 12,
                        "reserved": 0,
                        "available_quantity": 12,
                    },
                )
            return httpx.Response(404, json={"detail": "not found"})

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(stale_inventory_handler),
            base_url="http://inventory.test",
        ) as client:
            after_receipt = await store.evaluate_replenishment(
                client, "stale-read-after-receipt", product_id=product_id
            )

        assert read_count == 2
        assert after_receipt[0]["decision"] == "no_reorder"
        assert after_receipt[0]["inventory_position"] == 12
        purchase_orders = await store.list_purchase_orders()
        assert len(purchase_orders) == 1
        assert purchase_orders[0]["status"] == "received"
    finally:
        await manager.close()
