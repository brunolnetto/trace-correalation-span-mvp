from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import sql

from app.persistence.migrations import MIGRATIONS_DIR, apply_migrations


@pytest.fixture
def isolated_postgres_schema():
    database_url = os.getenv("POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("POSTGRES_TEST_URL is required for PostgreSQL supplier tests")

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


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _start_api_process(
    module: str,
    database_url: str,
    *,
    inventory_api_url: str | None = None,
) -> tuple[subprocess.Popen, str]:
    port = _free_port()
    environment = os.environ.copy()
    environment.update(
        {
            "ENVIRONMENT": "development",
            "PERSISTENCE_BACKEND": "postgres",
            "DATABASE_URL": database_url,
            "DATABASE_POOL_MIN_SIZE": "1",
            "DATABASE_POOL_MAX_SIZE": "4",
            "INVENTORY_SEED_DEMO_DATA": "false",
            "SERVICE_NAME": f"{module.rsplit('.', 1)[-1]}-supplier-test",
            "LOG_LEVEL": "WARNING",
        }
    )
    environment.pop("EVENT_COLLECTOR_URL", None)
    environment.pop("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", None)
    environment.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)
    if inventory_api_url is not None:
        environment["INVENTORY_API_URL"] = inventory_api_url

    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            f"{module}:app",
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


async def _wait_for_api(
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
                    f"API process exited during startup: {process.returncode}"
                )
            try:
                response = await client.get(f"{base_url}/healthz")
                if response.status_code == 200:
                    return
                last_error = AssertionError(
                    f"API readiness returned HTTP {response.status_code}"
                )
            except httpx.HTTPError as exc:
                last_error = exc
            await asyncio.sleep(0.1)
    raise AssertionError(f"API did not become ready at {base_url}: {last_error}")


async def _stop_api_process(process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        await asyncio.to_thread(process.wait, timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        await asyncio.to_thread(process.wait, timeout=5)


def _start_drop_first_receipt_proxy(
    upstream_url: str,
) -> tuple[ThreadingHTTPServer, threading.Thread, dict[str, bool | str]]:
    state: dict[str, bool | str] = {"dropped": False}

    class ProxyHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._forward()

        def do_POST(self) -> None:
            self._forward()

        def _forward(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else None
            headers = {
                key: value
                for key, value in self.headers.items()
                if key.lower() not in {"host", "connection", "content-length"}
            }
            upstream_request = Request(
                f"{upstream_url}{self.path}",
                data=body,
                headers=headers,
                method=self.command,
            )
            try:
                upstream_response = urlopen(upstream_request, timeout=10)
            except HTTPError as exc:
                upstream_response = exc

            status_code = upstream_response.status
            response_headers = dict(upstream_response.headers.items())
            payload = upstream_response.read()
            upstream_response.close()

            if (
                self.command == "POST"
                and self.path.startswith("/inventory/")
                and self.path.endswith("/receipts")
                and not state["dropped"]
            ):
                # Inventory has committed the receipt, but the Supplier API
                # never receives the response. This models the ambiguous
                # remote-side-effect window that must survive a process crash.
                state["dropped"] = True
                self.close_connection = True
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self.connection.close()
                return

            self.send_response(status_code)
            content_type = response_headers.get("Content-Type")
            if content_type:
                self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), ProxyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    proxy_url = f"http://127.0.0.1:{server.server_port}"
    state["url"] = proxy_url
    return server, thread, state


@pytest.mark.asyncio
async def test_supplier_state_survives_process_restarts_and_receipt_response_loss(
    isolated_postgres_schema: str,
):
    inventory_process: subprocess.Popen | None = None
    supplier_process: subprocess.Popen | None = None
    second_supplier_process: subprocess.Popen | None = None
    proxy_server: ThreadingHTTPServer | None = None
    proxy_thread: threading.Thread | None = None

    try:
        inventory_process, inventory_url = _start_api_process(
            "app.inventory.main", isolated_postgres_schema
        )
        await _wait_for_api(inventory_process, inventory_url)

        supplier_process, supplier_url = _start_api_process(
            "app.supplier_simulator.main",
            isolated_postgres_schema,
            inventory_api_url=inventory_url,
        )
        second_supplier_process, second_supplier_url = _start_api_process(
            "app.supplier_simulator.main",
            isolated_postgres_schema,
            inventory_api_url=inventory_url,
        )
        await asyncio.gather(
            _wait_for_api(supplier_process, supplier_url),
            _wait_for_api(second_supplier_process, second_supplier_url),
        )

        async with (
            httpx.AsyncClient(base_url=inventory_url, timeout=5) as inventory,
            httpx.AsyncClient(base_url=supplier_url, timeout=10) as supplier_a,
            httpx.AsyncClient(base_url=second_supplier_url, timeout=10) as supplier_b,
        ):
            # Simulate a crash after Inventory created the catalog product but
            # before Supplier persisted its onboarding reference. Onboarding
            # must recover the canonical product by SKU after a 409.
            preexisting_product = await inventory.post(
                "/products",
                json={
                    "sku": "COFFEE-BEANS-250G",
                    "name": "Whole bean coffee 250g",
                    "category": "grocery",
                    "unit_price": "24.90",
                },
            )
            assert preexisting_product.status_code == 201
            preexisting_product_id = preexisting_product.json()["product_id"]
            onboarded = await supplier_a.post("/supplier-catalog/BEANS-250/onboard")
            assert onboarded.status_code == 200
            product_id = onboarded.json()["product"]["product_id"]
            assert product_id == preexisting_product_id

            policy = await supplier_a.put(
                "/reorder-policies",
                json={
                    "product_id": product_id,
                    "reorder_point": 0,
                    "target_stock": 10,
                    "lead_time_seconds": 60,
                },
            )
            assert policy.status_code == 200

            # Two independent Supplier Simulator processes race to replenish.
            evaluations = await asyncio.gather(
                supplier_a.post("/replenishments/evaluate"),
                supplier_b.post("/replenishments/evaluate"),
            )
            assert [response.status_code for response in evaluations] == [200, 200]
            decisions = [
                decision
                for response in evaluations
                for decision in response.json()["decisions"]
            ]
            assert sorted(decision["decision"] for decision in decisions) == [
                "open_purchase_order_exists",
                "purchase_order_created",
            ]
            purchase_order_ids = {
                decision["purchase_order"]["purchase_order_id"]
                for decision in decisions
                if decision.get("purchase_order")
            }
            assert len(purchase_order_ids) == 1
            first_po_id = next(iter(purchase_order_ids))

            # Bypass the application lock and prove the partial unique index
            # itself rejects a second active PO for the same product.
            with psycopg.connect(isolated_postgres_schema) as connection:
                with pytest.raises(psycopg.errors.UniqueViolation), connection.transaction():
                    connection.execute(
                        """
                        INSERT INTO supplier_purchase_orders (
                            purchase_order_id, product_id, supplier_sku,
                            policy_version, reorder_point, target_stock,
                            inventory_position_at_order, on_hand_at_order,
                            reserved_at_order, on_order_at_order, quantity,
                            lead_time_seconds, status, created_at
                        )
                        VALUES (
                            %s, %s, 'BEANS-250', 1, 0, 10,
                            0, 0, 0, 0, 1, 0, 'created', now()
                        )
                        """,
                        (uuid4(), product_id),
                    )

                with pytest.raises(
                    psycopg.errors.RaiseException, match="append-only"
                ), connection.transaction():
                    connection.execute(
                        """
                        UPDATE supplier_purchase_order_transitions
                        SET details = '{}'::jsonb
                        WHERE purchase_order_id = %s
                        """,
                        (first_po_id,),
                    )

                with pytest.raises(
                    psycopg.errors.RaiseException, match="append-only"
                ), connection.transaction():
                    connection.execute(
                        "TRUNCATE supplier_purchase_order_transitions"
                    )

            ordered = await supplier_a.post(
                f"/purchase-orders/{first_po_id}/order"
            )
            assert ordered.status_code == 200
            assert ordered.json()["status"] == "ordered"
            clock_advance = await supplier_a.post(
                "/clock/advance", json={"seconds": 60}
            )
            assert clock_advance.status_code == 200

            # Crash/restart before Inventory is called. The PO and its due time
            # must remain visible to a fresh Supplier Simulator process.
            await _stop_api_process(supplier_process)
            supplier_process, supplier_url = _start_api_process(
                "app.supplier_simulator.main",
                isolated_postgres_schema,
                inventory_api_url=inventory_url,
            )
            await _wait_for_api(supplier_process, supplier_url)
            async with httpx.AsyncClient(
                base_url=supplier_url, timeout=10
            ) as restarted_supplier:
                persisted = await restarted_supplier.get(
                    f"/purchase-orders/{first_po_id}"
                )
                assert persisted.status_code == 200
                assert persisted.json()["status"] == "ordered"
                assert persisted.json()["lines"][0]["quantity"] == 10
                assert persisted.json()["lines"][0]["sku"] == "COFFEE-BEANS-250G"

                onboarding_after_restart = await restarted_supplier.post(
                    "/supplier-catalog/BEANS-250/onboard"
                )
                assert onboarding_after_restart.status_code == 200
                assert (
                    onboarding_after_restart.json()["product"]["product_id"]
                    == product_id
                )
                persisted_policies = await restarted_supplier.get("/reorder-policies")
                assert persisted_policies.status_code == 200
                persisted_policy = persisted_policies.json()[0]
                assert persisted_policy["version"] == 2
                assert persisted_policy["target_stock"] == 10

                persisted_clock = await restarted_supplier.get("/clock")
                assert persisted_clock.status_code == 200
                first_due_at = persisted.json()["due_at"]
                assert first_due_at <= persisted_clock.json()["now"]

                first_receipt = await restarted_supplier.post(
                    f"/purchase-orders/{first_po_id}/receive"
                )
                assert first_receipt.status_code == 200
                assert first_receipt.json()["status"] == "received"

                inventory_after_first = await inventory.get(
                    f"/inventory/{product_id}"
                )
                assert inventory_after_first.status_code == 200
                assert inventory_after_first.json()["on_hand"] == 10

                # The completed PO is no longer active. A second threshold
                # creates another PO; both supplier processes share its lock.
                policy = await restarted_supplier.put(
                    "/reorder-policies",
                    json={
                        "product_id": product_id,
                        "reorder_point": 10,
                        "target_stock": 20,
                        "lead_time_seconds": 0,
                    },
                )
                assert policy.status_code == 200

                supplier_b_process = second_supplier_process
                assert supplier_b_process is not None
                await _wait_for_api(supplier_b_process, second_supplier_url)
                async with httpx.AsyncClient(
                    base_url=second_supplier_url, timeout=10
                ) as concurrent_supplier:
                    repeated = await asyncio.gather(
                        restarted_supplier.post("/replenishments/evaluate"),
                        concurrent_supplier.post("/replenishments/evaluate"),
                    )
                    assert [response.status_code for response in repeated] == [200, 200]
                    repeated_decisions = [
                        decision
                        for response in repeated
                        for decision in response.json()["decisions"]
                    ]
                    assert sorted(
                        decision["decision"] for decision in repeated_decisions
                    ) == [
                        "open_purchase_order_exists",
                        "purchase_order_created",
                    ]
                    second_po_ids = {
                        decision["purchase_order"]["purchase_order_id"]
                        for decision in repeated_decisions
                        if decision.get("purchase_order")
                    }
                    assert len(second_po_ids) == 1
                    second_po_id = next(iter(second_po_ids))

                    second_ordered = await restarted_supplier.post(
                        f"/purchase-orders/{second_po_id}/order"
                    )
                    assert second_ordered.status_code == 200

                    # Restart behind a proxy that lets Inventory commit but
                    # drops the response before Supplier can persist 'received'.
                    proxy_server, proxy_thread, proxy_state = (
                        _start_drop_first_receipt_proxy(inventory_url)
                    )
                    proxy_url = str(proxy_state["url"])
                    await _stop_api_process(supplier_process)
                    supplier_process, supplier_url = _start_api_process(
                        "app.supplier_simulator.main",
                        isolated_postgres_schema,
                        inventory_api_url=proxy_url,
                    )
                    await _wait_for_api(supplier_process, supplier_url)
                    async with httpx.AsyncClient(
                        base_url=supplier_url, timeout=10
                    ) as proxy_supplier:
                        failed_response = await proxy_supplier.post(
                            f"/purchase-orders/{second_po_id}/receive"
                        )
                        assert failed_response.status_code == 503
                        assert proxy_state["dropped"] is True

                        receiving = await proxy_supplier.get(
                            f"/purchase-orders/{second_po_id}"
                        )
                        assert receiving.status_code == 200
                        assert receiving.json()["status"] == "receiving"

                        ambiguous_decision = await proxy_supplier.post(
                            "/replenishments/evaluate"
                        )
                        assert ambiguous_decision.status_code == 200
                        ambiguous_product_decision = (
                            ambiguous_decision.json()["decisions"][0]
                        )
                        assert (
                            ambiguous_product_decision["decision"]
                            == "open_purchase_order_exists"
                        )
                        assert ambiguous_product_decision["purchase_order_ids"] == [
                            second_po_id
                        ]

                    inventory_after_lost_response = await inventory.get(
                        f"/inventory/{product_id}"
                    )
                    assert inventory_after_lost_response.status_code == 200
                    assert inventory_after_lost_response.json()["on_hand"] == 20

                    # The Inventory transaction has committed its durable
                    # idempotency response even though Supplier never received it.
                    with psycopg.connect(isolated_postgres_schema) as connection:
                        receipt_row = connection.execute(
                            """
                            SELECT response->>'receipt_id'
                            FROM idempotency_records
                            WHERE scope = %s AND idempotency_key = %s
                            """,
                            (
                                f"receipt:{product_id}",
                                f"supplier-po:{second_po_id}",
                            ),
                        ).fetchone()
                    assert receipt_row is not None and receipt_row[0]
                    committed_receipt_id = receipt_row[0]

                    # Kill the process after the remote commit, restart against
                    # Inventory directly, and retry the stable PO idempotency key.
                    await _stop_api_process(supplier_process)
                    supplier_process, supplier_url = _start_api_process(
                        "app.supplier_simulator.main",
                        isolated_postgres_schema,
                        inventory_api_url=inventory_url,
                    )
                    await _wait_for_api(supplier_process, supplier_url)
                    async with httpx.AsyncClient(
                        base_url=supplier_url, timeout=10
                    ) as recovered_supplier:
                        recovered_state = await recovered_supplier.get(
                            f"/purchase-orders/{second_po_id}"
                        )
                        assert recovered_state.status_code == 200
                        assert recovered_state.json()["status"] == "receiving"
                        retried_receipt = await recovered_supplier.post(
                            f"/purchase-orders/{second_po_id}/receive"
                        )
                        assert retried_receipt.status_code == 200
                        assert retried_receipt.json()["status"] == "received"
                        assert (
                            retried_receipt.json()["receipt_id"]
                            == committed_receipt_id
                        )

                        transitions = await recovered_supplier.get(
                            f"/purchase-orders/{second_po_id}/transitions"
                        )
                        assert transitions.status_code == 200
                        assert [
                            item["to_status"] for item in transitions.json()
                        ] == [
                            "created",
                            "ordered",
                            "receiving",
                            "received",
                        ]

                        post_receipt_decision = await recovered_supplier.post(
                            "/replenishments/evaluate"
                        )
                        assert post_receipt_decision.status_code == 200
                        final_decision = post_receipt_decision.json()["decisions"][0]
                        assert final_decision["decision"] == "no_reorder"
                        assert final_decision["on_hand"] == 20
                        assert final_decision["on_order"] == 0
                        assert final_decision["inventory_position"] == 20

                    final_inventory = await inventory.get(
                        f"/inventory/{product_id}"
                    )
                    assert final_inventory.status_code == 200
                    assert final_inventory.json()["on_hand"] == 20
    finally:
        await _stop_api_process(supplier_process)
        await _stop_api_process(second_supplier_process)
        await _stop_api_process(inventory_process)
        if proxy_server is not None:
            proxy_server.shutdown()
            proxy_server.server_close()
        if proxy_thread is not None:
            proxy_thread.join(timeout=5)
