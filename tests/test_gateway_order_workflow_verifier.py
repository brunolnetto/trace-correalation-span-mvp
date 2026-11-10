from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import httpx
import pytest

SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "verify-gateway-order-workflow.py"
)
SPEC = importlib.util.spec_from_file_location(
    "gateway_order_workflow_verifier",
    SCRIPT_PATH,
)
assert SPEC is not None and SPEC.loader is not None
VERIFIER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFIER)


def test_order_policy_builder_changes_only_the_order_route():
    original = [
        {
            "route_id": "order-api",
            "path_prefix": "/orders",
            "upstream_url": "http://order-api:8000",
            "latency_min_ms": 10,
            "latency_max_ms": 20,
            "failure_probability": 0.25,
            "failure_mode": "timeout",
            "failure_status_code": 503,
            "requests_per_second": 5.0,
            "rate_limit_burst": 3,
            "max_in_flight": 2,
            "timeout_seconds": 3.0,
        },
        {
            "route_id": "inventory",
            "path_prefix": "/inventory",
            "upstream_url": "http://inventory-api:8001",
        },
    ]
    original_snapshot = [route.copy() for route in original]

    baseline = VERIFIER.make_order_route_policy(
        original,
        failure_probability=0.0,
    )
    degraded = VERIFIER.make_order_route_policy(
        baseline,
        failure_probability=1.0,
    )

    assert original == original_snapshot
    assert baseline[1] == original[1]
    assert degraded[1] == original[1]
    assert baseline[0]["failure_probability"] == 0.0
    assert baseline[0]["latency_min_ms"] == 0
    assert baseline[0]["latency_max_ms"] == 0
    assert baseline[0]["requests_per_second"] is None
    assert baseline[0]["max_in_flight"] is None
    assert degraded[0]["failure_probability"] == 1.0
    assert degraded[0]["failure_mode"] == "http"
    assert degraded[0]["failure_status_code"] == 503


def test_completed_saga_contract_preserves_payment_and_step_identities():
    saga_id = "saga-verifier-test"
    payment_id = str(uuid5(NAMESPACE_URL, f"payment:{saga_id}"))
    result = {
        "saga_id": saga_id,
        "payment_id": payment_id,
        "reservation_id": "reservation-verifier-test",
    }
    recovery = {
        "saga_id": saga_id,
        "status": "completed",
        "payment_id": payment_id,
        "reservation_id": "reservation-verifier-test",
        "steps": [
            {
                "step_name": step_name,
                "forward_status": "completed",
                "forward_attempts": 1,
            }
            for step_name in VERIFIER.ORDER_STEPS
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/orders/123/recovery"
        return httpx.Response(200, json=recovery)

    with httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="http://order.test",
    ) as client:
        observed = VERIFIER.validate_completed_saga(
            client,
            order_id=123,
            result=result,
            description="test workflow",
        )

    assert observed["saga_id"] == saga_id
    assert observed["payment_id"] == payment_id
    assert tuple(step["step_name"] for step in observed["steps"]) == (
        "reserve_inventory",
        "charge_payment",
        "confirm_order",
    )


def test_completed_saga_contract_rejects_repeated_forward_actions():
    saga_id = "saga-verifier-test"
    payment_id = str(uuid5(NAMESPACE_URL, f"payment:{saga_id}"))
    result = {
        "saga_id": saga_id,
        "payment_id": payment_id,
        "reservation_id": "reservation-verifier-test",
    }
    recovery = {
        "saga_id": saga_id,
        "status": "completed",
        "payment_id": payment_id,
        "reservation_id": "reservation-verifier-test",
        "steps": [
            {
                "step_name": step_name,
                "forward_status": "completed",
                "forward_attempts": 2 if step_name == "charge_payment" else 1,
            }
            for step_name in VERIFIER.ORDER_STEPS
        ],
    }

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=recovery)

    with (
        httpx.Client(
            transport=httpx.MockTransport(handler),
            base_url="http://order.test",
        ) as client,
        pytest.raises(RuntimeError, match="ran more than once"),
    ):
        VERIFIER.validate_completed_saga(
            client,
            order_id=123,
            result=result,
            description="test workflow",
        )



def test_policy_restore_uses_compare_and_swap_and_restores_original_snapshot():
    original_routes = [
        {
            "route_id": "order-api",
            "path_prefix": "/orders",
            "upstream_url": "http://order-api:8000",
        }
    ]
    observed_puts: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"revision": 7})
        if request.method == "PUT":
            payload = request.read()
            observed_puts.append(json.loads(payload))
            return httpx.Response(
                200,
                json={
                    "revision": 8,
                    "seed": 20261109,
                    "routes": original_routes,
                },
            )
        return httpx.Response(405)

    with httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="http://gateway.test",
    ) as client:
        restored_revision = VERIFIER.restore_original_policy(
            client,
            owned_revision=7,
            original_seed=20261109,
            original_routes=original_routes,
        )

    assert restored_revision == 8
    assert observed_puts == [
        {
            "expected_revision": 7,
            "seed": 20261109,
            "routes": original_routes,
        }
    ]


def test_policy_restore_refuses_to_overwrite_external_revision():
    put_attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal put_attempts
        if request.method == "GET":
            return httpx.Response(200, json={"revision": 9})
        if request.method == "PUT":
            put_attempts += 1
            return httpx.Response(200, json={})
        return httpx.Response(405)

    with (
        httpx.Client(
            transport=httpx.MockTransport(handler),
            base_url="http://gateway.test",
        ) as client,
        pytest.raises(RuntimeError, match="changed outside this verifier"),
    ):
        VERIFIER.restore_original_policy(
            client,
            owned_revision=7,
            original_seed=20261109,
            original_routes=[],
        )

    assert put_attempts == 0
