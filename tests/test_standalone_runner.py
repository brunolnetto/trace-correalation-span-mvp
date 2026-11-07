from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from app.standalone_runner import RunnerConfig, ScenarioError, run_scenario


def _scenario_transport(
    *,
    opening_stock: int = 42,
    reject_order_number: int | None = None,
) -> tuple[httpx.MockTransport, dict[str, Any]]:
    state: dict[str, Any] = {
        "on_hand": opening_stock,
        "reserved": 0,
        "policy": None,
        "purchase_order": None,
        "clock_seconds": 0,
        "lead_time_seconds": 0,
        "order_requests": [],
        "collector_events": [],
        "supplier_correlations": [],
    }

    def emit_event(correlation_id: str, event_name: str) -> None:
        event_id = f"service-event-{len(state['collector_events']) + 1}"
        state["collector_events"].append(
            {
                "event_id": event_id,
                "correlation_id": correlation_id,
                "event_name": event_name,
            }
        )

    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        path = request.url.path
        correlation_id = request.headers.get("X-Correlation-ID")
        if (
            host == "order-api"
            and request.method == "POST"
            and path.startswith("/orders/")
        ):
            order_id = int(path.split("/")[2])
            payload = json.loads(request.content)
            state["order_requests"].append(
                {
                    "order_id": order_id,
                    "quantity": payload["quantity"],
                    "correlation_id": correlation_id,
                }
            )
            if reject_order_number == len(state["order_requests"]):
                return httpx.Response(
                    409,
                    json={"detail": "simulated inventory rejection"},
                )
            state["reserved"] += payload["quantity"]
            for event_name in (
                "order.process.started",
                "order.process.completed",
                "inventory.reservation.created",
                "inventory.reservation.committed",
            ):
                emit_event(correlation_id or "", event_name)
            return httpx.Response(
                201,
                json={"order_id": order_id, "status": "processed"},
            )

        if (
            host == "event-collector"
            and request.method == "GET"
            and path == "/v1/events"
        ):
            return httpx.Response(
                200,
                json={
                    "events": [
                        {"event_id": event["event_id"], "event": event}
                        for event in state["collector_events"]
                    ]
                },
            )

        if (
            host == "event-collector"
            and request.method == "POST"
            and path == "/v1/events"
        ):
            payload = json.loads(request.content)
            events = payload["events"]
            state["collector_events"].extend(events)
            return httpx.Response(
                202,
                json={
                    "accepted": len(events),
                    "duplicates": 0,
                    "conflicts": 0,
                    "quarantined": 0,
                },
            )

        if host == "inventory-api":
            if request.method == "GET" and path == "/products/7":
                return httpx.Response(
                    200,
                    json={"product_id": 7, "is_active": True},
                )
            if request.method == "GET" and path == "/inventory/7":
                available = state["on_hand"] - state["reserved"]
                return httpx.Response(
                    200,
                    json={
                        "product_id": 7,
                        "on_hand": state["on_hand"],
                        "reserved": state["reserved"],
                        "available_quantity": available,
                    },
                )

        if host == "supplier-api":
            if correlation_id:
                state["supplier_correlations"].append(correlation_id)
            if request.method == "GET" and path == "/purchase-orders":
                orders = state["purchase_order"]
                return httpx.Response(
                    200,
                    json=[orders.copy()] if orders else [],
                )
            if request.method == "PUT" and path == "/reorder-policies":
                state["policy"] = json.loads(request.content)
                emit_event(correlation_id or "", "supplier.reorder_policy.updated")
                return httpx.Response(
                    200,
                    json={**state["policy"], "version": 1},
                )
            if request.method == "POST" and path == "/replenishments/evaluate":
                policy = state["policy"]
                available = state["on_hand"] - state["reserved"]
                order = state["purchase_order"]
                if order and order["status"] in {"created", "ordered"}:
                    decision = {
                        "product_id": 7,
                        "decision": "open_purchase_order_exists",
                        "on_order": order["quantity"],
                        "inventory_position": available + order["quantity"],
                        "purchase_order_ids": [order["purchase_order_id"]],
                    }
                elif available > policy["reorder_point"]:
                    decision = {
                        "product_id": 7,
                        "decision": "no_reorder",
                        "on_order": 0,
                        "inventory_position": available,
                        "purchase_order_ids": [],
                    }
                else:
                    quantity = policy["target_stock"] - available
                    state["purchase_order"] = {
                        "purchase_order_id": "po-test-001",
                        "product_id": 7,
                        "quantity": quantity,
                        "status": "created",
                    }
                    state["lead_time_seconds"] = policy["lead_time_seconds"]
                    decision = {
                        "product_id": 7,
                        "decision": "purchase_order_created",
                        "inventory_position": available,
                        "purchase_order": state["purchase_order"].copy(),
                    }
                emit_event(correlation_id or "", "supplier.replenishment.evaluated")
                return httpx.Response(200, json={"decisions": [decision]})
            if (
                request.method == "POST"
                and path == "/purchase-orders/po-test-001/order"
            ):
                state["purchase_order"]["status"] = "ordered"
                emit_event(correlation_id or "", "supplier.purchase_order.ordered")
                return httpx.Response(200, json=state["purchase_order"].copy())
            if request.method == "POST" and path == "/clock/advance":
                payload = json.loads(request.content)
                state["clock_seconds"] += payload["seconds"]
                return httpx.Response(
                    200,
                    json={"advanced_seconds": payload["seconds"], "now": "test"},
                )
            if (
                request.method == "POST"
                and path == "/purchase-orders/po-test-001/receive"
            ):
                order = state["purchase_order"]
                if state["clock_seconds"] < state["lead_time_seconds"]:
                    return httpx.Response(409, json={"detail": "lead time pending"})
                if order["status"] != "received":
                    state["on_hand"] += order["quantity"]
                    order["status"] = "received"
                    order["receipt_id"] = "receipt-test-001"
                    emit_event(correlation_id or "", "inventory.stock.received")
                    emit_event(correlation_id or "", "supplier.purchase_order.received")
                return httpx.Response(200, json=order.copy())

        return httpx.Response(404, json={"detail": f"unexpected request {request.method} {host}{path}"})

    return httpx.MockTransport(handler), state


@pytest.mark.asyncio
async def test_standalone_scenario_runs_both_batches_and_replenishes_once(
    tmp_path: Path,
):
    transport, state = _scenario_transport()
    config = RunnerConfig(
        order_api_url="http://order-api",
        inventory_api_url="http://inventory-api",
        supplier_api_url="http://supplier-api",
        event_collector_url="http://event-collector",
        report_path=str(tmp_path / "simulation-report.json"),
        run_id="standalone-test-run",
    )

    report = await run_scenario(config, transport=transport)

    assert report["status"] == "succeeded"
    assert report["summary"]["orders_processed"] == 12
    assert report["summary"]["purchase_order_quantity"] == 34
    assert report["summary"]["final_inventory_position"] == 52
    assert len(report["assertions"]) >= 15
    assert all(assertion["passed"] for assertion in report["assertions"])

    assert len(state["order_requests"]) == 12
    assert len({item["order_id"] for item in state["order_requests"]}) == 12
    assert len({item["correlation_id"] for item in state["order_requests"]}) == 12
    assert state["on_hand"] == 76
    assert state["reserved"] == 24
    assert state["purchase_order"]["status"] == "received"
    assert len(state["collector_events"]) == 69
    assert report["collector_barrier"]["status"] == "complete"
    assert report["collector_barrier"]["expected_count"] == 67
    assert report["collector_barrier"]["observed_count"] == 67
    assert report["summary"]["collector_events_observed"] == 67
    assert len(set(state["supplier_correlations"])) == 1

    saved = json.loads(Path(config.report_path).read_text(encoding="utf-8"))
    assert saved["run_id"] == "standalone-test-run"
    assert saved["status"] == "succeeded"
    assert saved["purchase_order"]["receipt_id"] == "receipt-test-001"


@pytest.mark.asyncio
async def test_standalone_scenario_fails_fast_and_writes_report_on_wrong_opening_stock(
    tmp_path: Path,
):
    transport, state = _scenario_transport(opening_stock=40)
    report_path = tmp_path / "failed-report.json"
    config = RunnerConfig(
        order_api_url="http://order-api",
        inventory_api_url="http://inventory-api",
        supplier_api_url="http://supplier-api",
        event_collector_url="http://event-collector",
        report_path=str(report_path),
        run_id="bad-opening-stock",
    )

    with pytest.raises(ScenarioError, match="opening inventory"):
        await run_scenario(config, transport=transport)

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert report["error"]["type"] == "ScenarioError"
    assert state["order_requests"] == []


@pytest.mark.asyncio
async def test_standalone_scenario_stops_when_an_order_is_not_successful(
    tmp_path: Path,
):
    transport, state = _scenario_transport(reject_order_number=1)
    report_path = tmp_path / "order-failure-report.json"
    config = RunnerConfig(
        order_api_url="http://order-api",
        inventory_api_url="http://inventory-api",
        supplier_api_url="http://supplier-api",
        event_collector_url="http://event-collector",
        report_path=str(report_path),
        run_id="order-failure-run",
    )

    with pytest.raises(ScenarioError, match="expected 8 successful"):
        await run_scenario(config, transport=transport)

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "failed"
    assert "unexpected outcomes" in report["error"]["message"]
    assert len(state["order_requests"]) == 8
    assert state["purchase_order"] is None
    assert len(state["collector_events"]) == 9
    assert (
        report["order_batches"][0]["unexpected_outcomes"][0]["outcome"]
        == "inventory_rejected"
    )
    assert len(report["correlation_ids"]["orders"]) == 8
