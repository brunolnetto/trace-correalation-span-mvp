from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx

from app.order_generator.generator import (
    GeneratorConfig,
    build_generator_events,
    build_plan,
    execute_plan,
    publish_generator_events,
    summarize,
)


class ScenarioError(RuntimeError):
    """Raised when the deterministic standalone scenario cannot be completed."""


@dataclass(frozen=True)
class RunnerConfig:
    order_api_url: str = "http://order-api:8000"
    inventory_api_url: str = "http://inventory-api:8001"
    supplier_api_url: str = "http://supplier-simulator:8002"
    event_collector_url: str = "http://event-collector:8010"
    product_id: int = 7
    opening_stock: int = 42
    reorder_point: int = 30
    target_stock: int = 60
    lead_time_seconds: int = 3600
    first_batch_count: int = 8
    second_batch_count: int = 4
    order_quantity: int = 2
    seed: int = 20261107
    request_timeout_seconds: float = 10.0
    report_path: str = "/run-state/simulation-report.json"
    run_id: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "order_api_url",
            "inventory_api_url",
            "supplier_api_url",
            "event_collector_url",
        ):
            value = getattr(self, name)
            if not value.startswith(("http://", "https://")):
                raise ValueError(f"{name} must be an HTTP(S) URL")
        if self.product_id < 1:
            raise ValueError("product_id must be positive")
        if min(
            self.opening_stock,
            self.order_quantity,
            self.first_batch_count,
            self.second_batch_count,
            self.lead_time_seconds,
        ) < 1:
            raise ValueError("scenario quantities and counts must be positive")
        if self.reorder_point < 0:
            raise ValueError("reorder_point must be non-negative")
        if self.target_stock <= self.reorder_point:
            raise ValueError("target_stock must exceed reorder_point")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        if not self.report_path:
            raise ValueError("report_path must not be empty")


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _run_order_id_base(run_id: str) -> int:
    """Derive stable, run-specific order IDs without relying on a process counter."""
    suffix = uuid5(NAMESPACE_URL, f"standalone-order-ids:{run_id}").int
    return 100_000_000 + suffix % 800_000_000


def _supplier_correlation_id(run_id: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"standalone-supplier:{run_id}"))


def _make_client(
    base_url: str,
    timeout_seconds: float,
    transport: httpx.AsyncBaseTransport | None,
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=base_url.rstrip("/"),
        timeout=httpx.Timeout(timeout_seconds, connect=min(3.0, timeout_seconds)),
        transport=transport,
    )


async def _request_json(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    *,
    context: str,
    correlation_id: str,
    payload: dict[str, Any] | None = None,
) -> Any:
    try:
        response = await client.request(
            method,
            path,
            json=payload,
            headers={"X-Correlation-ID": correlation_id},
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        try:
            detail = exc.response.json()
        except ValueError:
            detail = exc.response.text[:500]
        raise ScenarioError(
            f"{context}: HTTP {exc.response.status_code}: {detail}"
        ) from exc
    except httpx.HTTPError as exc:
        raise ScenarioError(
            f"{context}: {type(exc).__name__}: {exc}"
        ) from exc

    try:
        return response.json()
    except ValueError as exc:
        raise ScenarioError(f"{context}: response was not valid JSON") from exc


def _assert(
    report: dict[str, Any],
    name: str,
    expected: Any,
    actual: Any,
) -> None:
    passed = expected == actual
    report["assertions"].append(
        {
            "name": name,
            "passed": passed,
            "expected": expected,
            "actual": actual,
        }
    )
    if not passed:
        raise ScenarioError(
            f"Scenario assertion failed: {name}; expected {expected!r}, "
            f"received {actual!r}"
        )


def _write_report(report: dict[str, Any], report_path: str) -> None:
    target = Path(report_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(target)


async def _run_generator_batch(
    config: RunnerConfig,
    *,
    run_id: str,
    batch_name: str,
    count: int,
    first_order_id: int,
    transport: httpx.AsyncBaseTransport | None,
) -> dict[str, Any]:
    execution_id = f"{run_id}:{batch_name}"
    generator_config = GeneratorConfig(
        order_api_url=config.order_api_url,
        mode="random",
        count=count,
        seed=config.seed,
        product_id=config.product_id,
        first_order_id=first_order_id,
        min_quantity=config.order_quantity,
        max_quantity=config.order_quantity,
        event_collector_url=config.event_collector_url,
    )
    plan = build_plan(generator_config, execution_id=execution_id)

    async with _make_client(
        config.order_api_url,
        config.request_timeout_seconds,
        transport,
    ) as order_client:
        attempts = await execute_plan(order_client, plan)

    summary = summarize(generator_config, plan, attempts)
    summary["run_id"] = execution_id

    unexpected = [
        {
            "order_id": attempt.order_id,
            "correlation_id": attempt.correlation_id,
            "outcome": attempt.outcome,
            "http_status": attempt.http_status,
            "error": attempt.error,
        }
        for attempt in attempts
        if attempt.outcome != "processed"
        or attempt.http_status != 201
        or attempt.error is not None
    ]
    if len(attempts) != count or unexpected:
        raise ScenarioError(
            f"{batch_name}: expected {count} successful HTTP 201 orders; "
            f"attempted {len(attempts)}; unexpected outcomes: {unexpected}"
        )

    events = build_generator_events(
        generator_config,
        attempts,
        summary,
        run_id=execution_id,
    )
    async with _make_client(
        config.event_collector_url,
        config.request_timeout_seconds,
        transport,
    ) as collector_client:
        delivery = await publish_generator_events(collector_client, events)

    return {
        "batch": batch_name,
        "run_id": execution_id,
        "plan_id": generator_config.plan_id,
        "planned": len(plan),
        "attempted": len(attempts),
        "outcomes": {
            "processed": summary["processed"],
            "payment_declined": summary["payment_declined"],
            "inventory_rejected": summary["inventory_rejected"],
        },
        "orders": [
            {
                "order_id": attempt.order_id,
                "quantity": attempt.quantity,
                "correlation_id": attempt.correlation_id,
                "outcome": attempt.outcome,
                "http_status": attempt.http_status,
                "latency_ms": attempt.latency_ms,
            }
            for attempt in attempts
        ],
        "collector_delivery": {
            "status": "delivered",
            "event_count": len(events),
            **delivery,
        },
    }


async def run_scenario(
    config: RunnerConfig,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, Any]:
    run_id = config.run_id or str(uuid4())
    supplier_correlation_id = _supplier_correlation_id(run_id)
    first_order_id = _run_order_id_base(run_id)
    report: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "status": "running",
        "started_at": _now(),
        "finished_at": None,
        "report_path": config.report_path,
        "scenario": {
            "product_id": config.product_id,
            "opening_stock": config.opening_stock,
            "reorder_point": config.reorder_point,
            "target_stock": config.target_stock,
            "lead_time_seconds": config.lead_time_seconds,
            "order_quantity": config.order_quantity,
            "first_batch_count": config.first_batch_count,
            "second_batch_count": config.second_batch_count,
        },
        "correlation_ids": {
            "supplier": supplier_correlation_id,
            "orders": [],
        },
        "order_batches": [],
        "inventory_checkpoints": {},
        "purchase_order": None,
        "assertions": [],
        "error": None,
    }

    try:
        async with (
            _make_client(
                config.supplier_api_url,
                config.request_timeout_seconds,
                transport,
            ) as supplier,
            _make_client(
                config.inventory_api_url,
                config.request_timeout_seconds,
                transport,
            ) as inventory,
        ):
            product = await _request_json(
                inventory,
                "GET",
                f"/products/{config.product_id}",
                context="load demo product",
                correlation_id=supplier_correlation_id,
            )
            _assert(report, "demo product is active", True, product.get("is_active"))

            initial = await _request_json(
                inventory,
                "GET",
                f"/inventory/{config.product_id}",
                context="read opening inventory",
                correlation_id=supplier_correlation_id,
            )
            initial_checkpoint = {
                "on_hand": int(initial["on_hand"]),
                "reserved": int(initial["reserved"]),
                "available_quantity": int(initial["available_quantity"]),
            }
            report["inventory_checkpoints"]["initial"] = initial_checkpoint
            _assert(
                report,
                "opening inventory is the clean demo balance",
                {
                    "on_hand": config.opening_stock,
                    "reserved": 0,
                    "available_quantity": config.opening_stock,
                },
                initial_checkpoint,
            )

            existing_orders = await _request_json(
                supplier,
                "GET",
                "/purchase-orders",
                context="check for pre-existing purchase orders",
                correlation_id=supplier_correlation_id,
            )
            active_existing = [
                order
                for order in existing_orders
                if int(order["product_id"]) == config.product_id
                and order["status"] in {"created", "ordered"}
            ]
            _assert(
                report,
                "no active purchase order exists before the run",
                [],
                active_existing,
            )

            policy = await _request_json(
                supplier,
                "PUT",
                "/reorder-policies",
                context="configure supplier reorder policy",
                correlation_id=supplier_correlation_id,
                payload={
                    "product_id": config.product_id,
                    "reorder_point": config.reorder_point,
                    "target_stock": config.target_stock,
                    "lead_time_seconds": config.lead_time_seconds,
                },
            )
            _assert(
                report,
                "supplier policy product matches scenario",
                config.product_id,
                policy.get("product_id"),
            )

            first_batch = await _run_generator_batch(
                config,
                run_id=run_id,
                batch_name="orders-first-batch",
                count=config.first_batch_count,
                first_order_id=first_order_id,
                transport=transport,
            )
            report["order_batches"].append(first_batch)
            report["correlation_ids"]["orders"].extend(
                order["correlation_id"] for order in first_batch["orders"]
            )

            after_first = await _request_json(
                inventory,
                "GET",
                f"/inventory/{config.product_id}",
                context="read inventory after first order batch",
                correlation_id=supplier_correlation_id,
            )
            first_checkpoint = {
                "on_hand": int(after_first["on_hand"]),
                "reserved": int(after_first["reserved"]),
                "available_quantity": int(after_first["available_quantity"]),
            }
            report["inventory_checkpoints"]["after_first_batch"] = first_checkpoint
            expected_first = {
                "on_hand": config.opening_stock,
                "reserved": config.first_batch_count * config.order_quantity,
                "available_quantity": (
                    config.opening_stock
                    - config.first_batch_count * config.order_quantity
                ),
            }
            _assert(
                report,
                "first batch reserves exactly its requested quantity",
                expected_first,
                first_checkpoint,
            )

            first_decisions = await _request_json(
                supplier,
                "POST",
                f"/replenishments/evaluate?product_id={config.product_id}",
                context="evaluate first replenishment",
                correlation_id=supplier_correlation_id,
            )
            decisions = first_decisions.get("decisions", [])
            _assert(report, "first evaluation returns one decision", 1, len(decisions))
            first_decision = decisions[0]
            _assert(
                report,
                "first replenishment creates a purchase order",
                "purchase_order_created",
                first_decision.get("decision"),
            )
            purchase_order = first_decision.get("purchase_order") or {}
            _assert(
                report,
                "purchase order belongs to the demo product",
                config.product_id,
                purchase_order.get("product_id"),
            )
            _assert(
                report,
                "new purchase order starts in created state",
                "created",
                purchase_order.get("status"),
            )
            expected_po_quantity = (
                config.target_stock - first_checkpoint["available_quantity"]
            )
            _assert(
                report,
                "purchase order quantity fills inventory position to target",
                expected_po_quantity,
                purchase_order.get("quantity"),
            )
            purchase_order_id = str(purchase_order.get("purchase_order_id") or "")
            if not purchase_order_id:
                raise ScenarioError("Supplier API created a PO without an ID")
            report["purchase_order"] = {
                "purchase_order_id": purchase_order_id,
                "product_id": config.product_id,
                "quantity": purchase_order["quantity"],
                "created_status": purchase_order.get("status"),
            }

            ordered_po = await _request_json(
                supplier,
                "POST",
                f"/purchase-orders/{purchase_order_id}/order",
                context="place purchase order",
                correlation_id=supplier_correlation_id,
            )
            _assert(
                report,
                "purchase order enters ordered state",
                "ordered",
                ordered_po.get("status"),
            )

            second_batch = await _run_generator_batch(
                config,
                run_id=run_id,
                batch_name="orders-second-batch",
                count=config.second_batch_count,
                first_order_id=first_order_id + config.first_batch_count,
                transport=transport,
            )
            report["order_batches"].append(second_batch)
            report["correlation_ids"]["orders"].extend(
                order["correlation_id"] for order in second_batch["orders"]
            )

            after_second = await _request_json(
                inventory,
                "GET",
                f"/inventory/{config.product_id}",
                context="read inventory after second order batch",
                correlation_id=supplier_correlation_id,
            )
            second_checkpoint = {
                "on_hand": int(after_second["on_hand"]),
                "reserved": int(after_second["reserved"]),
                "available_quantity": int(after_second["available_quantity"]),
            }
            report["inventory_checkpoints"]["after_second_batch"] = second_checkpoint
            expected_second = {
                "on_hand": config.opening_stock,
                "reserved": (
                    config.first_batch_count + config.second_batch_count
                ) * config.order_quantity,
                "available_quantity": (
                    config.opening_stock
                    - (
                        config.first_batch_count + config.second_batch_count
                    ) * config.order_quantity
                ),
            }
            _assert(
                report,
                "second batch reserves exactly its requested quantity",
                expected_second,
                second_checkpoint,
            )

            in_flight = await _request_json(
                supplier,
                "POST",
                f"/replenishments/evaluate?product_id={config.product_id}",
                context="evaluate replenishment while PO is in flight",
                correlation_id=supplier_correlation_id,
            )
            in_flight_decisions = in_flight.get("decisions", [])
            _assert(
                report,
                "in-flight evaluation returns one decision",
                1,
                len(in_flight_decisions),
            )
            in_flight_decision = in_flight_decisions[0]
            _assert(
                report,
                "open purchase order suppresses duplicate replenishment",
                "open_purchase_order_exists",
                in_flight_decision.get("decision"),
            )
            _assert(
                report,
                "in-flight purchase order quantity is counted on order",
                expected_po_quantity,
                in_flight_decision.get("on_order"),
            )
            _assert(
                report,
                "in-flight inventory position includes outstanding PO",
                second_checkpoint["available_quantity"] + expected_po_quantity,
                in_flight_decision.get("inventory_position"),
            )
            _assert(
                report,
                "in-flight evaluation references the original PO",
                [purchase_order_id],
                in_flight_decision.get("purchase_order_ids"),
            )

            clock = await _request_json(
                supplier,
                "POST",
                "/clock/advance",
                context="advance supplier simulation clock",
                correlation_id=supplier_correlation_id,
                payload={"seconds": config.lead_time_seconds},
            )
            _assert(
                report,
                "supplier clock advances by configured lead time",
                config.lead_time_seconds,
                clock.get("advanced_seconds"),
            )

            received_po = await _request_json(
                supplier,
                "POST",
                f"/purchase-orders/{purchase_order_id}/receive",
                context="receive purchase order through Supplier and Inventory APIs",
                correlation_id=supplier_correlation_id,
            )
            _assert(
                report,
                "purchase order receipt completes",
                "received",
                received_po.get("status"),
            )
            _assert(
                report,
                "receipt quantity equals the PO quantity",
                expected_po_quantity,
                received_po.get("quantity"),
            )
            if not received_po.get("receipt_id"):
                raise ScenarioError("Received purchase order has no Inventory receipt ID")
            report["purchase_order"].update(
                {
                    "status": received_po["status"],
                    "receipt_id": received_po["receipt_id"],
                    "received_at": received_po.get("received_at"),
                }
            )

            final_inventory = await _request_json(
                inventory,
                "GET",
                f"/inventory/{config.product_id}",
                context="read final inventory after supplier receipt",
                correlation_id=supplier_correlation_id,
            )
            final_checkpoint = {
                "on_hand": int(final_inventory["on_hand"]),
                "reserved": int(final_inventory["reserved"]),
                "available_quantity": int(final_inventory["available_quantity"]),
            }
            report["inventory_checkpoints"]["after_receipt"] = final_checkpoint
            expected_final = {
                "on_hand": config.opening_stock + expected_po_quantity,
                "reserved": (
                    config.first_batch_count + config.second_batch_count
                ) * config.order_quantity,
                "available_quantity": (
                    config.opening_stock
                    + expected_po_quantity
                    - (
                        config.first_batch_count + config.second_batch_count
                    ) * config.order_quantity
                ),
            }
            _assert(
                report,
                "final inventory reflects exactly one supplier receipt",
                expected_final,
                final_checkpoint,
            )

            final_decisions = await _request_json(
                supplier,
                "POST",
                f"/replenishments/evaluate?product_id={config.product_id}",
                context="re-evaluate replenishment after receipt",
                correlation_id=supplier_correlation_id,
            )
            final_decision_list = final_decisions.get("decisions", [])
            _assert(
                report,
                "post-receipt evaluation returns one decision",
                1,
                len(final_decision_list),
            )
            final_decision = final_decision_list[0]
            _assert(
                report,
                "post-receipt inventory no longer requires replenishment",
                "no_reorder",
                final_decision.get("decision"),
            )
            _assert(
                report,
                "post-receipt inventory position is above reorder point",
                True,
                final_checkpoint["available_quantity"] > config.reorder_point,
            )
            _assert(
                report,
                "supplier decision matches final inventory position",
                final_checkpoint["available_quantity"],
                final_decision.get("inventory_position"),
            )
            final_orders = await _request_json(
                supplier,
                "GET",
                "/purchase-orders",
                context="verify final purchase-order count",
                correlation_id=supplier_correlation_id,
            )
            product_orders = [
                order
                for order in final_orders
                if int(order["product_id"]) == config.product_id
            ]
            _assert(
                report,
                "scenario creates exactly one purchase order",
                1,
                len(product_orders),
            )

        report["status"] = "succeeded"
        report["finished_at"] = _now()
        report["summary"] = {
            "orders_processed": config.first_batch_count + config.second_batch_count,
            "purchase_orders_created": 1,
            "purchase_order_quantity": expected_po_quantity,
            "final_inventory_position": final_checkpoint["available_quantity"],
            "assertions_passed": len(report["assertions"]),
        }
        return report
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        raise ScenarioError(str(exc)) from exc
    finally:
        report["finished_at"] = report.get("finished_at") or _now()
        _write_report(report, config.report_path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the deterministic order and supplier replenishment scenario."
    )
    parser.add_argument("--order-api-url", default=os.getenv("ORDER_API_URL", "http://order-api:8000"))
    parser.add_argument("--inventory-api-url", default=os.getenv("INVENTORY_API_URL", "http://inventory-api:8001"))
    parser.add_argument("--supplier-api-url", default=os.getenv("SUPPLIER_API_URL", "http://supplier-simulator:8002"))
    parser.add_argument("--event-collector-url", default=os.getenv("EVENT_COLLECTOR_URL", "http://event-collector:8010"))
    parser.add_argument("--product-id", type=int, default=int(os.getenv("SIMULATION_PRODUCT_ID", "7")))
    parser.add_argument("--opening-stock", type=int, default=int(os.getenv("SIMULATION_OPENING_STOCK", "42")))
    parser.add_argument("--reorder-point", type=int, default=int(os.getenv("SIMULATION_REORDER_POINT", "30")))
    parser.add_argument("--target-stock", type=int, default=int(os.getenv("SIMULATION_TARGET_STOCK", "60")))
    parser.add_argument("--lead-time-seconds", type=int, default=int(os.getenv("SIMULATION_LEAD_TIME_SECONDS", "3600")))
    parser.add_argument("--first-batch-count", type=int, default=int(os.getenv("SIMULATION_FIRST_BATCH_COUNT", "8")))
    parser.add_argument("--second-batch-count", type=int, default=int(os.getenv("SIMULATION_SECOND_BATCH_COUNT", "4")))
    parser.add_argument("--order-quantity", type=int, default=int(os.getenv("SIMULATION_ORDER_QUANTITY", "2")))
    parser.add_argument("--seed", type=int, default=int(os.getenv("SIMULATION_SEED", "20261107")))
    parser.add_argument("--request-timeout-seconds", type=float, default=float(os.getenv("SIMULATION_REQUEST_TIMEOUT_SECONDS", "10")))
    parser.add_argument("--report-path", default=os.getenv("SIMULATION_REPORT_PATH", "/run-state/simulation-report.json"))
    parser.add_argument("--run-id", default=os.getenv("STANDALONE_RUN_ID"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = RunnerConfig(
            order_api_url=args.order_api_url,
            inventory_api_url=args.inventory_api_url,
            supplier_api_url=args.supplier_api_url,
            event_collector_url=args.event_collector_url,
            product_id=args.product_id,
            opening_stock=args.opening_stock,
            reorder_point=args.reorder_point,
            target_stock=args.target_stock,
            lead_time_seconds=args.lead_time_seconds,
            first_batch_count=args.first_batch_count,
            second_batch_count=args.second_batch_count,
            order_quantity=args.order_quantity,
            seed=args.seed,
            request_timeout_seconds=args.request_timeout_seconds,
            report_path=args.report_path,
            run_id=args.run_id,
        )
        report = asyncio.run(run_scenario(config))
    except (ValueError, ScenarioError, OSError) as exc:
        print(f"simulation-runner failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
