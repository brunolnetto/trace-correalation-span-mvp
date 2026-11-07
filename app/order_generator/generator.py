from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx

from app.event_publisher import build_event_envelope

Mode = Literal["random", "coverage"]

COVERAGE_EXPECTATIONS = {
    "success": "processed",
    "payment_declined": "payment_declined",
    "insufficient_stock": "inventory_rejected",
}


@dataclass(frozen=True)
class GeneratorConfig:
    order_api_url: str = "http://localhost:18000"
    mode: Mode = "random"
    count: int = 10
    seed: int = 20261106
    product_id: int = 7
    first_order_id: int = 10_000
    min_quantity: int = 1
    max_quantity: int = 3
    success_quantity: int = 2
    decline_quantity: int = 11
    insufficient_quantity: int = 999_999
    event_collector_url: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in {"random", "coverage"}:
            raise ValueError("mode must be 'random' or 'coverage'")
        if self.count < 1:
            raise ValueError("count must be at least 1")
        if self.product_id < 1:
            raise ValueError("product_id must be positive")
        if self.first_order_id < 1:
            raise ValueError("first_order_id must be positive")
        if self.min_quantity < 1 or self.max_quantity < self.min_quantity:
            raise ValueError("quantity bounds must satisfy 1 <= min <= max")
        if min(
            self.success_quantity,
            self.decline_quantity,
            self.insufficient_quantity,
        ) < 1:
            raise ValueError("coverage quantities must be positive")
        if not self.order_api_url.startswith(("http://", "https://")):
            raise ValueError("order_api_url must be an HTTP(S) URL")
        if self.event_collector_url and not self.event_collector_url.startswith(
            ("http://", "https://")
        ):
            raise ValueError("event_collector_url must be an HTTP(S) URL")

    @property
    def plan_id(self) -> str:
        if self.mode == "coverage":
            workload = (
                f"coverage:{self.success_quantity}:"
                f"{self.decline_quantity}:{self.insufficient_quantity}"
            )
        else:
            workload = (
                f"random:{self.count}:{self.min_quantity}:{self.max_quantity}"
            )
        return str(
            uuid5(
                NAMESPACE_URL,
                (
                    f"order-generator-plan:{workload}:{self.seed}:"
                    f"{self.product_id}:{self.first_order_id}"
                ),
            )
        )


@dataclass(frozen=True)
class OrderCommand:
    order_id: int
    product_id: int
    quantity: int
    correlation_id: str
    scenario: str
    expected_outcome: str | None = None


@dataclass(frozen=True)
class OrderAttempt:
    order_id: int
    product_id: int
    quantity: int
    correlation_id: str
    scenario: str
    expected_outcome: str | None
    outcome: str
    http_status: int | None
    latency_ms: float
    response: dict[str, Any] | None = None
    error: str | None = None
    attempted_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _correlation_id(
    config: GeneratorConfig, index: int, execution_id: str | None = None
) -> str:
    namespace = execution_id or config.plan_id
    return str(
        uuid5(
            NAMESPACE_URL,
            (
                f"order-generator:{namespace}:{config.mode}:{config.seed}:"
                f"{config.product_id}:{config.first_order_id}:{index}"
            ),
        )
    )

def build_plan(
    config: GeneratorConfig, *, execution_id: str | None = None
) -> list[OrderCommand]:
    """Build a repeatable request plan from the seed and run configuration."""
    rng = random.Random(config.seed)

    if config.mode == "coverage":
        scenarios = [
            ("success", config.success_quantity),
            ("payment_declined", config.decline_quantity),
            ("insufficient_stock", config.insufficient_quantity),
        ]
        return [
            OrderCommand(
                order_id=config.first_order_id + index,
                product_id=config.product_id,
                quantity=quantity,
                correlation_id=_correlation_id(config, index, execution_id),
                scenario=scenario,
                expected_outcome=COVERAGE_EXPECTATIONS[scenario],
            )
            for index, (scenario, quantity) in enumerate(scenarios)
        ]

    return [
        OrderCommand(
            order_id=config.first_order_id + index,
            product_id=config.product_id,
            quantity=rng.randint(config.min_quantity, config.max_quantity),
            correlation_id=_correlation_id(config, index, execution_id),
            scenario="random",
        )
        for index in range(config.count)
    ]


def classify_response(status_code: int) -> str:
    if status_code == 201:
        return "processed"
    if status_code == 402:
        return "payment_declined"
    if status_code == 409:
        return "inventory_rejected"
    if status_code == 404:
        return "product_not_found"
    if status_code >= 500:
        return "server_error"
    return "unexpected_http_status"


def _parse_response(response: httpx.Response) -> dict[str, Any] | None:
    try:
        payload = response.json()
    except ValueError:
        return None
    return payload if isinstance(payload, dict) else {"data": payload}


async def execute_plan(
    client: httpx.AsyncClient,
    plan: list[OrderCommand],
) -> list[OrderAttempt]:
    attempts: list[OrderAttempt] = []

    for command in plan:
        started_at = time.perf_counter()
        attempted_at = datetime.now(UTC).isoformat()
        try:
            response = await client.post(
                f"/orders/{command.order_id}/process",
                json={
                    "product_id": command.product_id,
                    "quantity": command.quantity,
                },
                headers={"X-Correlation-ID": command.correlation_id},
            )
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            payload = _parse_response(response)
            outcome = classify_response(response.status_code)
            attempts.append(
                OrderAttempt(
                    order_id=command.order_id,
                    product_id=command.product_id,
                    quantity=command.quantity,
                    correlation_id=command.correlation_id,
                    scenario=command.scenario,
                    expected_outcome=command.expected_outcome,
                    outcome=outcome,
                    http_status=response.status_code,
                    latency_ms=round(elapsed_ms, 3),
                    response=payload,
                    attempted_at=attempted_at,
                    error=(
                        "Response body was not a JSON object"
                        if payload is None
                        else None
                    ),
                )
            )
        except httpx.HTTPError as exc:
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            attempts.append(
                OrderAttempt(
                    order_id=command.order_id,
                    product_id=command.product_id,
                    quantity=command.quantity,
                    correlation_id=command.correlation_id,
                    scenario=command.scenario,
                    expected_outcome=command.expected_outcome,
                    outcome="transport_error",
                    http_status=None,
                    latency_ms=round(elapsed_ms, 3),
                    error=f"{type(exc).__name__}: {exc}",
                    attempted_at=attempted_at,
                )
            )

    return attempts


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return round(
        ordered[lower] + (ordered[upper] - ordered[lower]) * fraction,
        3,
    )


def summarize(
    config: GeneratorConfig,
    plan: list[OrderCommand],
    attempts: list[OrderAttempt],
) -> dict[str, Any]:
    latencies = [attempt.latency_ms for attempt in attempts]
    outcome_counts = {
        name: sum(attempt.outcome == name for attempt in attempts)
        for name in (
            "processed",
            "payment_declined",
            "inventory_rejected",
            "product_not_found",
            "server_error",
            "unexpected_http_status",
            "transport_error",
        )
    }
    mismatches = [
        {
            "order_id": attempt.order_id,
            "scenario": attempt.scenario,
            "expected": attempt.expected_outcome,
            "actual": attempt.outcome,
        }
        for attempt in attempts
        if attempt.expected_outcome is not None
        and attempt.expected_outcome != attempt.outcome
    ]
    unexpected_failures = sum(
        attempt.outcome
        in {
            "product_not_found",
            "server_error",
            "unexpected_http_status",
            "transport_error",
        }
        or attempt.error is not None
        for attempt in attempts
    )
    return {
        "event": "order_generator.summary",
        "plan_id": config.plan_id,
        "mode": config.mode,
        "seed": config.seed,
        "product_id": config.product_id,
        "planned": len(plan),
        "attempted": len(attempts),
        **outcome_counts,
        "unexpected_failures": unexpected_failures,
        "scenario_mismatches": mismatches,
        "latency_ms": {
            "mean": round(statistics.fmean(latencies), 3) if latencies else None,
            "p50": _percentile(latencies, 0.50),
            "p95": _percentile(latencies, 0.95),
            "max": round(max(latencies), 3) if latencies else None,
        },
        "exit_code": int(unexpected_failures > 0 or bool(mismatches)),
    }


def build_generator_events(
    config: GeneratorConfig,
    attempts: list[OrderAttempt],
    summary: dict[str, Any],
    *,
    run_id: str,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for attempt in attempts:
        source = {
            "timestamp": attempt.attempted_at or datetime.now(UTC).isoformat(),
            "level": "INFO",
            "service": "order-generator",
            "logger": "app.order_generator",
            "message": "order_generator.attempt",
            "correlation_id": attempt.correlation_id,
            "trace_id": None,
            "span_id": None,
            "attributes": {
                **attempt.to_dict(),
                "run_id": run_id,
                "plan_id": config.plan_id,
                "mode": config.mode,
                "seed": config.seed,
            },
        }
        events.append(build_event_envelope(source))

    summary_source = {
        "timestamp": datetime.now(UTC).isoformat(),
        "level": "INFO",
        "service": "order-generator",
        "logger": "app.order_generator",
        "message": "order_generator.summary",
        "correlation_id": None,
        "trace_id": None,
        "span_id": None,
        "attributes": {
            **summary,
            "run_id": run_id,
            "plan_id": config.plan_id,
        },
    }
    events.append(build_event_envelope(summary_source))
    return events


async def publish_generator_events(
    client: httpx.AsyncClient,
    events: list[dict[str, Any]],
) -> dict[str, int]:
    accepted = 0
    duplicates = 0
    for start in range(0, len(events), 500):
        batch = events[start : start + 500]
        for attempt in range(3):
            try:
                response = await client.post("/v1/events", json={"events": batch})
                response.raise_for_status()
                result = response.json()
                if result.get("quarantined", 0) or result.get("conflicts", 0):
                    raise ValueError("Collector quarantined generated events")
                accepted += result.get("accepted", 0)
                duplicates += result.get("duplicates", 0)
                break
            except (httpx.HTTPError, ValueError) as exc:
                if attempt == 2:
                    raise RuntimeError(
                        f"Could not publish generator event batch: {exc}"
                    ) from exc
                await asyncio.sleep(0.1 * (2**attempt))
    return {"accepted": accepted, "duplicates": duplicates}


def emit_events(
    config: GeneratorConfig,
    plan: list[OrderCommand],
    attempts: list[OrderAttempt],
    summary: dict[str, Any] | None = None,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    effective_run_id = run_id or config.plan_id
    for attempt in attempts:
        print(
            json.dumps(
                {
                    "event": "order_generator.attempt",
                    "run_id": effective_run_id,
                    "plan_id": config.plan_id,
                    "mode": config.mode,
                    "seed": config.seed,
                    **attempt.to_dict(),
                },
                sort_keys=True,
            )
        )
    result = summary if summary is not None else summarize(config, plan, attempts)
    result.setdefault("run_id", effective_run_id)
    print(json.dumps(result, sort_keys=True))
    return result


async def run(config: GeneratorConfig) -> dict[str, Any]:
    run_id = str(uuid4())
    plan = build_plan(config, execution_id=run_id)
    async with httpx.AsyncClient(
        base_url=config.order_api_url.rstrip("/"),
        timeout=httpx.Timeout(10.0, connect=3.0),
    ) as client:
        attempts = await execute_plan(client, plan)

    summary = summarize(config, plan, attempts)
    summary["run_id"] = run_id
    if config.event_collector_url:
        events = build_generator_events(
            config, attempts, summary, run_id=run_id
        )
        try:
            async with httpx.AsyncClient(
                base_url=config.event_collector_url.rstrip("/"),
                timeout=httpx.Timeout(5.0, connect=2.0),
            ) as collector:
                delivery = await publish_generator_events(collector, events)
            summary["collector_delivery"] = {
                "status": "delivered",
                "event_count": len(events),
                **delivery,
            }
        except (httpx.HTTPError, RuntimeError) as exc:
            summary["collector_delivery"] = {
                "status": "failed",
                "event_count": len(events),
                "error": f"{type(exc).__name__}: {exc}",
            }
            summary["exit_code"] = 1
    return emit_events(
        config, plan, attempts, summary, run_id=run_id
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate deterministic synthetic orders through the Order API."
    )
    parser.add_argument(
        "--order-api-url",
        default=os.getenv("ORDER_API_URL", "http://localhost:18000"),
    )
    parser.add_argument(
        "--event-collector-url",
        default=os.getenv("EVENT_COLLECTOR_URL"),
    )
    parser.add_argument(
        "--mode",
        choices=("random", "coverage"),
        default=os.getenv("ORDER_GENERATOR_MODE", "random"),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=int(os.getenv("ORDER_GENERATOR_COUNT", "10")),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.getenv("ORDER_GENERATOR_SEED", "20261106")),
    )
    parser.add_argument(
        "--product-id",
        type=int,
        default=int(os.getenv("ORDER_GENERATOR_PRODUCT_ID", "7")),
    )
    parser.add_argument(
        "--first-order-id",
        type=int,
        default=int(os.getenv("ORDER_GENERATOR_FIRST_ORDER_ID", "10000")),
    )
    parser.add_argument(
        "--min-quantity",
        type=int,
        default=int(os.getenv("ORDER_GENERATOR_MIN_QUANTITY", "1")),
    )
    parser.add_argument(
        "--max-quantity",
        type=int,
        default=int(os.getenv("ORDER_GENERATOR_MAX_QUANTITY", "3")),
    )
    parser.add_argument(
        "--success-quantity",
        type=int,
        default=int(os.getenv("ORDER_GENERATOR_SUCCESS_QUANTITY", "2")),
    )
    parser.add_argument(
        "--decline-quantity",
        type=int,
        default=int(os.getenv("ORDER_GENERATOR_DECLINE_QUANTITY", "11")),
    )
    parser.add_argument(
        "--insufficient-quantity",
        type=int,
        default=int(os.getenv("ORDER_GENERATOR_INSUFFICIENT_QUANTITY", "999999")),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = GeneratorConfig(
            order_api_url=args.order_api_url,
            mode=args.mode,
            count=args.count,
            seed=args.seed,
            product_id=args.product_id,
            first_order_id=args.first_order_id,
            min_quantity=args.min_quantity,
            max_quantity=args.max_quantity,
            success_quantity=args.success_quantity,
            decline_quantity=args.decline_quantity,
            insufficient_quantity=args.insufficient_quantity,
            event_collector_url=args.event_collector_url,
        )
        summary = asyncio.run(run(config))
    except (ValueError, httpx.InvalidURL) as exc:
        print(f"order-generator configuration error: {exc}", file=sys.stderr)
        return 2
    return int(summary["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
