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
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid5

import httpx

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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _correlation_id(seed: int, index: int) -> str:
    return str(uuid5(NAMESPACE_URL, f"order-generator:{seed}:{index}"))


def build_plan(config: GeneratorConfig) -> list[OrderCommand]:
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
                correlation_id=_correlation_id(config.seed, index),
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
            correlation_id=_correlation_id(config.seed, index),
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


def emit_events(
    config: GeneratorConfig,
    plan: list[OrderCommand],
    attempts: list[OrderAttempt],
) -> dict[str, Any]:
    for attempt in attempts:
        print(
            json.dumps(
                {"event": "order_generator.attempt", **attempt.to_dict()},
                sort_keys=True,
            )
        )
    summary = summarize(config, plan, attempts)
    print(json.dumps(summary, sort_keys=True))
    return summary


async def run(config: GeneratorConfig) -> dict[str, Any]:
    plan = build_plan(config)
    async with httpx.AsyncClient(
        base_url=config.order_api_url.rstrip("/"),
        timeout=httpx.Timeout(10.0, connect=3.0),
    ) as client:
        attempts = await execute_plan(client, plan)
    return emit_events(config, plan, attempts)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate deterministic synthetic orders through the Order API."
    )
    parser.add_argument(
        "--order-api-url",
        default=os.getenv("ORDER_API_URL", "http://localhost:18000"),
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
        )
        summary = asyncio.run(run(config))
    except (ValueError, httpx.InvalidURL) as exc:
        print(f"order-generator configuration error: {exc}", file=sys.stderr)
        return 2
    return int(summary["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
