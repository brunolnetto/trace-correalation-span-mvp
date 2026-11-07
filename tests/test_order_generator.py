from __future__ import annotations

import json

import httpx
import pytest

from app.order_generator.generator import (
    GeneratorConfig,
    OrderAttempt,
    build_generator_events,
    build_plan,
    classify_response,
    execute_plan,
    publish_generator_events,
    summarize,
)


def test_random_plan_is_reproducible_from_seed():
    config = GeneratorConfig(
        mode="random",
        count=12,
        seed=731,
        min_quantity=1,
        max_quantity=5,
    )

    first = build_plan(config)
    repeated = build_plan(config)
    another_seed = build_plan(
        GeneratorConfig(
            mode="random",
            count=12,
            seed=732,
            min_quantity=1,
            max_quantity=5,
        )
    )

    assert first == repeated
    assert [command.quantity for command in first] != [
        command.quantity for command in another_seed
    ]
    assert len({command.correlation_id for command in first}) == 12
    assert [command.order_id for command in first] == list(range(10_000, 10_012))


def test_coverage_plan_has_named_expected_outcomes():
    plan = build_plan(
        GeneratorConfig(mode="coverage", seed=42, product_id=7)
    )

    assert [(command.scenario, command.quantity) for command in plan] == [
        ("success", 2),
        ("payment_declined", 11),
        ("insufficient_stock", 999_999),
    ]
    assert [command.expected_outcome for command in plan] == [
        "processed",
        "payment_declined",
        "inventory_rejected",
    ]
    assert len({command.correlation_id for command in plan}) == 3


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (201, "processed"),
        (402, "payment_declined"),
        (409, "inventory_rejected"),
        (404, "product_not_found"),
        (503, "server_error"),
        (418, "unexpected_http_status"),
    ],
)
def test_response_classification(status_code: int, expected: str):
    assert classify_response(status_code) == expected


def test_summary_reports_outcomes_percentiles_and_coverage_mismatches():
    config = GeneratorConfig(mode="coverage", seed=42)
    plan = build_plan(config)
    attempts = [
        OrderAttempt(
            order_id=command.order_id,
            product_id=command.product_id,
            quantity=command.quantity,
            correlation_id=command.correlation_id,
            scenario=command.scenario,
            expected_outcome=command.expected_outcome,
            outcome=outcome,
            http_status=status,
            latency_ms=latency,
        )
        for command, outcome, status, latency in zip(
            plan,
            ("processed", "payment_declined", "inventory_rejected"),
            (201, 402, 409),
            (2.0, 4.0, 6.0),
            strict=True,
        )
    ]

    summary = summarize(config, plan, attempts)

    assert summary["attempted"] == 3
    assert summary["processed"] == 1
    assert summary["payment_declined"] == 1
    assert summary["inventory_rejected"] == 1
    assert summary["unexpected_failures"] == 0
    assert summary["scenario_mismatches"] == []
    assert summary["latency_ms"] == {
        "mean": 4.0,
        "p50": 4.0,
        "p95": 5.8,
        "max": 6.0,
    }
    assert summary["exit_code"] == 0


@pytest.mark.asyncio
async def test_execute_plan_uses_order_api_and_propagates_correlation_id():
    plan = build_plan(
        GeneratorConfig(
            mode="random",
            count=1,
            seed=11,
            product_id=7,
            first_order_id=123,
            min_quantity=2,
            max_quantity=2,
        )
    )
    command = plan[0]
    received: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(
            201,
            json={
                "order_id": command.order_id,
                "status": "processed",
                "correlation_id": command.correlation_id,
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://order.test",
    ) as client:
        attempts = await execute_plan(client, plan)

    assert len(received) == 1
    assert received[0].method == "POST"
    assert received[0].url.path == "/orders/123/process"
    assert received[0].headers["X-Correlation-ID"] == command.correlation_id
    assert received[0].read() == (
        b'{"product_id":7,"quantity":2}'
    )
    assert attempts[0].outcome == "processed"
    assert attempts[0].http_status == 201
    assert attempts[0].attempted_at
    assert attempts[0].response["order_id"] == 123


@pytest.mark.asyncio
async def test_execute_plan_records_transport_errors_without_aborting_run():
    plan = build_plan(
        GeneratorConfig(
            mode="random",
            count=2,
            seed=7,
            min_quantity=1,
            max_quantity=1,
        )
    )
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("simulated connection failure", request=request)
        return httpx.Response(201, json={"status": "processed"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://order.test",
    ) as client:
        attempts = await execute_plan(client, plan)

    assert [attempt.outcome for attempt in attempts] == [
        "transport_error",
        "processed",
    ]
    assert attempts[0].error.startswith("ConnectError:")


def test_generator_plan_id_and_run_correlations_are_stable_and_unique():
    config = GeneratorConfig(
        mode="coverage",
        seed=42,
        product_id=7,
        first_order_id=200,
    )
    equivalent_config = GeneratorConfig(
        mode="coverage",
        seed=42,
        product_id=7,
        first_order_id=200,
    )
    plan = build_plan(config)
    attempts = [
        OrderAttempt(
            order_id=command.order_id,
            product_id=command.product_id,
            quantity=command.quantity,
            correlation_id=command.correlation_id,
            scenario=command.scenario,
            expected_outcome=command.expected_outcome,
            outcome=command.expected_outcome or "processed",
            http_status=201,
            latency_ms=1.0,
        )
        for command in plan
    ]
    summary = summarize(config, plan, attempts)
    execution_id = "coverage-run-001"
    events = build_generator_events(
        config, attempts, summary, run_id=execution_id
    )
    second_execution = build_plan(config, execution_id="coverage-run-002")

    # A plan identifies the deterministic workload; a run identifies one execution.
    assert config.plan_id == equivalent_config.plan_id
    assert config.plan_id
    assert len(events) == 4
    assert {event["event_name"] for event in events} == {
        "order_generator.attempt",
        "order_generator.summary",
    }
    attempt_events = [
        event for event in events
        if event["event_name"] == "order_generator.attempt"
    ]
    assert {
        event["correlation_id"] for event in attempt_events
    } == {command.correlation_id for command in plan}
    assert {
        event["attributes"]["run_id"] for event in attempt_events
    } == {execution_id}
    assert {
        event["attributes"]["plan_id"] for event in attempt_events
    } == {config.plan_id}
    summary_event = next(
        event for event in events
        if event["event_name"] == "order_generator.summary"
    )
    assert summary_event["attributes"]["run_id"] == execution_id
    assert summary_event["attributes"]["plan_id"] == config.plan_id
    assert [command.quantity for command in second_execution] == [
        command.quantity for command in plan
    ]
    assert {
        command.correlation_id for command in second_execution
    }.isdisjoint({command.correlation_id for command in plan})


@pytest.mark.asyncio
async def test_generator_event_publisher_posts_batches_to_collector():
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        payload = request.read()
        batch_size = len(json.loads(payload)["events"])
        return httpx.Response(
            202,
            json={
                "accepted": batch_size,
                "duplicates": 0,
                "conflicts": 0,
                "quarantined": 0,
            },
        )

    events = [
        {"event_id": f"event-{index}"}
        for index in range(501)
    ]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="http://collector.test",
    ) as client:
        result = await publish_generator_events(client, events)

    assert result == {"accepted": 501, "duplicates": 0}
    assert len(requests) == 2
    assert len(json.loads(requests[0].read())["events"]) == 500
    assert len(json.loads(requests[1].read())["events"]) == 1
