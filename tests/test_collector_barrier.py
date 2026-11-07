from __future__ import annotations

from collections import Counter
from typing import Any

import httpx
import pytest

from app.collector_barrier import (
    assess_events,
    expected_event_counts,
    wait_for_collector_events,
)


def _record(event_id: str, correlation_id: str, event_name: str) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event": {
            "event_id": event_id,
            "correlation_id": correlation_id,
            "event_name": event_name,
        },
    }


def _complete_records(expected: Counter[tuple[str, str]]) -> list[dict[str, Any]]:
    records = []
    index = 0
    for (correlation_id, event_name), count in sorted(expected.items()):
        for _ in range(count):
            index += 1
            records.append(_record(f"event-{index}", correlation_id, event_name))
    return records


def test_assessment_requires_every_pair_and_ignores_duplicates_and_other_runs():
    expected = expected_event_counts(["order-current"], "supplier-current")
    records = _complete_records(expected)
    records.extend(
        [
            records[0],
            _record(
                "unrelated-event",
                "order-from-previous-run",
                "order.process.completed",
            ),
            _record("unrelated-name", "order-current", "inventory.lookup.completed"),
        ]
    )

    result = assess_events(records, expected)

    assert result["complete"] is True
    assert result["expected_count"] == 11
    assert result["observed_count"] == 11
    assert result["missing"] == []


@pytest.mark.asyncio
async def test_barrier_waits_for_delayed_collector_visibility():
    expected = expected_event_counts(["order-current"], "supplier-current")
    records = _complete_records(expected)
    polls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal polls
        polls += 1
        visible = records if polls >= 2 else []
        return httpx.Response(
            200,
            json={"events": visible},
        )

    async with httpx.AsyncClient(
        base_url="http://collector",
        transport=httpx.MockTransport(handler),
    ) as client:
        result = await wait_for_collector_events(
            client,
            expected,
            timeout_seconds=1,
            initial_backoff_seconds=0.001,
            max_backoff_seconds=0.002,
            correlation_id="supplier-current",
        )

    assert result["status"] == "complete"
    assert result["poll_attempts"] == 2
    assert result["observed_count"] == 11
    assert result["completed_at"] is not None


@pytest.mark.asyncio
async def test_barrier_timeout_reports_missing_correlation_and_event_names():
    expected = expected_event_counts(["order-current"], "supplier-current")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"events": []})

    async with httpx.AsyncClient(
        base_url="http://collector",
        transport=httpx.MockTransport(handler),
    ) as client:
        result = await wait_for_collector_events(
            client,
            expected,
            timeout_seconds=0.01,
            initial_backoff_seconds=0.002,
            max_backoff_seconds=0.004,
            correlation_id="supplier-current",
        )

    assert result["status"] == "timeout"
    assert result["observed_count"] == 0
    assert result["completed_at"] is None
    assert len(result["missing"]) == 11
    assert {
        (item["correlation_id"], item["event_name"])
        for item in result["missing"]
    } == set(expected)
