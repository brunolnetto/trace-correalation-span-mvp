from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime
from time import monotonic
from typing import Any

import httpx

ORDER_EVENT_NAMES = (
    "order_generator.attempt",
    "order.process.started",
    "order.process.completed",
    "inventory.reservation.created",
    "inventory.reservation.committed",
)

SUPPLIER_EVENT_COUNTS = {
    "supplier.reorder_policy.updated": 1,
    "supplier.replenishment.evaluated": 3,
    "supplier.purchase_order.ordered": 1,
    "supplier.purchase_order.received": 1,
    "inventory.stock.received": 1,
}


def expected_event_counts(
    order_correlation_ids: list[str],
    supplier_correlation_id: str,
) -> Counter[tuple[str, str]]:
    """Build the barrier contract from the current producer log event names."""
    expected: Counter[tuple[str, str]] = Counter()
    for correlation_id in order_correlation_ids:
        for event_name in ORDER_EVENT_NAMES:
            expected[(correlation_id, event_name)] += 1
    for event_name, count in SUPPLIER_EVENT_COUNTS.items():
        expected[(supplier_correlation_id, event_name)] += count
    return expected


def assess_events(
    records: list[dict[str, Any]],
    expected: Counter[tuple[str, str]],
) -> dict[str, Any]:
    """Count only unique persisted events that match this run's contract."""
    observed: Counter[tuple[str, str]] = Counter()
    seen_event_ids: set[str] = set()

    for record in records:
        event = record.get("event", record)
        if not isinstance(event, dict):
            continue
        event_id = str(record.get("event_id") or event.get("event_id") or "")
        if event_id and event_id in seen_event_ids:
            continue
        if event_id:
            seen_event_ids.add(event_id)

        correlation_id = event.get("correlation_id")
        event_name = event.get("event_name")
        if not correlation_id or not event_name:
            continue
        key = (str(correlation_id), str(event_name))
        if key in expected:
            observed[key] += 1

    missing = [
        {
            "correlation_id": correlation_id,
            "event_name": event_name,
            "expected": count,
            "observed": observed[(correlation_id, event_name)],
            "missing": count - observed[(correlation_id, event_name)],
        }
        for (correlation_id, event_name), count in sorted(expected.items())
        if observed[(correlation_id, event_name)] < count
    ]
    expected_count = sum(expected.values())
    observed_count = sum(
        min(count, expected[key]) for key, count in observed.items()
    )
    return {
        "complete": not missing,
        "expected_count": expected_count,
        "observed_count": observed_count,
        "missing": missing,
    }


async def wait_for_collector_events(
    client: httpx.AsyncClient,
    expected: Counter[tuple[str, str]],
    *,
    timeout_seconds: float,
    initial_backoff_seconds: float,
    max_backoff_seconds: float,
    correlation_id: str,
) -> dict[str, Any]:
    """Poll the Collector read API until all expected durable events are visible."""
    started = monotonic()
    backoff = initial_backoff_seconds
    attempts = 0
    last_error: str | None = None
    assessment = assess_events([], expected)

    while True:
        remaining = timeout_seconds - (monotonic() - started)
        if remaining <= 0:
            break

        attempts += 1
        try:
            async with asyncio.timeout(remaining):
                response = await client.get(
                    "/v1/events?limit=10000",
                    headers={"X-Correlation-ID": correlation_id},
                )
                response.raise_for_status()
                payload = response.json()
            records = payload.get("events")
            if not isinstance(records, list):
                raise ValueError("Collector response has no events list")
            assessment = assess_events(records, expected)
            last_error = None
            if assessment["complete"]:
                elapsed = monotonic() - started
                return {
                    "status": "complete",
                    **assessment,
                    "poll_attempts": attempts,
                    "elapsed_seconds": round(elapsed, 3),
                    "completed_at": datetime.now(UTC).isoformat(),
                    "last_error": None,
                }
        except (httpx.HTTPError, ValueError, TimeoutError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"

        remaining = timeout_seconds - (monotonic() - started)
        if remaining <= 0:
            break
        await asyncio.sleep(min(backoff, remaining))
        backoff = min(backoff * 2, max_backoff_seconds)

    elapsed = monotonic() - started
    return {
        "status": "timeout",
        **assessment,
        "poll_attempts": attempts,
        "elapsed_seconds": round(elapsed, 3),
        "completed_at": None,
        "last_error": last_error,
    }
