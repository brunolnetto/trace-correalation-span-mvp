from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import pytest

from app.event_collector.store import (
    JsonlEventStore,
    RawEvent,
)
from app.event_publisher import build_event_envelope


def make_event(
    *,
    event_id: str = "b273f0d8-9c8e-4f68-9b4e-bd8c32c70de1",
    event_name: str = "order.process.completed",
) -> RawEvent:
    source = {
        "timestamp": "2026-11-07T10:00:00+00:00",
        "level": "INFO",
        "service": "order-api",
        "logger": "app.order.main",
        "message": event_name,
        "correlation_id": "550e8400-e29b-41d4-a716-446655440001",
        "trace_id": "0123456789abcdef0123456789abcdef",
        "span_id": "0123456789abcdef",
        "attributes": {"order_id": 123, "saga_id": "saga-123"},
    }
    envelope = build_event_envelope(source, event_id=event_id)
    return RawEvent.model_validate(envelope)


def test_event_envelope_preserves_correlation_trace_and_original_log():
    source = {
        "timestamp": "2026-11-07T10:00:00+00:00",
        "level": "INFO",
        "service": "inventory-api",
        "logger": "app.inventory.main",
        "message": "inventory.reservation.created",
        "correlation_id": "corr-123",
        "trace_id": "a" * 32,
        "span_id": "b" * 16,
        "attributes": {"product_id": 7, "quantity": 2},
    }

    event = build_event_envelope(source, event_id="event-1")

    assert event["schema_version"] == "1.0"
    assert event["event_id"] == "event-1"
    assert event["event_name"] == "inventory.reservation.created"
    assert event["correlation_id"] == "corr-123"
    assert event["trace_id"] == "a" * 32
    assert event["span_id"] == "b" * 16
    assert event["attributes"] == {"product_id": 7, "quantity": 2}
    assert event["source"] == source


def test_event_append_is_idempotent_and_survives_store_restart(tmp_path: Path):
    events_path = tmp_path / "raw" / "events.jsonl"
    quarantine_path = tmp_path / "raw" / "quarantine.jsonl"
    store = JsonlEventStore(events_path, quarantine_path)
    event = make_event()

    first = store.append_batch([event])
    replay = store.append_batch([event])

    assert first["accepted"] == 1
    assert replay["accepted"] == 0
    assert replay["duplicates"] == 1
    assert len(events_path.read_text().splitlines()) == 1

    restarted = JsonlEventStore(events_path, quarantine_path)
    replay_after_restart = restarted.append_batch([event])
    assert replay_after_restart["duplicates"] == 1
    assert restarted.stats()["known_event_ids"] == 1


def test_reusing_event_id_with_different_payload_is_quarantinable(tmp_path: Path):
    store = JsonlEventStore(
        tmp_path / "events.jsonl",
        tmp_path / "quarantine.jsonl",
    )
    original = make_event()
    conflicting = make_event(event_name="order.process.failed")

    first = store.append_batch([original])
    second = store.append_batch([conflicting])

    assert first["accepted"] == 1
    assert second["accepted"] == 0
    assert second["conflicts"] == 1
    store.quarantine(
        conflicting.model_dump(mode="json"),
        "same event_id with a different payload",
    )
    assert len(store.list_quarantine()) == 1
    assert store.get_event(UUID(str(original.event_id)))["event"]["event_name"] == (
        "order.process.completed"
    )


def test_unsupported_schema_is_rejected_by_event_contract(tmp_path: Path):
    store = JsonlEventStore(
        tmp_path / "events.jsonl",
        tmp_path / "quarantine.jsonl",
    )
    payload = make_event().model_dump(mode="json")
    payload["schema_version"] = "2.0"

    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        RawEvent.model_validate(payload)
    store.quarantine(payload, "unsupported schema version")
    assert store.list_quarantine()[0]["payload"]["schema_version"] == "2.0"


def test_malformed_persisted_line_is_preserved_and_quarantined_on_recovery(
    tmp_path: Path,
):
    events_path = tmp_path / "events.jsonl"
    quarantine_path = tmp_path / "quarantine.jsonl"
    events_path.write_text("{not-json}\n", encoding="utf-8")

    store = JsonlEventStore(events_path, quarantine_path)

    assert events_path.read_text(encoding="utf-8") == "{not-json}\n"
    quarantined = store.list_quarantine()
    assert len(quarantined) == 1
    assert "recovery_error" in quarantined[0]["reason"]

\n\ndef test_batch_deduplicates_repeated_id_within_the_same_request(tmp_path: Path):
    store = JsonlEventStore(
        tmp_path / "events.jsonl",
        tmp_path / "quarantine.jsonl",
    )
    event = make_event()

    result = store.append_batch([event, event])

    assert result["accepted"] == 1
    assert result["duplicates"] == 1
    assert len(store.list_events()) == 1


def test_recovery_separates_incomplete_final_record_before_next_append(
    tmp_path: Path,
):
    events_path = tmp_path / "events.jsonl"
    quarantine_path = tmp_path / "quarantine.jsonl"
    events_path.write_text('{"incomplete":', encoding="utf-8")

    store = JsonlEventStore(events_path, quarantine_path)
    store.append_batch([make_event()])
    lines = events_path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 2
    assert json.loads(lines[1])["event"]["event_name"] == (
        "order.process.completed"
    )
    assert len(store.list_quarantine()) == 1
