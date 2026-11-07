from __future__ import annotations

import hashlib
import json
import os
import threading
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class RawEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"]
    event_id: UUID
    occurred_at: datetime
    service: str = Field(min_length=1, max_length=128)
    logger: str = Field(min_length=1, max_length=256)
    level: str = Field(min_length=1, max_length=32)
    event_name: str = Field(min_length=1, max_length=256)
    correlation_id: str | None = Field(default=None, max_length=128)
    trace_id: str | None = Field(default=None, max_length=64)
    span_id: str | None = Field(default=None, max_length=32)
    attributes: dict[str, Any] = Field(default_factory=dict)
    source: dict[str, Any]


class EventIdentityConflict(ValueError):
    """An event ID was reused with a different immutable payload."""


def _canonical_hash(event: dict[str, Any]) -> str:
    serialized = json.dumps(
        event,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


class JsonlEventStore:
    """Append-only JSONL event store with restart-safe event-ID deduplication.

    Accepted writes are fsynced before the API acknowledges them. This MVP
    uses one process-local index and a single writer lock; do not mount the
    same file into multiple collector replicas.
    """

    def __init__(self, events_path: str | Path, quarantine_path: str | Path):
        self.events_path = Path(events_path)
        self.quarantine_path = Path(quarantine_path)
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        self.quarantine_path.parent.mkdir(parents=True, exist_ok=True)
        self._hashes: dict[str, str] = {}
        self._lock = threading.Lock()
        self._recover_index()

    def _recover_index(self) -> None:
        if not self.events_path.exists():
            return
        invalid: list[tuple[Any, str]] = []
        needs_separator = False
        with self.events_path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                needs_separator = not line.endswith("\n")
                try:
                    stored = json.loads(line)
                    event = RawEvent.model_validate(stored["event"])
                    event_payload = event.model_dump(mode="json")
                    event_id = str(event.event_id)
                    fingerprint = _canonical_hash(event_payload)
                    existing = self._hashes.get(event_id)
                    if existing is not None and existing != fingerprint:
                        raise EventIdentityConflict(
                            f"event_id {event_id} has conflicting persisted payloads"
                        )
                    self._hashes[event_id] = fingerprint
                except (json.JSONDecodeError, KeyError, TypeError, ValidationError, EventIdentityConflict) as exc:  # malformed historical lines are preserved
                    invalid.append(
                        (
                            {
                                "line_number": line_number,
                                "raw_line": line.rstrip("\n"),
                            },
                            f"recovery_error: {type(exc).__name__}: {exc}",
                        )
                    )
        if needs_separator:
            # Preserve an incomplete final record; append only a delimiter so
            # the next valid event cannot be concatenated onto the bad bytes.
            with self.events_path.open("a", encoding="utf-8") as stream:
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
        for payload, reason in invalid:
            self.quarantine(payload, reason)

    def append_batch(
        self, events: list[RawEvent]
    ) -> dict[str, Any]:
        accepted: list[dict[str, Any]] = []
        duplicates: list[str] = []
        conflicts: list[RawEvent] = []

        with self._lock:
            pending_hashes: dict[str, str] = {}
            for event in events:
                event_payload = event.model_dump(mode="json")
                event_id = str(event.event_id)
                fingerprint = _canonical_hash(event_payload)
                existing = self._hashes.get(
                    event_id, pending_hashes.get(event_id)
                )
                if existing is not None:
                    if existing == fingerprint:
                        duplicates.append(event_id)
                        continue
                    conflicts.append(event)
                    continue

                record = {
                    "event_id": event_id,
                    "ingested_at": datetime.now(UTC).isoformat(),
                    "event": event_payload,
                }
                accepted.append(record)
                pending_hashes[event_id] = fingerprint

            if accepted:
                with self.events_path.open("a", encoding="utf-8") as stream:
                    for record in accepted:
                        stream.write(
                            json.dumps(
                                record,
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                            + "\n"
                        )
                    stream.flush()
                    os.fsync(stream.fileno())
                self._hashes.update(pending_hashes)

        return {
            "accepted": len(accepted),
            "duplicates": len(duplicates),
            "conflicts": len(conflicts),
            "accepted_event_ids": [item["event_id"] for item in accepted],
            "duplicate_event_ids": duplicates,
            "conflicting_event_ids": [
                str(item.event_id) for item in conflicts
            ],
            "conflicting_events": conflicts,
        }

    def quarantine(self, payload: Any, reason: str) -> None:
        record = {
            "quarantined_at": datetime.now(UTC).isoformat(),
            "reason": reason,
            "payload": payload,
        }
        with self._lock, self.quarantine_path.open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
                + "\\n"
            )
            stream.flush()
            os.fsync(stream.fileno())

    def list_events(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not self.events_path.exists():
            return []
        with self._lock, self.events_path.open("r", encoding="utf-8") as stream:
            return list(
                deque(
                    (json.loads(line) for line in stream if line.strip()),
                    maxlen=limit,
                )
            )[::-1]

    def list_quarantine(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not self.quarantine_path.exists():
            return []
        with self._lock, self.quarantine_path.open("r", encoding="utf-8") as stream:
            return list(
                deque(
                    (json.loads(line) for line in stream if line.strip()),
                    maxlen=limit,
                )
            )[::-1]

    def get_event(self, event_id: UUID) -> dict[str, Any] | None:
        for record in self.list_events(limit=10_000):
            if record["event_id"] == str(event_id):
                return record
        return None

    def list_quarantine(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not self.quarantine_path.exists():
            return []
        with self._lock:
            with self.quarantine_path.open("r", encoding="utf-8") as stream:
                return list(
                    deque(
                        (json.loads(line) for line in stream if line.strip()),
                        maxlen=limit,
                    )
                )[::-1]

    def stats(self) -> dict[str, int]:
        return {
            "known_event_ids": len(self._hashes),
            "quarantine_records": len(self.list_quarantine(limit=10_000)),
        }
