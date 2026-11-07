from __future__ import annotations

import os
from typing import Any
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, ValidationError

from app.event_collector.store import (
    JsonlEventStore,
    RawEvent,
)
from app.observability import configure_observability

app = FastAPI(
    title="Raw Event Collector",
    description="Versioned, append-only JSONL event ingestion with quarantine.",
    version="0.1.0",
)
configure_observability(app, os.getenv("SERVICE_NAME", "event-collector"))

store = JsonlEventStore(
    os.getenv("EVENT_STORE_PATH", "/data/events.jsonl"),
    os.getenv("EVENT_QUARANTINE_PATH", "/data/quarantine.jsonl"),
)


class EventBatchRequest(BaseModel):
    events: list[Any] = Field(min_length=1, max_length=500)


@app.get("/healthz", tags=["health"])
async def healthcheck():
    return {"status": "ok", "service": "event-collector"}


@app.post("/v1/events", status_code=202, tags=["events"])
def ingest_events(request: EventBatchRequest):
    valid: list[RawEvent] = []
    quarantined = 0
    quarantine_results: list[dict[str, Any]] = []

    for index, payload in enumerate(request.events):
        try:
            event = RawEvent.model_validate(payload)
        except ValidationError as exc:
            errors = exc.errors(include_url=False)
            store.quarantine(
                {"batch_index": index, "payload": payload},
                json_errors := str(errors),
            )
            quarantined += 1
            quarantine_results.append(
                {"batch_index": index, "reason": json_errors}
            )
            continue
        valid.append(event)

    result = store.append_batch(valid)
    for event in result.pop("conflicting_events"):
        reason = f"event_id {event.event_id} reused with a different payload"
        store.quarantine(event.model_dump(mode="json"), reason)
    result["quarantined"] = quarantined + result["conflicts"]
    result["quarantine_details"] = quarantine_results
    return result


@app.get("/v1/events", tags=["events"])
async def list_events(limit: int = Query(default=100, ge=1, le=10_000)):
    return {"events": store.list_events(limit=limit)}


@app.get("/v1/events/{event_id}", tags=["events"])
async def get_event(event_id: UUID):
    result = store.get_event(event_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Event not found.")
    return result


@app.get("/v1/quarantine", tags=["events"])
async def list_quarantine(limit: int = Query(default=100, ge=1, le=10_000)):
    return {"records": store.list_quarantine(limit=limit)}


@app.get("/v1/stats", tags=["events"])
async def get_stats():
    return store.stats()
