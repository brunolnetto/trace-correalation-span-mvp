from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from typing import Any
from uuid import uuid4

import httpx


def build_event_envelope(
    source: dict[str, Any], *, event_id: str | None = None
) -> dict[str, Any]:
    """Wrap the original structured log without discarding source attributes."""
    return {
        "schema_version": "1.0",
        "event_id": event_id or str(uuid4()),
        "occurred_at": source["timestamp"],
        "service": source["service"],
        "logger": source["logger"],
        "level": source["level"],
        "event_name": source["message"],
        "correlation_id": source.get("correlation_id"),
        "trace_id": source.get("trace_id"),
        "span_id": source.get("span_id"),
        "attributes": source.get("attributes", {}),
        "source": source,
    }


class AsyncCollectorHandler(logging.Handler):
    """Bounded, non-blocking logging handler with batched HTTP delivery.

    Events accepted by the collector are durable and replay-safe. This
    in-process queue is best-effort before collector acknowledgement: queue
    overflow or an application crash can lose events, and the dropped counter
    makes local delivery pressure visible.
    """

    def __init__(
        self,
        *,
        service_name: str,
        collector_url: str,
        formatter: logging.Formatter,
        max_queue_size: int = 10_000,
        batch_size: int = 100,
    ) -> None:
        super().__init__()
        self.service_name = service_name
        self.collector_url = collector_url.rstrip("/")
        self._formatter = formatter
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(
            maxsize=max_queue_size
        )
        self._batch_size = batch_size
        self._stopping = threading.Event()
        self.dropped_events = 0
        self.failed_batches = 0
        self._worker = threading.Thread(
            target=self._run,
            name=f"{service_name}-event-publisher",
            daemon=True,
        )
        self._worker.start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            source = json.loads(self._formatter.format(record))
            event = build_event_envelope(source)
            self._queue.put_nowait(event)
        except queue.Full:
            self.dropped_events += 1
        except (json.JSONDecodeError, KeyError, TypeError):
            self.handleError(record)

    def _take_batch(self) -> list[dict[str, Any]]:
        try:
            first = self._queue.get(timeout=0.25)
        except queue.Empty:
            return []
        batch = [first]
        while len(batch) < self._batch_size:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return batch

    def _deliver(self, batch: list[dict[str, Any]]) -> bool:
        for attempt in range(3):
            try:
                with httpx.Client(timeout=2.0) as client:
                    response = client.post(
                        f"{self.collector_url}/v1/events",
                        json={"events": batch},
                    )
                    response.raise_for_status()
                return True
            except httpx.HTTPError:
                if attempt < 2:
                    time.sleep(0.1 * (2**attempt))
        self.failed_batches += 1
        return False

    def _run(self) -> None:
        while not self._stopping.is_set() or not self._queue.empty():
            batch = self._take_batch()
            if not batch:
                continue
            self._deliver(batch)
            for _ in batch:
                self._queue.task_done()

    def close(self) -> None:
        self._stopping.set()
        self._worker.join(timeout=2.0)
        super().close()


def collector_handler_from_environment(
    service_name: str, formatter: logging.Formatter
) -> AsyncCollectorHandler | None:
    collector_url = os.getenv("EVENT_COLLECTOR_URL")
    if not collector_url:
        return None
    return AsyncCollectorHandler(
        service_name=service_name,
        collector_url=collector_url,
        formatter=formatter,
    )
