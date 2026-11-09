from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import httpx

from app.persistence.database import DatabaseManager

logger = logging.getLogger(__name__)


class OutboxProtocolError(RuntimeError):
    """The Event Collector accepted the HTTP request but rejected the event."""


class PostgresOutboxRepository:
    """Claim and update transactional outbox rows using PostgreSQL leases."""

    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    async def claim_batch(
        self,
        *,
        lease_owner: str,
        batch_size: int,
        lease_seconds: int,
    ) -> list[dict[str, Any]]:
        if not lease_owner:
            raise ValueError("lease_owner cannot be empty")
        if batch_size < 1 or lease_seconds < 1:
            raise ValueError("batch_size and lease_seconds must be positive")

        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                WITH candidates AS (
                    SELECT outbox_id
                    FROM outbox_events
                    WHERE (
                        status = 'pending'
                        AND available_at <= now()
                    ) OR (
                        status = 'leased'
                        AND lease_expires_at <= now()
                    )
                    ORDER BY available_at, created_at, outbox_id
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox_events AS event
                SET status = 'leased',
                    lease_owner = %s,
                    lease_expires_at = now() + (%s * interval '1 second'),
                    attempts = event.attempts + 1
                FROM candidates
                WHERE event.outbox_id = candidates.outbox_id
                RETURNING
                    event.outbox_id,
                    event.event_id,
                    event.event_name,
                    event.event_payload,
                    event.attempts,
                    event.correlation_id
                """,
                (batch_size, lease_owner, lease_seconds),
            )
            return [dict(row) for row in await cursor.fetchall()]

    async def mark_delivered(
        self,
        *,
        outbox_id: int,
        lease_owner: str,
    ) -> bool:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE outbox_events
                SET status = 'delivered',
                    delivered_at = now(),
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_error = NULL
                WHERE outbox_id = %s
                  AND status = 'leased'
                  AND lease_owner = %s
                """,
                (outbox_id, lease_owner),
            )
            return cursor.rowcount == 1

    async def mark_failed(
        self,
        *,
        outbox_id: int,
        lease_owner: str,
        error: str,
        retryable: bool,
        attempts: int,
        max_attempts: int,
        delay_seconds: float,
    ) -> str:
        should_block = not retryable or attempts >= max_attempts
        status = "blocked" if should_block else "pending"
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE outbox_events
                SET status = %s,
                    available_at = CASE
                        WHEN %s THEN available_at
                        ELSE now() + (%s * interval '1 second')
                    END,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_error = %s,
                    blocked_at = CASE WHEN %s THEN now() ELSE NULL END
                WHERE outbox_id = %s
                  AND status = 'leased'
                  AND lease_owner = %s
                """,
                (
                    status,
                    should_block,
                    delay_seconds,
                    error[:4000],
                    should_block,
                    outbox_id,
                    lease_owner,
                ),
            )
            return status if cursor.rowcount == 1 else "lease_lost"

    async def stats(self) -> dict[str, Any]:
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE status IN ('pending', 'leased')
                    ) AS pending_count,
                    COALESCE(
                        EXTRACT(
                            EPOCH FROM (
                                now() - MIN(created_at) FILTER (
                                    WHERE status IN ('pending', 'leased')
                                )
                            )
                        ),
                        0
                    )::DOUBLE PRECISION AS oldest_pending_age_seconds,
                    COALESCE(
                        SUM(GREATEST(attempts - 1, 0)) FILTER (
                            WHERE status IN ('pending', 'leased', 'blocked')
                        ),
                        0
                    )::BIGINT AS retry_count,
                    COUNT(*) FILTER (WHERE status = 'blocked') AS blocked_count,
                    COUNT(*) FILTER (WHERE status = 'delivered') AS delivered_count
                FROM outbox_events
                """
            )
            row = await cursor.fetchone()
            return {
                "pending_count": int(row["pending_count"]),
                "oldest_pending_age_seconds": round(
                    float(row["oldest_pending_age_seconds"]), 3
                ),
                "retry_count": int(row["retry_count"]),
                "blocked_count": int(row["blocked_count"]),
                "delivered_count": int(row["delivered_count"]),
            }

    async def list_blocked(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        async with self.database.connection() as connection:
            cursor = await connection.execute(
                """
                SELECT
                    outbox_id,
                    event_id,
                    event_name,
                    service,
                    aggregate_type,
                    aggregate_id,
                    correlation_id,
                    attempts,
                    last_error,
                    created_at,
                    blocked_at
                FROM outbox_events
                WHERE status = 'blocked'
                ORDER BY blocked_at DESC, outbox_id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [
                {
                    **dict(row),
                    "created_at": row["created_at"].isoformat(),
                    "blocked_at": row["blocked_at"].isoformat(),
                }
                for row in await cursor.fetchall()
            ]


class OutboxDispatcher:
    """At-least-once HTTP delivery with expiring claims and bounded retries."""

    def __init__(
        self,
        repository: PostgresOutboxRepository,
        *,
        collector_url: str,
        batch_size: int = 50,
        lease_seconds: int = 30,
        max_attempts: int = 12,
        base_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 300.0,
        poll_interval_seconds: float = 0.5,
        request_timeout_seconds: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
        worker_id: str | None = None,
    ) -> None:
        if not collector_url.startswith(("http://", "https://")):
            raise ValueError("collector_url must be an HTTP(S) URL")
        if batch_size < 1 or lease_seconds < 1 or max_attempts < 1:
            raise ValueError(
                "batch_size, lease_seconds, and max_attempts must be positive"
            )
        if (
            base_backoff_seconds < 0
            or max_backoff_seconds < base_backoff_seconds
            or poll_interval_seconds <= 0
            or request_timeout_seconds <= 0
        ):
            raise ValueError("Invalid outbox timing configuration")

        self.repository = repository
        self.collector_url = collector_url.rstrip("/")
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self.transport = transport
        self.worker_id = worker_id or str(uuid4())

    @asynccontextmanager
    async def _client(self) -> AsyncIterator[httpx.AsyncClient]:
        async with httpx.AsyncClient(
            base_url=self.collector_url,
            timeout=self.request_timeout_seconds,
            transport=self.transport,
        ) as client:
            yield client

    async def dispatch_once(self) -> dict[str, int]:
        """Claim one bounded batch and attempt each row exactly once."""
        claims = await self.repository.claim_batch(
            lease_owner=self.worker_id,
            batch_size=self.batch_size,
            lease_seconds=self.lease_seconds,
        )
        if not claims:
            return {
                "claimed": 0,
                "delivered": 0,
                "retried": 0,
                "blocked": 0,
                "lease_lost": 0,
            }

        async with self._client() as client:
            results = await asyncio.gather(
                *(self._deliver_one(client, claim) for claim in claims),
                return_exceptions=True,
            )
        summary = {
            "claimed": len(claims),
            "delivered": 0,
            "retried": 0,
            "blocked": 0,
            "lease_lost": 0,
        }
        for result in results:
            if isinstance(result, BaseException):
                logger.exception(
                    "outbox.dispatch.unhandled",
                    exc_info=(type(result), result, result.__traceback__),
                )
                continue
            summary[result] += 1
        return summary

    async def _deliver_one(
        self,
        client: httpx.AsyncClient,
        claim: dict[str, Any],
    ) -> str:
        outbox_id = int(claim["outbox_id"])
        event = claim["event_payload"]
        try:
            response = await client.post(
                "/v1/events",
                json={"events": [event]},
                headers={
                    "X-Correlation-ID": claim.get("correlation_id") or ""
                },
            )
            response.raise_for_status()
            try:
                acknowledgement = response.json()
            except ValueError as exc:
                raise OutboxProtocolError(
                    "Collector returned a non-JSON acknowledgement"
                ) from exc
            if not isinstance(acknowledgement, dict):
                raise OutboxProtocolError(
                    "Collector acknowledgement must be a JSON object"
                )
            counts: dict[str, int] = {}
            for field in ("accepted", "duplicates", "quarantined"):
                value = acknowledgement.get(field, 0)
                if type(value) is not int or value < 0:
                    raise OutboxProtocolError(
                        f"Collector acknowledgement has invalid {field} count"
                    )
                counts[field] = value
            if counts["quarantined"] > 0:
                raise OutboxProtocolError(
                    "Collector quarantined the outbox event"
                )
            if counts["accepted"] + counts["duplicates"] != 1:
                raise OutboxProtocolError(
                    "Collector acknowledgement did not account for exactly one event"
                )
        except OutboxProtocolError as exc:
            return await self._record_failure(
                claim, exc, retryable=False
            )
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            retryable = status_code in {408, 409, 425, 429} or status_code >= 500
            return await self._record_failure(
                claim, exc, retryable=retryable
            )
        except httpx.HTTPError as exc:
            return await self._record_failure(
                claim, exc, retryable=True
            )

        acknowledged = await self.repository.mark_delivered(
            outbox_id=outbox_id,
            lease_owner=self.worker_id,
        )
        if not acknowledged:
            logger.warning(
                "outbox.dispatch.lease_lost_after_ack",
                extra={"outbox_id": outbox_id, "event_id": str(claim["event_id"])},
            )
            return "lease_lost"
        return "delivered"

    async def _record_failure(
        self,
        claim: dict[str, Any],
        error: Exception,
        *,
        retryable: bool,
    ) -> str:
        attempts = int(claim["attempts"])
        delay = min(
            self.base_backoff_seconds * (2 ** max(attempts - 1, 0)),
            self.max_backoff_seconds,
        )
        status = await self.repository.mark_failed(
            outbox_id=int(claim["outbox_id"]),
            lease_owner=self.worker_id,
            error=f"{type(error).__name__}: {error}",
            retryable=retryable,
            attempts=attempts,
            max_attempts=self.max_attempts,
            delay_seconds=delay,
        )
        if status == "lease_lost":
            return status
        logger.log(
            logging.WARNING if status == "blocked" else logging.INFO,
            "outbox.dispatch.failed",
            extra={
                "outbox_id": claim["outbox_id"],
                "event_id": str(claim["event_id"]),
                "event_name": claim["event_name"],
                "attempts": attempts,
                "status": status,
                "retryable": retryable,
                "error": str(error),
            },
        )
        return "blocked" if status == "blocked" else "retried"

    async def run(self, stop: asyncio.Event) -> None:
        """Run until shutdown is requested; finish the current claimed batch."""
        logger.info(
            "outbox.dispatcher.started",
            extra={"worker_id": self.worker_id, "batch_size": self.batch_size},
        )
        while not stop.is_set():
            result = await self.dispatch_once()
            if result["claimed"]:
                continue
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self.poll_interval_seconds,
                )
            except TimeoutError:
                pass
        logger.info(
            "outbox.dispatcher.stopped",
            extra={"worker_id": self.worker_id},
        )
