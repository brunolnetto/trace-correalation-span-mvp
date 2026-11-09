from __future__ import annotations

import asyncio
import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class RoutePolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    route_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9_-]*$")
    path_prefix: str = Field(min_length=1, max_length=256)
    upstream_url: str = Field(min_length=8, max_length=512)
    methods: tuple[str, ...] = Field(
        default=("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"),
        max_length=16,
    )
    latency_min_ms: int = Field(default=0, ge=0, le=30_000)
    latency_max_ms: int = Field(default=0, ge=0, le=30_000)
    failure_probability: float = Field(default=0.0, ge=0.0, le=1.0)
    failure_mode: Literal["http", "timeout", "connection"] = "http"
    failure_status_code: int = Field(default=503, ge=400, le=599)
    requests_per_second: float | None = Field(default=None, gt=0, le=100_000)
    rate_limit_burst: int = Field(default=1, ge=1, le=100_000)
    max_in_flight: int | None = Field(default=None, ge=1, le=100_000)
    timeout_seconds: float = Field(default=5.0, gt=0, le=120)

    @field_validator("path_prefix")
    @classmethod
    def normalize_path_prefix(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("path_prefix must start with '/'")
        if value != "/":
            value = value.rstrip("/")
        return value

    @field_validator("upstream_url")
    @classmethod
    def validate_upstream_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("upstream_url must be an absolute HTTP(S) URL")
        if parsed.query or parsed.fragment:
            raise ValueError("upstream_url cannot contain a query or fragment")
        return value.rstrip("/")

    @field_validator("methods")
    @classmethod
    def normalize_methods(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        methods = tuple(method.upper() for method in value)
        if not methods or len(set(methods)) != len(methods):
            raise ValueError("methods must contain unique HTTP methods")
        return methods

    @model_validator(mode="after")
    def validate_ranges(self) -> RoutePolicy:
        if self.latency_min_ms > self.latency_max_ms:
            raise ValueError("latency_min_ms cannot exceed latency_max_ms")
        if self.requests_per_second is None and self.rate_limit_burst != 1:
            raise ValueError(
                "rate_limit_burst is meaningful only when requests_per_second is set"
            )
        return self


class GatewayPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    revision: int = Field(default=1, ge=1)
    seed: int = Field(default=20261109, ge=0, le=2**63 - 1)
    routes: tuple[RoutePolicy, ...] = Field(max_length=128)

    @model_validator(mode="after")
    def validate_route_ids(self) -> GatewayPolicy:
        route_ids = [route.route_id for route in self.routes]
        if len(route_ids) != len(set(route_ids)):
            raise ValueError("route_id values must be unique")
        return self

    def match(self, method: str, path: str) -> RoutePolicy | None:
        candidates = [
            route
            for route in self.routes
            if method.upper() in route.methods
            and (
                route.path_prefix == "/"
                or path == route.path_prefix
                or path.startswith(f"{route.path_prefix}/")
            )
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda route: len(route.path_prefix))


class PolicyUpdate(BaseModel):
    expected_revision: int = Field(ge=1)
    seed: int = Field(default=20261109, ge=0, le=2**63 - 1)
    routes: tuple[RoutePolicy, ...] = Field(max_length=128)


class PolicyRevisionConflict(ValueError):
    pass


class PolicyStore:
    def __init__(self, initial: GatewayPolicy):
        self._policy = initial
        self._lock = asyncio.Lock()
        self._audit = [
            {
                "revision": initial.revision,
                "action": "initialized",
                "route_count": len(initial.routes),
            }
        ]

    def snapshot(self) -> GatewayPolicy:
        # GatewayPolicy and RoutePolicy are frozen and use tuples; callers get
        # an immutable request-time snapshot without holding the update lock.
        return self._policy

    async def replace(self, update: PolicyUpdate) -> GatewayPolicy:
        async with self._lock:
            current = self._policy
            if update.expected_revision != current.revision:
                raise PolicyRevisionConflict(
                    f"expected revision {update.expected_revision}, "
                    f"current revision is {current.revision}"
                )
            next_policy = GatewayPolicy(
                revision=current.revision + 1,
                seed=update.seed,
                routes=update.routes,
            )
            self._policy = next_policy
            self._audit.append(
                {
                    "revision": next_policy.revision,
                    "action": "replaced",
                    "previous_revision": current.revision,
                    "route_count": len(next_policy.routes),
                }
            )
            self._audit = self._audit[-100:]
            return next_policy

    def audit(self) -> list[dict[str, int | str]]:
        return [entry.copy() for entry in self._audit]


@dataclass(frozen=True)
class RequestDecision:
    latency_ms: int
    inject_failure: bool
    failure_mode: str
    failure_status_code: int


def decide_request(
    policy: GatewayPolicy,
    route: RoutePolicy,
    request_key: str,
) -> RequestDecision:
    """Derive stable latency/fault decisions from policy revision and request key."""
    material = (
        f"{policy.seed}:{policy.revision}:{route.route_id}:{request_key}".encode()
    )
    digest = hashlib.sha256(material).digest()
    failure_draw = int.from_bytes(digest[:8], "big") / 2**64
    latency_draw = int.from_bytes(digest[8:16], "big") / 2**64
    latency_range = route.latency_max_ms - route.latency_min_ms
    latency_ms = route.latency_min_ms + round(latency_draw * latency_range)
    return RequestDecision(
        latency_ms=latency_ms,
        inject_failure=failure_draw < route.failure_probability,
        failure_mode=route.failure_mode,
        failure_status_code=route.failure_status_code,
    )


@dataclass
class _RouteRuntime:
    tokens: float
    updated_at: float
    in_flight: int = 0


class GatewayRuntime:
    """Process-local token buckets and concurrency counters for local experiments."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._states: dict[tuple[int, str], _RouteRuntime] = {}
        self._counts: dict[tuple[str, str], int] = defaultdict(int)

    async def try_admit(
        self,
        policy: GatewayPolicy,
        route: RoutePolicy,
        *,
        now: float | None = None,
    ) -> str | None:
        current_time = time.monotonic() if now is None else now
        key = (policy.revision, route.route_id)
        async with self._lock:
            state = self._states.get(key)
            if state is None:
                initial_tokens = (
                    float(route.rate_limit_burst)
                    if route.requests_per_second is not None
                    else 0.0
                )
                state = _RouteRuntime(tokens=initial_tokens, updated_at=current_time)
                self._states[key] = state

            if (
                route.max_in_flight is not None
                and state.in_flight >= route.max_in_flight
            ):
                self._counts[(route.route_id, "overloaded")] += 1
                return "overloaded"

            if route.requests_per_second is not None:
                elapsed = max(0.0, current_time - state.updated_at)
                state.tokens = min(
                    float(route.rate_limit_burst),
                    state.tokens + elapsed * route.requests_per_second,
                )
                state.updated_at = current_time
                if state.tokens < 1.0:
                    self._counts[(route.route_id, "rate_limited")] += 1
                    return "rate_limited"
                state.tokens -= 1.0

            state.in_flight += 1
            return None

    async def release(self, policy: GatewayPolicy, route: RoutePolicy) -> None:
        key = (policy.revision, route.route_id)
        async with self._lock:
            state = self._states.get(key)
            if state is not None:
                state.in_flight = max(0, state.in_flight - 1)

    async def record(self, route_id: str, outcome: str) -> None:
        async with self._lock:
            self._counts[(route_id, outcome)] += 1

    async def snapshot(self) -> dict[str, object]:
        async with self._lock:
            routes: dict[str, dict[str, int]] = {}
            for (route_id, outcome), count in sorted(self._counts.items()):
                routes.setdefault(route_id, {})[outcome] = count
            return {"routes": routes}
