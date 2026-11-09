from __future__ import annotations

import asyncio
import logging
import os
from time import perf_counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from opentelemetry import metrics, trace

from app.gateway_simulator.metrics import GatewayMetrics
from app.gateway_simulator.policy import (
    GatewayPolicy,
    GatewayRuntime,
    PolicyRevisionConflict,
    PolicyStore,
    PolicyUpdate,
    RoutePolicy,
    decide_request,
)
from app.observability import configure_observability, correlation_id_context

logger = logging.getLogger(__name__)
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}
GATEWAY_CONTROL_PREFIX = "/_simulator"


def default_policy() -> GatewayPolicy:
    order_url = os.getenv("ORDER_API_URL", "http://localhost:8000")
    inventory_url = os.getenv("INVENTORY_API_URL", "http://localhost:8001")
    supplier_url = os.getenv("SUPPLIER_API_URL", "http://localhost:8002")
    routes = (
        RoutePolicy(
            route_id="order-api",
            path_prefix="/orders",
            upstream_url=order_url,
        ),
        RoutePolicy(
            route_id="inventory-products",
            path_prefix="/products",
            upstream_url=inventory_url,
        ),
        RoutePolicy(
            route_id="inventory",
            path_prefix="/inventory",
            upstream_url=inventory_url,
        ),
        RoutePolicy(
            route_id="supplier-catalog",
            path_prefix="/supplier-catalog",
            upstream_url=supplier_url,
        ),
        RoutePolicy(
            route_id="supplier-policies",
            path_prefix="/reorder-policies",
            upstream_url=supplier_url,
        ),
        RoutePolicy(
            route_id="supplier-replenishment",
            path_prefix="/replenishments",
            upstream_url=supplier_url,
        ),
        RoutePolicy(
            route_id="supplier-purchase-orders",
            path_prefix="/purchase-orders",
            upstream_url=supplier_url,
        ),
        RoutePolicy(
            route_id="supplier-clock",
            path_prefix="/clock",
            upstream_url=supplier_url,
        ),
    )
    return GatewayPolicy(
        seed=int(os.getenv("GATEWAY_SIMULATION_SEED", "20261109")),
        routes=routes,
    )


policy_store = PolicyStore(default_policy())
gateway_runtime = GatewayRuntime()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.http_client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=40),
        follow_redirects=False,
    )
    try:
        yield
    finally:
        await app.state.http_client.aclose()


app = FastAPI(
    title="API Gateway Behavior Simulator",
    description=(
        "Local reverse proxy for deterministic routing, latency, throttling, "
        "concurrency, and upstream-failure experiments."
    ),
    version="0.1.0",
    lifespan=lifespan,
)
app.state.policy_store = policy_store
app.state.gateway_runtime = gateway_runtime
configure_observability(app, os.getenv("SERVICE_NAME", "gateway-simulator"))
app.state.gateway_metrics = GatewayMetrics(
    metrics.get_meter("app.gateway_simulator")
)


@app.middleware("http")
async def collect_gateway_request_metrics(request: Request, call_next):
    """Record data-plane requests without high-cardinality request attributes."""
    if request.url.path.startswith(GATEWAY_CONTROL_PREFIX):
        return await call_next(request)

    started_at = perf_counter()
    status_code = 500
    simulation_outcome = None
    try:
        response = await call_next(request)
        status_code = response.status_code
        simulation_outcome = response.headers.get("X-Simulation-Decision")
        return response
    finally:
        request.app.state.gateway_metrics.record(
            method=request.method,
            status_code=status_code,
            simulation_outcome=simulation_outcome,
            duration_seconds=perf_counter() - started_at,
        )


def gateway_headers(
    *,
    route_id: str,
    revision: int,
    correlation_id: str,
    decision: str,
) -> dict[str, str]:
    return {
        "X-Correlation-ID": correlation_id,
        "X-Gateway-Route": route_id,
        "X-Gateway-Policy-Revision": str(revision),
        "X-Simulation-Decision": decision,
    }


def simulator_response(
    *,
    status_code: int,
    detail: str,
    route_id: str,
    revision: int,
    correlation_id: str,
    decision: str,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    response_headers = gateway_headers(
        route_id=route_id,
        revision=revision,
        correlation_id=correlation_id,
        decision=decision,
    )
    if headers:
        response_headers.update(headers)
    return JSONResponse(
        status_code=status_code,
        content={"detail": detail, "route_id": route_id, "policy_revision": revision},
        headers=response_headers,
    )


@app.get("/_simulator/healthz", tags=["simulator"])
async def healthcheck():
    policy = app.state.policy_store.snapshot()
    return {
        "status": "ok",
        "service": "gateway-simulator",
        "policy_revision": policy.revision,
        "route_count": len(policy.routes),
    }


@app.get("/_simulator/policy", tags=["simulator"])
async def get_policy():
    policy = app.state.policy_store.snapshot()
    return {
        **policy.model_dump(mode="json"),
        "audit": app.state.policy_store.audit(),
    }


@app.put("/_simulator/policy", tags=["simulator"])
async def replace_policy(update: PolicyUpdate):
    try:
        policy = await app.state.policy_store.replace(update)
    except PolicyRevisionConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None

    logger.info(
        "gateway.policy.updated",
        extra={
            "policy_revision": policy.revision,
            "previous_revision": update.expected_revision,
            "route_count": len(policy.routes),
            "scenario_seed": policy.seed,
        },
    )
    return policy.model_dump(mode="json")


@app.get("/_simulator/stats", tags=["simulator"])
async def get_stats():
    return await app.state.gateway_runtime.snapshot()


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
    tags=["gateway"],
)
async def proxy(path: str, request: Request):
    if request.url.path.startswith(GATEWAY_CONTROL_PREFIX):
        raise HTTPException(status_code=404, detail="Route not found")

    policy = request.app.state.policy_store.snapshot()
    route = policy.match(request.method, request.url.path)
    if route is None:
        raise HTTPException(
            status_code=404,
            detail="No gateway route matches this request",
        )

    correlation_id = (
        correlation_id_context.get()
        or request.headers.get("X-Correlation-ID")
        or str(uuid4())
    )
    request_key = (
        request.headers.get("X-Simulation-Request-ID")
        or correlation_id
    )
    decision = decide_request(policy, route, request_key)

    span = trace.get_current_span()
    if span.is_recording():
        span.set_attribute("gateway.route_id", route.route_id)
        span.set_attribute("gateway.policy.revision", policy.revision)
        span.set_attribute("gateway.simulation.request_key", request_key)
        span.set_attribute("gateway.simulation.latency_ms", decision.latency_ms)

    admission = await request.app.state.gateway_runtime.try_admit(policy, route)
    if admission == "rate_limited":
        logger.warning(
            "gateway.request.rate_limited",
            extra={"route_id": route.route_id, "policy_revision": policy.revision},
        )
        return simulator_response(
            status_code=429,
            detail="Gateway rate limit exceeded",
            route_id=route.route_id,
            revision=policy.revision,
            correlation_id=correlation_id,
            decision=admission,
            headers={"Retry-After": "1"},
        )
    if admission == "overloaded":
        logger.warning(
            "gateway.request.overloaded",
            extra={"route_id": route.route_id, "policy_revision": policy.revision},
        )
        return simulator_response(
            status_code=503,
            detail="Gateway concurrency limit exceeded",
            route_id=route.route_id,
            revision=policy.revision,
            correlation_id=correlation_id,
            decision=admission,
            headers={"Retry-After": "1"},
        )

    try:
        if decision.latency_ms:
            await asyncio.sleep(decision.latency_ms / 1000)

        if decision.inject_failure:
            if decision.failure_mode == "timeout":
                await asyncio.sleep(route.timeout_seconds)
                await request.app.state.gateway_runtime.record(
                    route.route_id, "injected_timeout"
                )
                return simulator_response(
                    status_code=504,
                    detail="Gateway injected an upstream timeout",
                    route_id=route.route_id,
                    revision=policy.revision,
                    correlation_id=correlation_id,
                    decision="injected_timeout",
                )
            if decision.failure_mode == "connection":
                await request.app.state.gateway_runtime.record(
                    route.route_id, "injected_connection_failure"
                )
                return simulator_response(
                    status_code=502,
                    detail="Gateway injected an upstream connection failure",
                    route_id=route.route_id,
                    revision=policy.revision,
                    correlation_id=correlation_id,
                    decision="injected_connection_failure",
                )
            await request.app.state.gateway_runtime.record(
                route.route_id, "injected_http_failure"
            )
            return simulator_response(
                status_code=route.failure_status_code,
                detail="Gateway injected an upstream HTTP failure",
                route_id=route.route_id,
                revision=policy.revision,
                correlation_id=correlation_id,
                decision="injected_http_failure",
            )

        connection_tokens = {
            token.strip().lower()
            for token in request.headers.get("connection", "").split(",")
            if token.strip()
        }
        excluded_request_headers = HOP_BY_HOP_HEADERS | connection_tokens | {
            "x-simulation-request-id",
            "x-correlation-id",
        }
        forwarded_headers = [
            (name.decode("latin-1"), value.decode("latin-1"))
            for name, value in request.scope.get("headers", [])
            if name.decode("latin-1").lower() not in excluded_request_headers
        ]
        forwarded_headers.append(("X-Correlation-ID", correlation_id))
        target_url = f"{route.upstream_url}{request.url.path}"
        if request.url.query:
            target_url = f"{target_url}?{request.url.query}"

        upstream_response = await request.app.state.http_client.request(
            request.method,
            target_url,
            content=await request.body(),
            headers=forwarded_headers,
            timeout=route.timeout_seconds,
        )
        await request.app.state.gateway_runtime.record(
            route.route_id, f"upstream_{upstream_response.status_code // 100}xx"
        )

        connection_tokens = {
            token.strip().lower()
            for token in upstream_response.headers.get("connection", "").split(",")
            if token.strip()
        }
        excluded_response_headers = (
            HOP_BY_HOP_HEADERS
            | connection_tokens
            | {
                "content-encoding",
                "x-correlation-id",
                "x-gateway-route",
                "x-gateway-policy-revision",
                "x-simulation-decision",
            }
        )
        response = Response(
            content=upstream_response.content,
            status_code=upstream_response.status_code,
        )
        response.raw_headers = [
            (b"content-length", str(len(upstream_response.content)).encode("ascii")),
            *[
                (name.lower().encode("latin-1"), value.encode("latin-1"))
                for name, value in upstream_response.headers.multi_items()
                if name.lower() not in excluded_response_headers
            ],
            *[
                (name.lower().encode("latin-1"), value.encode("latin-1"))
                for name, value in gateway_headers(
                    route_id=route.route_id,
                    revision=policy.revision,
                    correlation_id=correlation_id,
                    decision="forwarded",
                ).items()
            ],
        ]
        logger.info(
            "gateway.request.forwarded",
            extra={
                "route_id": route.route_id,
                "policy_revision": policy.revision,
                "upstream_status": upstream_response.status_code,
                "decision": "forwarded",
            },
        )
        return response
    except httpx.TimeoutException:
        await request.app.state.gateway_runtime.record(
            route.route_id, "upstream_timeout"
        )
        logger.warning(
            "gateway.request.upstream_timeout",
            extra={"route_id": route.route_id, "policy_revision": policy.revision},
        )
        return simulator_response(
            status_code=504,
            detail="Upstream request timed out",
            route_id=route.route_id,
            revision=policy.revision,
            correlation_id=correlation_id,
            decision="upstream_timeout",
        )
    except httpx.HTTPError:
        await request.app.state.gateway_runtime.record(
            route.route_id, "upstream_unavailable"
        )
        logger.warning(
            "gateway.request.upstream_unavailable",
            extra={"route_id": route.route_id, "policy_revision": policy.revision},
        )
        return simulator_response(
            status_code=502,
            detail="Upstream service is unavailable",
            route_id=route.route_id,
            revision=policy.revision,
            correlation_id=correlation_id,
            decision="upstream_unavailable",
        )
    finally:
        await request.app.state.gateway_runtime.release(policy, route)
