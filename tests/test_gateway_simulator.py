from __future__ import annotations

import asyncio
import io
import json
import logging
from importlib import import_module

import httpx
import pytest
from fastapi import FastAPI, Request
from opentelemetry import trace
from opentelemetry.propagate import get_global_textmap, set_global_textmap
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

from app import observability
from app.gateway_simulator.metrics import (
    GATEWAY_METRIC_VIEWS,
    GATEWAY_REQUEST_DURATION_BUCKETS_SECONDS,
    GatewayMetrics,
    metric_attributes,
)
from app.gateway_simulator.policy import (
    GatewayPolicy,
    GatewayRuntime,
    PolicyRevisionConflict,
    PolicyStore,
    PolicyUpdate,
    RoutePolicy,
    decide_request,
)

configure_observability = observability.configure_observability
observability.configure_observability = (
    lambda app, service_name, *, metric_views=(): None
)
try:
    gateway_module = import_module("app.gateway_simulator.main")
finally:
    observability.configure_observability = configure_observability

gateway_app = gateway_module.app


def make_route(**overrides) -> RoutePolicy:
    values = {
        "route_id": "orders",
        "path_prefix": "/orders",
        "upstream_url": "http://order.test",
    }
    values.update(overrides)
    return RoutePolicy(**values)


def make_policy(route: RoutePolicy | None = None, *, revision: int = 1) -> GatewayPolicy:
    return GatewayPolicy(
        revision=revision,
        seed=42,
        routes=(route or make_route(),),
    )


@pytest.fixture(autouse=True)
async def reset_gateway_state():
    gateway_app.state.policy_store = PolicyStore(make_policy())
    gateway_app.state.gateway_runtime = GatewayRuntime()
    gateway_app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"ok": True})
        )
    )
    yield
    await gateway_app.state.http_client.aclose()


async def test_policy_matches_longest_path_prefix_and_respects_boundaries():
    broad = make_route(route_id="broad", path_prefix="/api")
    narrow = make_route(route_id="narrow", path_prefix="/api/orders")
    policy = GatewayPolicy(revision=1, seed=42, routes=(broad, narrow))

    assert policy.match("GET", "/api/orders/7") == narrow
    assert policy.match("GET", "/api/products") == broad
    assert policy.match("GET", "/apiculture") is None
    assert policy.match("HEAD", "/api/orders/7") == narrow


async def test_route_policy_rejects_invalid_ranges_and_non_http_upstreams():
    with pytest.raises(ValueError, match="cannot exceed"):
        make_route(latency_min_ms=100, latency_max_ms=10)
    with pytest.raises(ValueError, match="absolute HTTP"):
        make_route(upstream_url="redis://order.test")


async def test_decision_is_reproducible_for_same_policy_and_request_key():
    route = make_route(
        latency_min_ms=25,
        latency_max_ms=250,
        failure_probability=0.4,
    )
    policy = make_policy(route)

    first = decide_request(policy, route, "scenario-1/order-123")
    second = decide_request(policy, route, "scenario-1/order-123")
    assert first == second
    assert 25 <= first.latency_ms <= 250


@pytest.mark.asyncio
async def test_policy_revision_update_is_compare_and_swap_and_audited():
    store = PolicyStore(make_policy())
    updated = await store.replace(
        PolicyUpdate(
            expected_revision=1,
            seed=43,
            routes=(make_route(latency_min_ms=10, latency_max_ms=20),),
        )
    )

    assert updated.revision == 2
    assert updated.seed == 43
    assert store.snapshot().revision == 2
    assert store.audit()[-1] == {
        "revision": 2,
        "action": "replaced",
        "previous_revision": 1,
        "route_count": 1,
    }

    with pytest.raises(PolicyRevisionConflict):
        await store.replace(
            PolicyUpdate(expected_revision=1, seed=44, routes=(make_route(),))
        )


@pytest.mark.asyncio
async def test_token_bucket_and_max_in_flight_are_enforced():
    route = make_route(
        requests_per_second=1,
        rate_limit_burst=1,
        max_in_flight=1,
    )
    policy = make_policy(route)
    runtime = GatewayRuntime()

    assert await runtime.try_admit(policy, route, now=10.0) is None
    assert await runtime.try_admit(policy, route, now=10.0) == "overloaded"
    await runtime.release(policy, route)

    # The first request consumed the only token; releasing concurrency does not
    # replenish the token bucket.
    assert await runtime.try_admit(policy, route, now=10.0) == "rate_limited"
    assert await runtime.try_admit(policy, route, now=11.0) is None
    await runtime.release(policy, route)


@pytest.mark.asyncio
async def test_proxy_preserves_method_query_body_and_correlation_context():
    captured = {}

    async def upstream_handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = request.content
        captured["correlation_id"] = request.headers.get("x-correlation-id")
        return httpx.Response(
            201,
            json={"created": True},
            headers={"X-Upstream": "order-api"},
        )

    await gateway_app.state.http_client.aclose()
    gateway_app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(upstream_handler)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app),
        base_url="http://gateway.test",
    ) as client:
        response = await client.post(
            "/orders/123/process?dry_run=true",
            headers={
                "X-Correlation-ID": "550e8400-e29b-41d4-a716-446655440011",
                "X-Simulation-Request-ID": "request-123",
            },
            json={"quantity": 2},
        )

    assert response.status_code == 201
    assert response.headers["x-gateway-route"] == "orders"
    assert response.headers["x-gateway-policy-revision"] == "1"
    assert response.headers["x-correlation-id"] == "550e8400-e29b-41d4-a716-446655440011"
    assert response.headers["x-upstream"] == "order-api"
    assert captured == {
        "method": "POST",
        "url": "http://order.test/orders/123/process?dry_run=true",
        "body": b'{"quantity":2}',
        "correlation_id": "550e8400-e29b-41d4-a716-446655440011",
    }


@pytest.mark.asyncio
async def test_proxy_returns_deterministic_injected_http_failure_without_upstream_call():
    gateway_app.state.policy_store = PolicyStore(
        make_policy(make_route(failure_probability=1.0, failure_status_code=429))
    )
    calls = 0

    async def upstream_handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200)

    await gateway_app.state.http_client.aclose()
    gateway_app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(upstream_handler)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app),
        base_url="http://gateway.test",
    ) as client:
        response = await client.get(
            "/orders/123",
            headers={"X-Simulation-Request-ID": "forced-failure"},
        )

    assert response.status_code == 429
    assert response.json()["detail"] == "Gateway injected an upstream HTTP failure"
    assert response.headers["x-simulation-decision"] == "injected_http_failure"
    assert calls == 0


@pytest.mark.asyncio
async def test_policy_control_endpoint_requires_expected_revision():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app),
        base_url="http://gateway.test",
    ) as client:
        update = await client.put(
            "/_simulator/policy",
            json={
                "expected_revision": 1,
                "seed": 99,
                "routes": [make_route().model_dump(mode="json")],
            },
        )
        conflict = await client.put(
            "/_simulator/policy",
            json={
                "expected_revision": 1,
                "seed": 100,
                "routes": [make_route().model_dump(mode="json")],
            },
        )

    assert update.status_code == 200
    assert update.json()["revision"] == 2
    assert conflict.status_code == 409


@pytest.mark.asyncio
async def test_unmatched_path_is_not_forwarded():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app),
        base_url="http://gateway.test",
    ) as client:
        response = await client.get("/not-configured")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_timeout_fault_injection_returns_gateway_504():
    gateway_app.state.policy_store = PolicyStore(
        make_policy(
            make_route(
                failure_probability=1.0,
                failure_mode="timeout",
                timeout_seconds=0.001,
            )
        )
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app),
        base_url="http://gateway.test",
    ) as client:
        response = await client.get(
            "/orders/123",
            headers={"X-Simulation-Request-ID": "forced-timeout"},
        )

    assert response.status_code == 504
    assert response.headers["x-simulation-decision"] == "injected_timeout"


@pytest.mark.asyncio
async def test_request_keeps_old_policy_snapshot_during_policy_update():
    store = PolicyStore(make_policy(make_route(latency_min_ms=10, latency_max_ms=10)))
    admitted_policy = store.snapshot()
    await store.replace(
        PolicyUpdate(
            expected_revision=1,
            seed=77,
            routes=(make_route(latency_min_ms=200, latency_max_ms=200),),
        )
    )

    assert admitted_policy.revision == 1
    assert admitted_policy.routes[0].latency_min_ms == 10
    assert store.snapshot().revision == 2
    assert store.snapshot().routes[0].latency_min_ms == 200


@pytest.mark.asyncio
async def test_proxy_preserves_multiple_set_cookie_headers():
    async def upstream_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"ok",
            headers=[
                ("Set-Cookie", "session=one; HttpOnly"),
                ("Set-Cookie", "preference=dark; Path=/"),
                ("Content-Encoding", "identity"),
            ],
        )

    await gateway_app.state.http_client.aclose()
    gateway_app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(upstream_handler)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app),
        base_url="http://gateway.test",
    ) as client:
        response = await client.get(
            "/orders/123",
            headers={"X-Correlation-ID": "550e8400-e29b-41d4-a716-446655440011"},
        )

    assert response.status_code == 200
    assert response.headers.get_list("set-cookie") == [
        "session=one; HttpOnly",
        "preference=dark; Path=/",
    ]



@pytest.mark.asyncio
async def test_proxy_enforces_max_in_flight_under_concurrent_http_requests():
    started = asyncio.Event()
    release = asyncio.Event()

    async def upstream_handler(request: httpx.Request) -> httpx.Response:
        started.set()
        await release.wait()
        return httpx.Response(200, json={"ok": True})

    gateway_app.state.policy_store = PolicyStore(
        make_policy(make_route(max_in_flight=1))
    )
    await gateway_app.state.http_client.aclose()
    gateway_app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(upstream_handler)
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app),
        base_url="http://gateway.test",
    ) as client:
        first_request = asyncio.create_task(client.get("/orders/first"))
        await asyncio.wait_for(started.wait(), timeout=1)
        second_response = await client.get("/orders/second")
        release.set()
        first_response = await first_request

    assert first_response.status_code == 200
    assert second_response.status_code == 503
    assert second_response.headers["x-simulation-decision"] == "overloaded"


@pytest.mark.asyncio
async def test_proxy_enforces_token_bucket_under_concurrent_http_requests():
    started = asyncio.Event()
    release = asyncio.Event()

    async def upstream_handler(request: httpx.Request) -> httpx.Response:
        started.set()
        await release.wait()
        return httpx.Response(200, json={"ok": True})

    gateway_app.state.policy_store = PolicyStore(
        make_policy(
            make_route(
                requests_per_second=0.001,
                rate_limit_burst=1,
            )
        )
    )
    await gateway_app.state.http_client.aclose()
    gateway_app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(upstream_handler)
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app),
        base_url="http://gateway.test",
    ) as client:
        first_request = asyncio.create_task(client.get("/orders/first"))
        await asyncio.wait_for(started.wait(), timeout=1)
        second_response = await client.get("/orders/second")
        release.set()
        first_response = await first_request

    assert first_response.status_code == 200
    assert second_response.status_code == 429
    assert second_response.headers["x-simulation-decision"] == "rate_limited"


@pytest.mark.asyncio
async def test_baseline_degraded_policy_and_recovery_are_observable():
    upstream_calls = 0

    async def upstream_handler(request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        return httpx.Response(200, json={"ok": True})

    await gateway_app.state.http_client.aclose()
    gateway_app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(upstream_handler)
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app),
        base_url="http://gateway.test",
    ) as client:
        headers = {"X-Simulation-Request-ID": "baseline-degraded-recovery"}
        baseline = await client.get("/orders/123", headers=headers)

        degraded_policy = await client.put(
            "/_simulator/policy",
            json={
                "expected_revision": 1,
                "seed": 42,
                "routes": [
                    make_route(
                        failure_probability=1.0,
                        failure_status_code=503,
                    ).model_dump(mode="json")
                ],
            },
        )
        degraded = await client.get("/orders/123", headers=headers)

        recovery_policy = await client.put(
            "/_simulator/policy",
            json={
                "expected_revision": 2,
                "seed": 42,
                "routes": [make_route().model_dump(mode="json")],
            },
        )
        recovered = await client.get("/orders/123", headers=headers)

    assert baseline.status_code == 200
    assert degraded_policy.status_code == 200
    assert degraded_policy.json()["revision"] == 2
    assert degraded.status_code == 503
    assert degraded.headers["x-simulation-decision"] == "injected_http_failure"
    assert recovery_policy.status_code == 200
    assert recovery_policy.json()["revision"] == 3
    assert recovered.status_code == 200
    assert recovered.headers["x-gateway-policy-revision"] == "3"
    assert upstream_calls == 2


def test_gateway_metric_dimensions_are_bounded():
    attributes = metric_attributes(
        "CUSTOM-VERB",
        503,
        "untrusted-route-specific-outcome",
    )

    assert attributes == {
        "http.request.method": "_OTHER",
        "http.response.status_code": 503,
        "gateway.simulation.outcome": "server_error",
    }


@pytest.mark.asyncio
async def test_gateway_records_request_counter_and_duration_histogram():
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        views=GATEWAY_METRIC_VIEWS,
    )
    gateway_app.state.gateway_metrics = GatewayMetrics(
        provider.get_meter("app.gateway_simulator")
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway_app),
        base_url="http://gateway.test",
    ) as client:
        forwarded = await client.get("/orders/metric-test")
        unmatched = await client.get("/not-configured")
        health = await client.get("/_simulator/healthz")

    assert forwarded.status_code == 200
    assert unmatched.status_code == 404
    assert health.status_code == 200

    metric_data = reader.get_metrics_data()
    recorded = {
        metric.name: metric.data
        for resource in metric_data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }
    request_points = recorded["gateway.requests"].data_points
    duration_points = recorded["gateway.request.duration"].data_points

    assert sum(point.value for point in request_points) == 2
    assert sum(point.count for point in duration_points) == 2
    assert tuple(duration_points[0].explicit_bounds) == (
        GATEWAY_REQUEST_DURATION_BUCKETS_SECONDS
    )
    assert {
        point.attributes["gateway.simulation.outcome"]
        for point in request_points
    } == {"forwarded", "client_error"}
    assert all(
        "http.request.method" in point.attributes
        and "http.response.status_code" in point.attributes
        for point in request_points
    )
    assert all(
        "correlation_id" not in point.attributes
        and "route_id" not in point.attributes
        for point in request_points
    )
    provider.shutdown()



@pytest.mark.asyncio
async def test_gateway_and_downstream_share_w3c_trace_and_correlated_logs():
    provider = TracerProvider(
        resource=Resource.create({"service.name": "trace-context-test"})
    )
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    previous_propagator = get_global_textmap()
    set_global_textmap(TraceContextTextMapPropagator())
    client_instrumentor = HTTPXClientInstrumentor()
    was_client_instrumented = client_instrumentor.is_instrumented_by_opentelemetry
    if was_client_instrumented:
        client_instrumentor.uninstrument()

    captured: dict[str, str | None] = {}
    downstream = FastAPI()

    @downstream.get("/orders/probe")
    async def downstream_probe(request: Request):
        captured["traceparent"] = request.headers.get("traceparent")
        captured["tracestate"] = request.headers.get("tracestate")
        captured["correlation_id"] = observability.correlation_id_context.get()
        logging.getLogger("test.downstream").info("downstream.probe")
        return {"ok": True}

    downstream.add_middleware(observability.CorrelationIdMiddleware)
    FastAPIInstrumentor.instrument_app(downstream, tracer_provider=provider)

    gateway = FastAPI()
    gateway.include_router(gateway_app.router)
    gateway.add_middleware(observability.CorrelationIdMiddleware)
    FastAPIInstrumentor.instrument_app(gateway, tracer_provider=provider)

    gateway.state.policy_store = PolicyStore(make_policy())
    gateway.state.gateway_runtime = GatewayRuntime()
    gateway.state.http_client = httpx.AsyncClient(
        base_url="http://order.test",
        transport=httpx.ASGITransport(app=downstream),
    )
    client_instrumentor.instrument_client(
        gateway.state.http_client,
        tracer_provider=provider,
    )

    trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    caller_span_id = "00f067aa0ba902b7"
    incoming_traceparent = f"00-{trace_id}-{caller_span_id}-01"
    incoming_tracestate = "vendor=value"
    correlation_id = "550e8400-e29b-41d4-a716-446655440011"
    log_stream = io.StringIO()
    log_handler = logging.StreamHandler(log_stream)
    log_handler.setFormatter(observability.JsonFormatter("trace-context-test"))
    root_logger = logging.getLogger()
    previous_log_level = root_logger.level
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(log_handler)

    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=gateway),
            base_url="http://gateway.test",
        ) as client:
            response = await client.get(
                "/orders/probe",
                headers={
                    "traceparent": incoming_traceparent,
                    "tracestate": incoming_tracestate,
                    "X-Correlation-ID": correlation_id,
                },
            )

        assert response.status_code == 200
        assert response.headers["x-correlation-id"] == correlation_id
        assert captured["correlation_id"] == correlation_id
        assert captured["tracestate"] == incoming_tracestate
        assert captured["traceparent"] is not None

        spans = exporter.get_finished_spans()
        gateway_server = next(
            span
            for span in spans
            if span.kind is SpanKind.SERVER
            and span.attributes.get("http.route") == "/{path:path}"
        )
        downstream_server = next(
            span
            for span in spans
            if span.kind is SpanKind.SERVER
            and span.attributes.get("http.route") == "/orders/probe"
        )
        gateway_client = next(
            span
            for span in spans
            if span.kind is SpanKind.CLIENT
            and span.parent is not None
            and span.parent.span_id == gateway_server.context.span_id
        )

        expected_trace_id = f"{int(trace_id, 16):032x}"
        expected_caller_span_id = int(caller_span_id, 16)
        assert gateway_server.context.trace_id == int(trace_id, 16)
        assert gateway_server.parent is not None
        assert gateway_server.parent.is_remote
        assert gateway_server.parent.span_id == expected_caller_span_id
        assert downstream_server.context.trace_id == gateway_server.context.trace_id
        assert gateway_client.context.trace_id == gateway_server.context.trace_id
        assert gateway_client.parent is not None
        assert gateway_client.parent.span_id == gateway_server.context.span_id
        assert downstream_server.parent is not None
        assert downstream_server.parent.span_id == gateway_client.context.span_id

        traceparent_parts = captured["traceparent"].split("-")
        assert traceparent_parts[1] == expected_trace_id
        assert traceparent_parts[2] == f"{gateway_client.context.span_id:016x}"

        logs = [
            json.loads(line)
            for line in log_stream.getvalue().splitlines()
            if line.strip()
        ]
        gateway_log = next(
            item for item in logs if item["message"] == "gateway.request.forwarded"
        )
        downstream_log = next(
            item for item in logs if item["message"] == "downstream.probe"
        )
        assert gateway_log["correlation_id"] == correlation_id
        assert gateway_log["trace_id"] == expected_trace_id
        assert gateway_log["span_id"] == f"{gateway_server.context.span_id:016x}"
        assert downstream_log["correlation_id"] == correlation_id
        assert downstream_log["trace_id"] == expected_trace_id
        assert downstream_log["span_id"] == f"{downstream_server.context.span_id:016x}"
    finally:
        root_logger.removeHandler(log_handler)
        root_logger.setLevel(previous_log_level)
        await gateway.state.http_client.aclose()
        client_instrumentor.uninstrument_client(gateway.state.http_client)
        FastAPIInstrumentor.uninstrument_app(gateway)
        FastAPIInstrumentor.uninstrument_app(downstream)
        if was_client_instrumented:
            client_instrumentor.instrument()
        set_global_textmap(previous_propagator)
        provider.shutdown()
