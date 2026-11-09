from __future__ import annotations

from importlib import import_module

import httpx
import pytest

from app import observability
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
observability.configure_observability = lambda app, service_name: None
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
    assert response.headers["x-gateway-route"] == "order-api"
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

from __future__ import annotations

from importlib import import_module

import httpx
import pytest

from app import observability
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
observability.configure_observability = lambda app, service_name: None
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
    assert response.headers["x-gateway-route"] == "order-api"
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
