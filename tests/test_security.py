from __future__ import annotations

import time
from collections import OrderedDict

import pytest
from fastapi import HTTPException
from fastapi.security import SecurityScopes

import app.security as security


@pytest.fixture
def oidc_environment(monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "oidc")
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.setenv("OIDC_ISSUER", "https://identity.example.test/realms/test")
    monkeypatch.setenv(
        "OIDC_TOKEN_URL",
        "https://identity.example.test/realms/test/protocol/openid-connect/token",
    )
    monkeypatch.setenv(
        "OIDC_INTROSPECTION_URL",
        "https://identity.example.test/realms/test/protocol/openid-connect/token/introspect",
    )
    monkeypatch.setenv("OIDC_AUDIENCE", "inventory-api")
    monkeypatch.setenv("OIDC_CLIENT_ID", "inventory-api")
    monkeypatch.setenv("OIDC_CLIENT_SECRET", "not-a-real-secret")
    monkeypatch.setenv(
        "OIDC_SERVICE_SCOPES",
        "catalog:read inventory:read inventory:reserve events:ingest",
    )
    monkeypatch.setenv("OIDC_INTROSPECTION_CACHE_SECONDS", "10")
    monkeypatch.setattr(security, "_introspection_cache", OrderedDict())
    monkeypatch.setattr(security, "_async_token_cache", {})
    monkeypatch.setattr(security, "_sync_token_cache", {})
    return monkeypatch


def active_payload(**overrides):
    payload = {
        "active": True,
        "iss": "https://identity.example.test/realms/test",
        "sub": "service-account-order-api",
        "aud": ["inventory-api", "event-collector"],
        "scope": "catalog:read inventory:read inventory:reserve",
        "client_id": "order-api",
        "iat": time.time() - 10,
        "exp": time.time() + 300,
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
async def test_introspection_validates_claims_and_caches_short_lived_result(
    oidc_environment, monkeypatch
):
    calls = 0

    async def fake_introspection(token, settings):
        nonlocal calls
        calls += 1
        assert token == "opaque-or-jwt-bearer"
        assert settings.client_id == "inventory-api"
        return active_payload()

    monkeypatch.setattr(security, "_request_introspection", fake_introspection)

    first = await security.introspect_access_token("opaque-or-jwt-bearer")
    second = await security.introspect_access_token("opaque-or-jwt-bearer")

    assert first == second
    assert first.subject == "service-account-order-api"
    assert first.client_id == "order-api"
    assert first.audiences == {"inventory-api", "event-collector"}
    assert {"catalog:read", "inventory:read"} <= first.scopes
    assert calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"active": False},
        active_payload(iss="https://attacker.example.test"),
        active_payload(aud=["order-api"]),
        active_payload(exp=time.time() - 60),
        active_payload(nbf=time.time() + 60),
        active_payload(iat=time.time() + 60),
        active_payload(sub=""),
        active_payload(scope=""),
        active_payload(exp="not-a-timestamp"),
    ],
)
async def test_invalid_introspection_claims_are_rejected(
    oidc_environment, monkeypatch, payload
):
    async def fake_introspection(token, settings):
        return payload

    monkeypatch.setattr(security, "_request_introspection", fake_introspection)

    with pytest.raises(HTTPException) as raised:
        await security.introspect_access_token("invalid-token")

    assert raised.value.status_code == 401


@pytest.mark.asyncio
async def test_missing_bearer_token_returns_401(oidc_environment):
    with pytest.raises(HTTPException) as raised:
        await security.authenticate_request(SecurityScopes(scopes=["inventory:read"]), None)

    assert raised.value.status_code == 401
    assert raised.value.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.asyncio
async def test_valid_identity_without_required_scope_returns_403(
    oidc_environment, monkeypatch
):
    async def fake_introspection(token, settings):
        return active_payload(scope="catalog:read")

    monkeypatch.setattr(security, "_request_introspection", fake_introspection)

    with pytest.raises(HTTPException) as raised:
        await security.authenticate_request(
            SecurityScopes(scopes=["inventory:reserve"]),
            "valid-but-underprivileged",
        )

    assert raised.value.status_code == 403
    assert "insufficient_scope" in raised.value.headers["WWW-Authenticate"]


@pytest.mark.asyncio
async def test_valid_scope_is_accepted(oidc_environment, monkeypatch):
    async def fake_introspection(token, settings):
        return active_payload()

    monkeypatch.setattr(security, "_request_introspection", fake_introspection)

    principal = await security.authenticate_request(
        SecurityScopes(scopes=["inventory:reserve"]),
        "valid-token",
    )

    assert principal.client_id == "order-api"


@pytest.mark.asyncio
async def test_service_client_credentials_are_cached_and_scope_limited(
    oidc_environment, monkeypatch
):
    calls = 0

    async def fake_service_token(settings, scopes):
        nonlocal calls
        calls += 1
        assert scopes == frozenset({"events:ingest"})
        return "service-access-token", time.monotonic() + 60

    monkeypatch.setattr(security, "_request_service_token", fake_service_token)

    first = await security.service_authorization_headers("events:ingest")
    second = await security.service_authorization_headers("events:ingest")

    assert first == {"Authorization": "Bearer service-access-token"}
    assert second == first
    assert calls == 1

    with pytest.raises(RuntimeError, match="not configured for required scopes"):
        await security.service_authorization_headers("events:read")


def test_production_oidc_endpoints_must_use_https(oidc_environment, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("OIDC_ISSUER", "http://identity.example.test/realms/test")

    with pytest.raises(RuntimeError, match="must use HTTPS"):
        security.AuthSettings.from_env()


def test_test_auth_mode_is_unavailable_outside_pytest(oidc_environment, monkeypatch):
    monkeypatch.setenv("AUTH_MODE", "test")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    with pytest.raises(RuntimeError, match="only inside an active pytest run"):
        security.AuthSettings.from_env()


def test_openapi_declares_oauth_flows_and_route_scopes():
    from app.inventory.main import app

    schema = app.openapi()
    oauth = schema["components"]["securitySchemes"]["OIDC"]

    assert "clientCredentials" in oauth["flows"]
    assert "authorizationCode" in oauth["flows"]
    assert "inventory:reserve" in oauth["flows"]["clientCredentials"]["scopes"]
    assert {"OIDC": ["catalog:read"]} in schema["paths"]["/products"]["get"]["security"]
    assert "security" not in schema["paths"]["/healthz"]["get"]
