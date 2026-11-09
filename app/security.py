from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import sys
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Annotated, Any

import httpx
from fastapi import HTTPException, Security
from fastapi.security import (
    OAuth2,
    OAuthFlowAuthorizationCode,
    OAuthFlowClientCredentials,
    OAuthFlows,
    SecurityScopes,
)

logger = logging.getLogger(__name__)

_DEFAULT_LOCAL_ISSUER = "http://localhost:18080/realms/trace-local"
_CONFIGURED_ISSUER = os.getenv("OIDC_ISSUER", _DEFAULT_LOCAL_ISSUER).rstrip("/")
_PUBLIC_TOKEN_URL = os.getenv(
    "OIDC_PUBLIC_TOKEN_URL",
    os.getenv(
        "OIDC_TOKEN_URL",
        f"{_CONFIGURED_ISSUER}/protocol/openid-connect/token",
    ),
)
_AUTHORIZATION_URL = os.getenv(
    "OIDC_AUTHORIZATION_URL",
    f"{_CONFIGURED_ISSUER}/protocol/openid-connect/auth",
)

_SCOPES = {
    "catalog:read": "Read product catalog",
    "catalog:write": "Create and update products",
    "orders:read": "Read order state",
    "orders:process": "Process orders",
    "inventory:read": "Read inventory state",
    "inventory:reserve": "Reserve or release inventory",
    "inventory:fulfill": "Commit an inventory reservation",
    "inventory:receive": "Receive stock",
    "inventory:adjust": "Adjust stock",
    "suppliers:read": "Read supplier and purchase-order state",
    "suppliers:manage": "Manage suppliers, policies, and purchase orders",
    "suppliers:evaluate": "Evaluate replenishment",
    "simulation:control": "Control the simulation clock",
    "events:ingest": "Publish business events",
    "events:read": "Read business events and quarantine",
    "operations:read": "Read operational state",
}

oauth2_scheme = OAuth2(
    flows=OAuthFlows(
        authorizationCode=OAuthFlowAuthorizationCode(
            authorizationUrl=_AUTHORIZATION_URL,
            tokenUrl=_PUBLIC_TOKEN_URL,
            scopes=_SCOPES,
        ),
        clientCredentials=OAuthFlowClientCredentials(
            tokenUrl=_PUBLIC_TOKEN_URL,
            scopes=_SCOPES,
        ),
    ),
    scheme_name="OIDC",
    auto_error=False,
)


@dataclass(frozen=True, slots=True)
class AuthSettings:
    mode: str
    issuer: str
    token_url: str
    introspection_url: str
    audience: str
    client_id: str
    client_secret: str
    service_scopes: frozenset[str]
    timeout_seconds: float
    cache_ttl_seconds: int

    @classmethod
    def from_env(cls) -> AuthSettings:
        mode = os.getenv("AUTH_MODE", "oidc").strip().lower()
        if mode == "test":
            if (
                os.getenv("ENVIRONMENT", "development").lower()
                in {"production", "prod"}
                or (
                    not os.getenv("PYTEST_CURRENT_TEST")
                    and "pytest" not in sys.modules
                )
            ):
                raise RuntimeError(
                    "AUTH_MODE=test is permitted only inside a non-production pytest process"
                )
            return cls(
                mode="test",
                issuer="",
                token_url="",
                introspection_url="",
                audience="",
                client_id="pytest",
                client_secret="",
                service_scopes=frozenset(_SCOPES),
                timeout_seconds=1.0,
                cache_ttl_seconds=1,
            )
        if mode != "oidc":
            raise RuntimeError(
                "AUTH_MODE must be 'oidc'; insecure bypass modes are forbidden"
            )

        required = (
            "OIDC_ISSUER",
            "OIDC_TOKEN_URL",
            "OIDC_INTROSPECTION_URL",
            "OIDC_AUDIENCE",
            "OIDC_CLIENT_ID",
            "OIDC_CLIENT_SECRET",
        )
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise RuntimeError(
                "OIDC authentication is not configured; missing "
                + ", ".join(missing)
            )

        environment = os.getenv("ENVIRONMENT", "development").lower()
        issuer = os.environ["OIDC_ISSUER"].rstrip("/")
        token_url = os.environ["OIDC_TOKEN_URL"]
        introspection_url = os.environ["OIDC_INTROSPECTION_URL"]
        if environment in {"production", "prod"} and not all(
            value.startswith("https://")
            for value in (issuer, token_url, introspection_url)
        ):
            raise RuntimeError("Production OIDC endpoints must use HTTPS")

        timeout = float(os.getenv("OIDC_TIMEOUT_SECONDS", "3"))
        cache_ttl = int(os.getenv("OIDC_INTROSPECTION_CACHE_SECONDS", "10"))
        if not 0.1 <= timeout <= 15:
            raise RuntimeError("OIDC_TIMEOUT_SECONDS must be between 0.1 and 15")
        if not 0 <= cache_ttl <= 30:
            raise RuntimeError(
                "OIDC_INTROSPECTION_CACHE_SECONDS must be between 0 and 30"
            )

        return cls(
            mode=mode,
            issuer=issuer,
            token_url=token_url,
            introspection_url=introspection_url,
            audience=os.environ["OIDC_AUDIENCE"],
            client_id=os.environ["OIDC_CLIENT_ID"],
            client_secret=os.environ["OIDC_CLIENT_SECRET"],
            service_scopes=frozenset(
                os.getenv("OIDC_SERVICE_SCOPES", "").split()
            ),
            timeout_seconds=timeout,
            cache_ttl_seconds=cache_ttl,
        )


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str
    scopes: frozenset[str]
    client_id: str
    issuer: str
    audiences: frozenset[str]


_introspection_cache: OrderedDict[str, tuple[Principal, float]] = OrderedDict()
_introspection_lock = asyncio.Lock()
_async_token_cache: dict[tuple[str, str], tuple[str, float]] = {}
_async_token_lock = asyncio.Lock()
_sync_token_cache: dict[tuple[str, str], tuple[str, float]] = {}
_sync_token_lock = threading.Lock()


def _unauthorized(detail: str = "Invalid or inactive access token") -> HTTPException:
    return HTTPException(
        status_code=401,
        detail=detail,
        headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
    )


async def _request_introspection(
    token: str, settings: AuthSettings
) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(
            timeout=settings.timeout_seconds,
            follow_redirects=False,
        ) as client:
            response = await client.post(
                settings.introspection_url,
                data={"token": token, "token_type_hint": "access_token"},
                auth=(settings.client_id, settings.client_secret),
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("oidc.introspection.unavailable: %s", type(exc).__name__)
        raise HTTPException(
            status_code=503,
            detail="Identity provider is temporarily unavailable",
        ) from None
    if not isinstance(payload, dict):
        raise HTTPException(status_code=503, detail="Invalid identity-provider response")
    return payload


def _audiences(value: Any) -> frozenset[str]:
    if isinstance(value, str):
        return frozenset({value}) if value else frozenset()
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return frozenset(item for item in value if item)
    return frozenset()


def _scopes(value: Any) -> frozenset[str]:
    if isinstance(value, str):
        return frozenset(value.split())
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return frozenset(value)
    return frozenset()


def _principal_from_introspection(
    payload: dict[str, Any], settings: AuthSettings
) -> tuple[Principal, float]:
    if payload.get("active") is not True:
        raise _unauthorized()

    issuer = payload.get("iss")
    subject = payload.get("sub")
    if issuer != settings.issuer or not isinstance(subject, str) or not subject:
        raise _unauthorized("Access token has invalid issuer or subject")

    audiences = _audiences(payload.get("aud"))
    if settings.audience not in audiences:
        raise _unauthorized("Access token has an invalid audience")

    now = time.time()
    try:
        expires_at = float(payload["exp"])
    except (KeyError, TypeError, ValueError):
        raise _unauthorized("Access token has no valid expiry") from None
    if not math.isfinite(expires_at):
        raise _unauthorized("Access token has invalid expiry")
    if expires_at <= now - 5:
        raise _unauthorized("Access token has expired")

    not_before = payload.get("nbf")
    if not_before is not None:
        try:
            parsed_not_before = float(not_before)
            if not math.isfinite(parsed_not_before):
                raise _unauthorized("Access token has invalid not-before claim")
            if parsed_not_before > now + 5:
                raise _unauthorized("Access token is not active yet")
        except (TypeError, ValueError):
            raise _unauthorized("Access token has invalid not-before claim") from None

    issued_at = payload.get("iat")
    if issued_at is not None:
        try:
            parsed_issued_at = float(issued_at)
            if not math.isfinite(parsed_issued_at):
                raise _unauthorized("Access token has invalid issued-at claim")
            if parsed_issued_at > now + 5:
                raise _unauthorized("Access token has an invalid issued-at claim")
        except (TypeError, ValueError):
            raise _unauthorized("Access token has invalid issued-at claim") from None

    scopes = _scopes(payload.get("scope"))
    if not scopes:
        raise _unauthorized("Access token has no authorization scopes")

    client_id = payload.get("client_id", payload.get("azp", ""))
    if not isinstance(client_id, str) or not client_id:
        raise _unauthorized("Access token has no valid client identity")

    principal = Principal(
        subject=subject,
        scopes=scopes,
        client_id=client_id,
        issuer=issuer,
        audiences=audiences,
    )
    cache_ttl = min(
        settings.cache_ttl_seconds,
        max(0, int(expires_at - now - 5)),
    )
    return principal, now + cache_ttl


async def introspect_access_token(token: str) -> Principal:
    if not token or len(token) > 16_384:
        raise _unauthorized()
    settings = AuthSettings.from_env()
    if settings.mode == "test":
        return Principal(
            subject="pytest",
            scopes=frozenset(_SCOPES),
            client_id="pytest",
            issuer="pytest",
            audiences=frozenset({"pytest"}),
        )

    token_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
    now = time.time()
    async with _introspection_lock:
        cached = _introspection_cache.get(token_key)
        if cached is not None:
            principal, cache_until = cached
            if cache_until > now:
                _introspection_cache.move_to_end(token_key)
                return principal
            del _introspection_cache[token_key]

    payload = await _request_introspection(token, settings)
    principal, cache_until = _principal_from_introspection(payload, settings)
    if cache_until > now:
        async with _introspection_lock:
            _introspection_cache[token_key] = (principal, cache_until)
            _introspection_cache.move_to_end(token_key)
            while len(_introspection_cache) > 4096:
                _introspection_cache.popitem(last=False)
    return principal


async def authenticate_request(
    security_scopes: SecurityScopes,
    token: Annotated[str | None, Security(oauth2_scheme)],
) -> Principal:
    settings = AuthSettings.from_env()
    if settings.mode == "test" and not token:
        return Principal(
            subject="pytest",
            scopes=frozenset(_SCOPES),
            client_id="pytest",
            issuer="pytest",
            audiences=frozenset({"pytest"}),
        )
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Bearer access token required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    principal = await introspect_access_token(token)
    missing = set(security_scopes.scopes) - principal.scopes
    if missing:
        required = " ".join(sorted(security_scopes.scopes))
        raise HTTPException(
            status_code=403,
            detail="Insufficient scope",
            headers={
                "WWW-Authenticate": (
                    f'Bearer error="insufficient_scope", scope="{required}"'
                )
            },
        )
    return principal


async def _request_service_token(
    settings: AuthSettings, scopes: frozenset[str]
) -> tuple[str, float]:
    try:
        async with httpx.AsyncClient(
            timeout=settings.timeout_seconds,
            follow_redirects=False,
        ) as client:
            response = await client.post(
                settings.token_url,
                data={
                    "grant_type": "client_credentials",
                    "scope": " ".join(sorted(scopes)),
                },
                auth=(settings.client_id, settings.client_secret),
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.error("oidc.service_token.unavailable: %s", type(exc).__name__)
        raise RuntimeError("Could not obtain service access token") from None

    if not isinstance(payload, dict):
        raise RuntimeError("Identity provider returned an invalid service-token response")
    access_token = payload.get("access_token")
    token_type = payload.get("token_type", "Bearer")
    try:
        expires_in = int(payload.get("expires_in", 0))
    except (TypeError, ValueError):
        expires_in = 0
    if (
        not isinstance(access_token, str)
        or not access_token
        or not isinstance(token_type, str)
        or token_type.lower() != "bearer"
    ):
        raise RuntimeError("Identity provider returned an invalid service token")
    if expires_in <= 0:
        raise RuntimeError(
            "Identity provider returned a service token without a valid expiry"
        )
    return access_token, time.monotonic() + max(0, expires_in - 30)


async def service_authorization_headers(*required_scopes: str) -> dict[str, str]:
    settings = AuthSettings.from_env()
    if settings.mode == "test":
        return {}
    requested = frozenset(required_scopes)
    if not requested or not requested.issubset(settings.service_scopes):
        missing = requested - settings.service_scopes
        raise RuntimeError(
            "Service identity is not configured for required scopes: "
            + " ".join(sorted(missing or requested))
        )
    key = (settings.client_id, " ".join(sorted(requested)))
    now = time.monotonic()
    async with _async_token_lock:
        cached = _async_token_cache.get(key)
        if cached is not None and cached[1] > now:
            return {"Authorization": f"Bearer {cached[0]}"}
        access_token, expires_at = await _request_service_token(settings, requested)
        if expires_at > now:
            _async_token_cache[key] = (access_token, expires_at)
        return {"Authorization": f"Bearer {access_token}"}


def service_authorization_headers_sync(*required_scopes: str) -> dict[str, str]:
    settings = AuthSettings.from_env()
    if settings.mode == "test":
        return {}
    requested = frozenset(required_scopes)
    if not requested or not requested.issubset(settings.service_scopes):
        missing = requested - settings.service_scopes
        raise RuntimeError(
            "Service identity is not configured for required scopes: "
            + " ".join(sorted(missing or requested))
        )
    key = (settings.client_id, " ".join(sorted(requested)))
    now = time.monotonic()
    with _sync_token_lock:
        cached = _sync_token_cache.get(key)
        if cached is not None and cached[1] > now:
            return {"Authorization": f"Bearer {cached[0]}"}
        try:
            with httpx.Client(
                timeout=settings.timeout_seconds,
                follow_redirects=False,
            ) as client:
                response = client.post(
                    settings.token_url,
                    data={
                        "grant_type": "client_credentials",
                        "scope": " ".join(sorted(requested)),
                    },
                    auth=(settings.client_id, settings.client_secret),
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            logger.error("oidc.service_token.unavailable: %s", type(exc).__name__)
            raise RuntimeError("Could not obtain service access token") from None

        if not isinstance(payload, dict):
            raise RuntimeError("Identity provider returned an invalid service-token response")
        access_token = payload.get("access_token")
        token_type = payload.get("token_type", "Bearer")
        try:
            expires_in = int(payload.get("expires_in", 0))
        except (TypeError, ValueError):
            expires_in = 0
        if (
            not isinstance(access_token, str)
            or not access_token
            or not isinstance(token_type, str)
            or token_type.lower() != "bearer"
            or expires_in <= 0
        ):
            raise RuntimeError("Identity provider returned an invalid service token")
        expires_at = now + max(0, expires_in - 30)
        if expires_at > now:
            _sync_token_cache[key] = (access_token, expires_at)
        return {"Authorization": f"Bearer {access_token}"}
