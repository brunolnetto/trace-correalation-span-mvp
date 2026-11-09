# API Authentication and Authorization Design

Issue: [#27](https://github.com/brunolnetto/trace-correalation-span-mvp/issues/27)

## Decision

Use standard OpenID Connect access tokens and OAuth 2.0 client credentials. Do not introduce an application-specific token format or use a shared development bypass in Compose.

- **Local identity provider:** Keycloak in the development Compose stack, with a dedicated development realm.
- **Non-local deployments:** Any standards-compliant OIDC provider may be configured through issuer, JWKS, audience, and token-endpoint settings. The application must not depend on Keycloak-specific claims.
- **API verification:** Use the OAuth 2.0 token-introspection endpoint over HTTPS. The identity provider validates token signature, signing-key state, and token activity. Each API independently validates the returned issuer, audience, expiry, not-before, issued-at, subject, and scope claims. Do not accept claims supplied by the caller or decode an unverified JWT locally.
- **Human identity:** User access tokens carry explicit API scopes.
- **Machine identity:** Each calling service obtains its own access token using OAuth 2.0 client credentials. A user's token is never forwarded to another service.
- **Secrets:** Client secrets are injected from an ignored local environment file or a deployment secret manager. They must not be committed, copied into application images, or embedded in realm exports.

The development realm is deliberately a local test IdP, not a production identity configuration. Its test users and client credentials must be clearly marked as development-only. Production startup must reject insecure development settings.

## Scope vocabulary

Use OAuth 2.0 `scope` values as the portable authorization contract. Do not derive permissions from usernames, email addresses, or a caller-supplied service name.

| Scope | Permission |
|---|---|
| `catalog:read` | List and inspect products |
| `catalog:write` | Create and update products |
| `orders:read` | Read order state |
| `orders:process` | Start or resume order processing |
| `inventory:read` | Read stock and movement history |
| `inventory:reserve` | Create or release a reservation |
| `inventory:fulfill` | Commit a reservation |
| `inventory:receive` | Receive stock |
| `inventory:adjust` | Apply an inventory adjustment |
| `suppliers:read` | Read supplier offerings, policies, and purchase orders |
| `suppliers:manage` | Onboard products and manage policies and purchase orders |
| `suppliers:evaluate` | Evaluate replenishment |
| `simulation:control` | Read or advance the simulator clock |
| `events:ingest` | Publish business events to the Event Collector |
| `events:read` | Read events, quarantine records, and collector statistics |
| `operations:read` | Read operational outbox state and recovery projections |

A token may carry several scopes, but each route declares only the minimum scope it needs. Missing credentials return 401; valid credentials without the required scope return 403.

## Route policy

| API surface | Required scope |
|---|---|
| `GET /products`, `GET /products/{id}` | `catalog:read` |
| `POST /products`, `PATCH /products/{id}` | `catalog:write` |
| `GET /inventory/{id}`, `GET /inventory/{id}/movements` | `inventory:read` |
| `POST /inventory/{id}/reservations`, `DELETE /inventory/reservations/{id}` | `inventory:reserve` |
| `POST /inventory/reservations/{id}/commit` | `inventory:fulfill` |
| `POST /inventory/{id}/receipts` | `inventory:receive` |
| `POST /inventory/{id}/adjustments` | `inventory:adjust` |
| `GET /orders/{id}` | `orders:read` |
| `POST /orders/{id}/process` | `orders:process` |
| `GET /orders/{id}/recovery` | `operations:read` |
| Supplier catalog, policies, and purchase-order reads | `suppliers:read` |
| Supplier onboarding and purchase-order mutations | `suppliers:manage` |
| `POST /replenishments/evaluate` | `suppliers:evaluate` |
| `GET /clock`, `POST /clock/advance` | `simulation:control` |
| `POST /v1/events` | `events:ingest` |
| Event, quarantine, and stats reads | `events:read` |
| Outbox stats and blocked-event reads | `operations:read` |

Health/readiness endpoints remain unauthenticated only when they return a minimal status and do not expose configuration, credentials, topology, database details, or operational records.

## Service identities and trust boundaries

The initial machine identities are:

| Caller | Downstream permission |
|---|---|
| `order-api` | `catalog:read inventory:read inventory:reserve inventory:fulfill events:ingest` |
| `inventory-api` | `events:ingest` |
| `supplier-simulator` | `catalog:read catalog:write inventory:read inventory:receive events:ingest` |
| `outbox-dispatcher` | `events:ingest` |
| `order-generator` | `orders:process` |
| `standalone-runner` | The minimum order, inventory, supplier, and event scopes required by its configured workflow |

The caller obtains a token for its own identity, requests the minimum downstream scopes, caches it only until shortly before expiry, and sends it as a Bearer token. Each downstream API independently validates that token and its scopes. The identity provider, not an inbound `X-Service-Name` header, establishes the caller identity.

The Event Collector is a separate trust boundary: ingestion is for machine identities; querying stored events and quarantine data requires an explicit read permission. The outbox dispatcher does not receive broader Event Collector read permissions.

## Configuration and safe defaults

The implementation will expose configuration for:

- OIDC issuer, token endpoint, and token-introspection endpoint;
- API audience;
- bounded introspection cache TTL (default 10 seconds, maximum 30 seconds);
- token endpoint for client-credentials grants;
- service client ID, secret, and requested scopes;
- bounded HTTP timeouts and token-cache safety margin.

OIDC mode is the only application mode outside pytest. Missing issuer, audience, introspection endpoint, token endpoint, or client credentials fail closed. Production endpoints must use HTTPS. Introspection failures return 503 rather than silently accepting a token. A positive result is cached for at most 10 seconds by default (30 seconds maximum), which bounds the revocation propagation delay.

## Test strategy

Tests must cover:

1. Missing Authorization header, malformed Bearer header, malformed token, and inactive/invalid-signature token as reported by the IdP.
2. Wrong issuer, wrong audience, expired token, future `nbf`/`iat`, and missing subject or scope claims.
3. Valid token with required scope and valid token without required scope.
4. Role/scope boundaries across catalog, orders, inventory, suppliers, Event Collector, and operations endpoints.
5. Separate user and service identities; a service token must be accepted only for permissions it was granted.
6. OAuth client-credentials token acquisition, cache expiry/refresh, failed token endpoint responses, and downstream Authorization propagation.
7. Local Compose user and service flows against the development OIDC realm.
8. OpenAPI security declarations and minimal public health responses.

The local reproduction acquires a broad, development-only `local-developer` client-credentials token before calling protected APIs. This is a local operator identity, not a human user login. Non-local human clients should use the provider's Authorization Code flow; internal service-to-service calls must use independent client-credentials identities.

## Non-goals

- Building a custom identity provider or token format.
- Storing user passwords, role assignments, or refresh tokens in this application.
- Exposing Keycloak's administration endpoint as part of the application API.
- Treating a successful local identity-provider test as evidence of production identity-provider configuration or security review.
