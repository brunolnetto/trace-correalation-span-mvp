from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from uuid import uuid4

import httpx


def check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def get_policy(client: httpx.Client) -> dict[str, Any]:
    response = client.get("/_simulator/policy")
    response.raise_for_status()
    return response.json()


def replace_route(
    client: httpx.Client,
    route_id: str,
    changes: dict[str, Any],
) -> int:
    policy = get_policy(client)
    routes = policy["routes"]
    matched = [route for route in routes if route["route_id"] == route_id]
    check(len(matched) == 1, f"Expected exactly one route named {route_id!r}")
    matched[0].update(changes)

    response = client.put(
        "/_simulator/policy",
        json={
            "expected_revision": policy["revision"],
            "seed": policy["seed"],
            "routes": routes,
        },
    )
    response.raise_for_status()
    updated = response.json()
    expected_revision = policy["revision"] + 1
    check(
        updated["revision"] == expected_revision,
        f"Expected policy revision {expected_revision}, got {updated['revision']}",
    )
    return updated["revision"]


def request_inventory(client: httpx.Client, scenario: str) -> httpx.Response:
    return client.get(
        "/inventory/7",
        headers={
            "X-Correlation-ID": str(uuid4()),
            "X-Simulation-Request-ID": f"live-gateway-{scenario}",
        },
    )


def expect_status(
    response: httpx.Response,
    *,
    status: int,
    decision: str,
    scenario: str,
) -> None:
    check(
        response.status_code == status,
        f"{scenario}: expected HTTP {status}, got {response.status_code}: "
        f"{response.text[:500]}",
    )
    actual_decision = response.headers.get("x-simulation-decision")
    check(
        actual_decision == decision,
        f"{scenario}: expected decision {decision!r}, got {actual_decision!r}",
    )
    revision = response.headers.get("x-gateway-policy-revision")
    check(revision is not None, f"{scenario}: missing gateway policy revision")
    print(
        f"{scenario}: HTTP {response.status_code}, "
        f"decision={actual_decision}, policy_revision={revision}"
    )


def run(base_url: str) -> None:
    timeout = httpx.Timeout(10.0, connect=3.0)
    with httpx.Client(base_url=base_url, timeout=timeout) as client:
        health = client.get("/_simulator/healthz")
        health.raise_for_status()
        check(health.json().get("status") == "ok", "Gateway health check failed")

        initial_policy = get_policy(client)
        initial_routes = initial_policy["routes"]
        inventory_routes = [
            route for route in initial_routes if route["route_id"] == "inventory"
        ]
        check(
            len(inventory_routes) == 1,
            "The active policy must contain exactly one 'inventory' route",
        )
        original_inventory_route = inventory_routes[0].copy()
        policy_changed = False

        try:
            baseline = request_inventory(client, "baseline")
            check(
                200 <= baseline.status_code < 300,
                f"baseline: expected a successful downstream response, got "
                f"{baseline.status_code}: {baseline.text[:500]}",
            )
            expect_status(
                baseline,
                status=baseline.status_code,
                decision="forwarded",
                scenario="baseline forwarding",
            )

            replace_route(
                client,
                "inventory",
                {
                    "requests_per_second": 0.000001,
                    "rate_limit_burst": 1,
                    "failure_probability": 0.0,
                    "failure_mode": "http",
                },
            )
            policy_changed = True
            admitted = request_inventory(client, "rate-limit-admitted")
            rejected = request_inventory(client, "rate-limit-rejected")
            check(
                200 <= admitted.status_code < 300,
                f"rate limit first request should pass, got {admitted.status_code}",
            )
            expect_status(
                rejected,
                status=429,
                decision="rate_limited",
                scenario="rate limiting",
            )

            replace_route(
                client,
                "inventory",
                {
                    "requests_per_second": None,
                    "rate_limit_burst": 1,
                    "failure_probability": 1.0,
                    "failure_mode": "http",
                    "failure_status_code": 503,
                },
            )
            policy_changed = True
            injected = request_inventory(client, "injected-503")
            expect_status(
                injected,
                status=503,
                decision="injected_http_failure",
                scenario="injected upstream failure",
            )

            replace_route(
                client,
                "inventory",
                {
                    "failure_probability": 1.0,
                    "failure_mode": "timeout",
                    "timeout_seconds": 0.05,
                },
            )
            policy_changed = True
            timed_out = request_inventory(client, "injected-timeout")
            expect_status(
                timed_out,
                status=504,
                decision="injected_timeout",
                scenario="injected timeout",
            )
        finally:
            if policy_changed:
                restored_revision = replace_route(
                    client,
                    "inventory",
                    original_inventory_route,
                )
                recovered = request_inventory(client, "recovery")
                check(
                    200 <= recovered.status_code < 300,
                    "recovery: expected successful forwarding after restoring "
                    f"the original inventory policy, got {recovered.status_code}: "
                    f"{recovered.text[:500]}",
                )
                expect_status(
                    recovered,
                    status=recovered.status_code,
                    decision="forwarded",
                    scenario=f"recovery (policy revision {restored_revision})",
                )

        print("All live gateway scenarios passed; original inventory route restored.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise baseline forwarding, rate limiting, injected HTTP failure, "
            "timeout, and policy recovery against a running Compose gateway."
        )
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:18004",
        help="Gateway host URL (default: http://127.0.0.1:18004)",
    )
    args = parser.parse_args()
    try:
        run(args.base_url.rstrip("/"))
    except (httpx.HTTPError, RuntimeError, KeyError, ValueError) as exc:
        print(f"Gateway scenario verification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
