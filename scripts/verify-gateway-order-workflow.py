from __future__ import annotations

import argparse
import copy
import sys
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx

ORDER_ROUTE_ID = "order-api"
ORDER_STEPS = (
    "reserve_inventory",
    "charge_payment",
    "confirm_order",
)


def check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def response_json(response: httpx.Response, description: str) -> dict:
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"{description} returned non-JSON content: {response.text[:500]}"
        ) from exc
    check(isinstance(payload, dict), f"{description} returned a non-object JSON value")
    return payload


def make_order_route_policy(
    routes: list[dict],
    *,
    failure_probability: float,
) -> list[dict]:
    updated_routes = copy.deepcopy(routes)
    matching = [
        route for route in updated_routes if route.get("route_id") == ORDER_ROUTE_ID
    ]
    check(
        len(matching) == 1,
        f"Expected exactly one {ORDER_ROUTE_ID!r} route in the gateway policy",
    )

    route = matching[0]
    check(
        route.get("path_prefix") == "/orders",
        "The order-api route must continue to match /orders",
    )
    check(
        "POST" in route.get("methods", ["GET", "POST", "PUT", "PATCH", "DELETE"]),
        "The order-api route must allow POST",
    )
    route["latency_min_ms"] = 0
    route["latency_max_ms"] = 0
    route["failure_probability"] = failure_probability
    route["failure_mode"] = "http"
    route["failure_status_code"] = 503
    route["requests_per_second"] = None
    route["rate_limit_burst"] = 1
    route["max_in_flight"] = None
    route["timeout_seconds"] = 30.0
    return updated_routes


def replace_policy(
    client: httpx.Client,
    *,
    expected_revision: int,
    seed: int,
    routes: list[dict],
) -> dict:
    response = client.put(
        "/_simulator/policy",
        json={
            "expected_revision": expected_revision,
            "seed": seed,
            "routes": routes,
        },
    )
    response.raise_for_status()
    payload = response_json(response, "Gateway policy update")
    check(
        payload.get("revision") == expected_revision + 1,
        "Gateway policy revision did not advance exactly once",
    )
    return payload


def new_order_id(order_api: httpx.Client) -> int:
    for _ in range(10):
        candidate = 100_000_000 + int(uuid4().hex[:8], 16) % 900_000_000
        response = order_api.get(f"/orders/{candidate}/recovery")
        if response.status_code == 404:
            return candidate
        check(
            response.status_code == 200,
            f"Could not check whether order ID {candidate} is unused: "
            f"HTTP {response.status_code}: {response.text[:500]}",
        )
    raise RuntimeError("Could not allocate an unused demo order ID")


def validate_product_availability(
    gateway: httpx.Client,
    *,
    product_id: int,
    quantity: int,
) -> None:
    product_response = gateway.get(f"/products/{product_id}")
    product_response.raise_for_status()
    product = response_json(product_response, "Product preflight")
    check(product.get("is_active") is True, f"Product {product_id} is not active")

    inventory_response = gateway.get(f"/inventory/{product_id}")
    inventory_response.raise_for_status()
    inventory = response_json(inventory_response, "Inventory preflight")
    available = inventory.get("available_quantity")
    check(
        isinstance(available, int) and available >= quantity,
        f"Product {product_id} has {available!r} available units; "
        f"{quantity} required",
    )


def order_headers(label: str) -> dict[str, str]:
    return {
        "X-Correlation-ID": str(uuid4()),
        "X-Simulation-Request-ID": f"order-workflow-{label}-{uuid4()}",
    }


def submit_order(
    gateway: httpx.Client,
    *,
    order_id: int,
    product_id: int,
    quantity: int,
    label: str,
) -> httpx.Response:
    return gateway.post(
        f"/orders/{order_id}/process",
        headers=order_headers(label),
        json={"product_id": product_id, "quantity": quantity},
    )


def validate_gateway_success(
    response: httpx.Response,
    *,
    order_id: int,
    expected_revision: int,
    description: str,
) -> dict:
    check(
        response.status_code == 201,
        f"{description}: expected HTTP 201, got "
        f"{response.status_code}: {response.text[:500]}",
    )
    check(
        response.headers.get("x-simulation-decision") == "forwarded",
        f"{description}: gateway did not report a forwarded request",
    )
    check(
        response.headers.get("x-gateway-route") == ORDER_ROUTE_ID,
        f"{description}: request did not use the order-api gateway route",
    )
    check(
        response.headers.get("x-gateway-policy-revision")
        == str(expected_revision),
        f"{description}: unexpected gateway policy revision",
    )
    payload = response_json(response, description)
    check(payload.get("order_id") == order_id, f"{description}: wrong order ID")
    check(payload.get("status") == "processed", f"{description}: order not processed")
    for field in ("saga_id", "reservation_id", "payment_id"):
        check(payload.get(field), f"{description}: missing {field}")
    expected_payment_id = str(
        uuid5(NAMESPACE_URL, f"payment:{payload['saga_id']}")
    )
    check(
        payload["payment_id"] == expected_payment_id,
        f"{description}: payment ID no longer follows the demo adapter's "
        "deterministic Saga identity",
    )
    return payload


def validate_completed_saga(
    order_api: httpx.Client,
    *,
    order_id: int,
    result: dict,
    description: str,
) -> dict:
    response = order_api.get(f"/orders/{order_id}/recovery")
    check(
        response.status_code == 200,
        f"{description}: recovery endpoint returned HTTP "
        f"{response.status_code}: {response.text[:500]}",
    )
    recovery = response_json(response, f"{description} recovery")
    check(recovery.get("status") == "completed", f"{description}: Saga not completed")
    check(
        recovery.get("saga_id") == result["saga_id"],
        f"{description}: recovery returned a different Saga ID",
    )
    check(
        recovery.get("payment_id") == result["payment_id"],
        f"{description}: payment ID differs from the Order result",
    )
    check(
        recovery.get("reservation_id") == result["reservation_id"],
        f"{description}: reservation ID differs from the Order result",
    )

    steps = recovery.get("steps")
    check(isinstance(steps, list), f"{description}: recovery has no step list")
    step_names = tuple(step.get("step_name") for step in steps)
    check(
        step_names == ORDER_STEPS,
        f"{description}: unexpected Saga action sequence: {step_names}",
    )
    for step in steps:
        check(
            step.get("forward_status") == "completed",
            f"{description}: step {step.get('step_name')} did not complete",
        )
        check(
            step.get("forward_attempts") == 1,
            f"{description}: step {step.get('step_name')} ran more than once",
        )
    return recovery


def verify_blocked_order_has_no_saga(
    order_api: httpx.Client,
    *,
    order_id: int,
) -> None:
    response = order_api.get(f"/orders/{order_id}/recovery")
    check(
        response.status_code == 404,
        "The gateway-blocked request unexpectedly created an Order Saga: "
        f"recovery returned HTTP {response.status_code}: {response.text[:500]}",
    )


def restore_original_policy(
    gateway: httpx.Client,
    *,
    owned_revision: int | None,
    original_seed: int,
    original_routes: list[dict],
) -> int | None:
    if owned_revision is None:
        return None

    current_response = gateway.get("/_simulator/policy")
    current_response.raise_for_status()
    current = response_json(current_response, "Current gateway policy")
    current_revision = current.get("revision")
    check(
        current_revision == owned_revision,
        "Gateway policy changed outside this verifier; refusing to overwrite "
        f"revision {current_revision} (last verifier-owned revision "
        f"{owned_revision}). Restore the intended policy manually.",
    )

    restored = replace_policy(
        gateway,
        expected_revision=owned_revision,
        seed=original_seed,
        routes=original_routes,
    )
    check(
        restored.get("seed") == original_seed
        and restored.get("routes") == original_routes,
        "Gateway policy restoration did not reproduce the original seed/routes",
    )
    return restored["revision"]


def run(
    gateway_url: str,
    order_url: str,
    product_id: int,
    quantity: int,
) -> None:
    with (
        httpx.Client(
            base_url=gateway_url,
            timeout=httpx.Timeout(35.0, connect=3.0),
        ) as gateway,
        httpx.Client(
            base_url=order_url,
            timeout=httpx.Timeout(10.0, connect=3.0),
        ) as order_api,
    ):
        gateway_health = gateway.get("/_simulator/healthz")
        gateway_health.raise_for_status()
        check(
            response_json(gateway_health, "Gateway health").get("status") == "ok",
            "Gateway health check did not report status=ok",
        )
        order_health = order_api.get("/healthz")
        order_health.raise_for_status()
        check(
            response_json(order_health, "Order API health").get("status") == "ok",
            "Order API health check did not report status=ok",
        )

        original_response = gateway.get("/_simulator/policy")
        original_response.raise_for_status()
        original = response_json(original_response, "Original gateway policy")
        original_revision = original.get("revision")
        original_seed = original.get("seed")
        original_routes = original.get("routes")
        check(isinstance(original_revision, int), "Gateway policy has no revision")
        check(isinstance(original_seed, int), "Gateway policy has no scenario seed")
        check(isinstance(original_routes, list), "Gateway policy has no route list")

        validate_product_availability(
            gateway,
            product_id=product_id,
            quantity=quantity,
        )
        baseline_order_id = new_order_id(order_api)
        degraded_order_id = new_order_id(order_api)

        baseline_routes = make_order_route_policy(
            original_routes,
            failure_probability=0.0,
        )
        degraded_routes = make_order_route_policy(
            baseline_routes,
            failure_probability=1.0,
        )

        owned_revision: int | None = None
        restored_revision: int | None = None

        try:
            baseline_policy = replace_policy(
                gateway,
                expected_revision=original_revision,
                seed=original_seed,
                routes=baseline_routes,
            )
            owned_revision = baseline_policy["revision"]
            baseline_result = validate_gateway_success(
                submit_order(
                    gateway,
                    order_id=baseline_order_id,
                    product_id=product_id,
                    quantity=quantity,
                    label="baseline",
                ),
                order_id=baseline_order_id,
                expected_revision=owned_revision,
                description="Baseline workflow",
            )
            validate_completed_saga(
                order_api,
                order_id=baseline_order_id,
                result=baseline_result,
                description="Baseline workflow",
            )

            # Replaying the completed order must not execute a second charge or
            # repeat the Inventory reservation.
            replay_result = validate_gateway_success(
                submit_order(
                    gateway,
                    order_id=baseline_order_id,
                    product_id=product_id,
                    quantity=quantity,
                    label="baseline-replay",
                ),
                order_id=baseline_order_id,
                expected_revision=owned_revision,
                description="Baseline replay",
            )
            check(
                replay_result["saga_id"] == baseline_result["saga_id"]
                and replay_result["payment_id"] == baseline_result["payment_id"]
                and replay_result["reservation_id"]
                == baseline_result["reservation_id"],
                "Baseline replay changed Saga/payment/reservation identity",
            )
            validate_completed_saga(
                order_api,
                order_id=baseline_order_id,
                result=replay_result,
                description="Baseline replay",
            )

            degraded_policy = replace_policy(
                gateway,
                expected_revision=owned_revision,
                seed=original_seed,
                routes=degraded_routes,
            )
            owned_revision = degraded_policy["revision"]
            degraded_response = submit_order(
                gateway,
                order_id=degraded_order_id,
                product_id=product_id,
                quantity=quantity,
                label="degraded",
            )
            check(
                degraded_response.status_code == 503,
                "Degraded workflow: expected the gateway to return HTTP 503, "
                f"got {degraded_response.status_code}: "
                f"{degraded_response.text[:500]}",
            )
            check(
                degraded_response.headers.get("x-simulation-decision")
                == "injected_http_failure",
                "Degraded workflow was not rejected by the injected gateway fault",
            )
            check(
                degraded_response.headers.get("x-gateway-route") == ORDER_ROUTE_ID
                and degraded_response.headers.get("x-gateway-policy-revision")
                == str(owned_revision),
                "Degraded workflow returned unexpected route/policy metadata",
            )
            verify_blocked_order_has_no_saga(
                order_api,
                order_id=degraded_order_id,
            )

        finally:
            restored_revision = restore_original_policy(
                gateway,
                owned_revision=owned_revision,
                original_seed=original_seed,
                original_routes=original_routes,
            )

        check(restored_revision is not None, "Original gateway policy was not restored")
        final_policy_response = gateway.get("/_simulator/policy")
        final_policy_response.raise_for_status()
        final_policy = response_json(final_policy_response, "Restored gateway policy")
        check(
            final_policy.get("revision") == restored_revision
            and final_policy.get("seed") == original_seed
            and final_policy.get("routes") == original_routes,
            "Final gateway policy does not match the original configuration",
        )

    print(
        f"baseline workflow: order={baseline_order_id}, HTTP 201, "
        f"Saga={baseline_result['saga_id']}, payment={baseline_result['payment_id']}"
    )
    print("baseline replay: same Saga, payment, and reservation identities")
    print(
        f"degraded workflow: order={degraded_order_id}, HTTP 503, "
        "decision=injected_http_failure"
    )
    print("degraded workflow state: no Order Saga created; no payment/reservation")
    print("restoration: original policy seed and routes restored")
    print(
        "Saga contract: reserve_inventory -> charge_payment -> confirm_order; "
        "all steps completed once"
    )
    print(
        f"gateway policy: restored original seed/routes at revision {restored_revision}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare a successful Order Saga with a deterministic gateway "
            "ingress rejection before Order execution."
        )
    )
    parser.add_argument(
        "--gateway-url",
        default="http://127.0.0.1:18004",
        help="Gateway host URL (default: http://127.0.0.1:18004)",
    )
    parser.add_argument(
        "--order-url",
        default="http://127.0.0.1:18000",
        help="Direct Order API host URL, used only for recovery inspection",
    )
    parser.add_argument(
        "--product-id",
        type=int,
        default=7,
        help="Existing active product to order (default: 7)",
    )
    parser.add_argument(
        "--quantity",
        type=int,
        default=1,
        help="Quantity for each successful order (default: 1)",
    )
    args = parser.parse_args()

    try:
        check(args.product_id > 0, "--product-id must be positive")
        check(args.quantity > 0, "--quantity must be positive")
        run(
            args.gateway_url.rstrip("/"),
            args.order_url.rstrip("/"),
            args.product_id,
            args.quantity,
        )
    except (httpx.HTTPError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        print(f"Gateway Order workflow verification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
