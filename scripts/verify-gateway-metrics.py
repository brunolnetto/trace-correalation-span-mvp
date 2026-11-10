from __future__ import annotations

import argparse
import sys
import time
import uuid

import httpx

METRIC_QUERY = (
    'gateway_requests_total{service_name="gateway-simulator",'
    'gateway_simulation_outcome="forwarded"}'
)


def check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def query_counter(client: httpx.Client) -> tuple[float, float] | None:
    response = client.get("/api/v1/query", params={"query": METRIC_QUERY})
    response.raise_for_status()
    payload = response.json()
    check(
        payload.get("status") == "success",
        f"Prometheus query returned unexpected payload: {payload}",
    )
    results = payload.get("data", {}).get("result", [])
    if not results:
        return None

    sample_timestamps = [float(item["value"][0]) for item in results]
    values = [float(item["value"][1]) for item in results]
    return max(sample_timestamps), sum(values)


def wait_for_sample_after(
    client: httpx.Client,
    *,
    not_before: float,
    timeout_seconds: float,
    greater_than: float | None = None,
) -> float:
    deadline = time.monotonic() + timeout_seconds
    last_sample: tuple[float, float] | None = None

    while time.monotonic() < deadline:
        last_sample = query_counter(client)
        if last_sample is not None:
            sample_timestamp, value = last_sample
            timestamp_is_fresh = sample_timestamp >= not_before
            value_increased = greater_than is None or value > greater_than
            if timestamp_is_fresh and value_increased:
                return value
        time.sleep(1)

    raise RuntimeError(
        "Timed out waiting for a fresh Prometheus gateway counter sample. "
        f"Query: {METRIC_QUERY}. Required sample timestamp >= {not_before}, "
        f"counter > {greater_than}; last_sample={last_sample}. "
        "Check Collector export and Prometheus target health at /targets."
    )


def send_inventory_request(gateway: httpx.Client) -> httpx.Response:
    response = gateway.get(
        "/inventory/7",
        headers={
            "X-Correlation-ID": str(uuid.uuid4()),
            "X-Simulation-Request-ID": f"prometheus-gateway-verification-{uuid.uuid4()}",
        },
    )
    check(
        200 <= response.status_code < 300,
        f"Expected successful Inventory forwarding, got "
        f"HTTP {response.status_code}: {response.text[:500]}",
    )
    check(
        response.headers.get("x-simulation-decision") == "forwarded",
        "Gateway did not report a forwarded decision",
    )
    check(
        response.headers.get("x-correlation-id")
        == response.request.headers.get("x-correlation-id"),
        "Gateway response did not preserve the supplied correlation ID",
    )
    return response


def run(
    gateway_url: str,
    prometheus_url: str,
    timeout_seconds: float,
) -> None:
    with (
        httpx.Client(
            base_url=gateway_url,
            timeout=httpx.Timeout(10.0, connect=3.0),
        ) as gateway,
        httpx.Client(
            base_url=prometheus_url,
            timeout=httpx.Timeout(5.0, connect=2.0),
        ) as prometheus,
    ):
        ready = prometheus.get("/-/ready")
        ready.raise_for_status()

        gateway_health = gateway.get("/_simulator/healthz")
        gateway_health.raise_for_status()
        check(
            gateway_health.json().get("status") == "ok",
            "Gateway health check failed",
        )

        # Prime the counter and wait for a scrape completed after that request.
        # This prevents the first scrape's accumulated value from being
        # mistaken for the measured request's increment.
        warmup_response = send_inventory_request(gateway)
        warmup_completed_at = time.time()
        baseline = wait_for_sample_after(
            prometheus,
            not_before=warmup_completed_at,
            timeout_seconds=timeout_seconds,
        )

        measured_response = send_inventory_request(gateway)
        measured_completed_at = time.time()
        observed = wait_for_sample_after(
            prometheus,
            not_before=measured_completed_at,
            timeout_seconds=timeout_seconds,
            greater_than=baseline,
        )

    print(
        f"warm-up request: HTTP {warmup_response.status_code}, "
        "decision=forwarded"
    )
    print(
        f"measured request: HTTP {measured_response.status_code}, "
        "decision=forwarded"
    )
    print(f"Prometheus query: {METRIC_QUERY}")
    print(f"gateway request counter: {baseline:g} -> {observed:g}")
    print("Prometheus search: observed the measured request counter increment")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify that a live gateway request increments a queryable "
            "Prometheus metric."
        )
    )
    parser.add_argument(
        "--gateway-url",
        default="http://127.0.0.1:18004",
        help="Gateway host URL (default: http://127.0.0.1:18004)",
    )
    parser.add_argument(
        "--prometheus-url",
        default="http://127.0.0.1:19090",
        help="Prometheus host URL (default: http://127.0.0.1:19090)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=75.0,
        help="Maximum time to wait for each scrape (default: 75)",
    )
    args = parser.parse_args()

    try:
        check(args.timeout_seconds > 0, "--timeout-seconds must be positive")
        run(
            args.gateway_url.rstrip("/"),
            args.prometheus_url.rstrip("/"),
            args.timeout_seconds,
        )
    except (httpx.HTTPError, RuntimeError, ValueError, KeyError) as exc:
        print(f"Gateway metrics verification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
