from __future__ import annotations

import argparse
import sys
import time
from uuid import uuid4

import httpx


def check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def find_gateway_log(
    client: httpx.Client,
    *,
    correlation_id: str,
    started_ns: int,
    timeout_seconds: float,
) -> tuple[int, list[str]]:
    # These are Loki structured-metadata filters, not index-label selectors.
    query = (
        '{service_name="gateway-simulator"}'
        f' | correlation_id="{correlation_id}"'
        ' | trace_id!=""'
        ' | span_id!=""'
        ' | route_id="inventory"'
        ' | decision="forwarded"'
    )
    deadline = time.monotonic() + timeout_seconds
    last_error: str | None = None

    while time.monotonic() < deadline:
        end_ns = time.time_ns()
        response = client.get(
            "/loki/api/v1/query_range",
            params={
                "query": query,
                "start": str(started_ns),
                "end": str(end_ns),
                "limit": "100",
                "direction": "backward",
            },
        )
        response.raise_for_status()
        payload = response.json()
        check(
            payload.get("status") == "success",
            f"Loki query returned unexpected payload: {payload}",
        )

        streams = payload.get("data", {}).get("result", [])
        entries = [
            (stream, value)
            for stream in streams
            for value in stream.get("values", [])
        ]
        matching_lines = [
            str(value[1])
            for _, value in entries
            if len(value) >= 2 and "gateway.request.forwarded" in str(value[1])
        ]
        if matching_lines:
            return len(entries), matching_lines

        last_error = (
            f"query returned {len(entries)} entries but no "
            "gateway.request.forwarded record"
        )
        time.sleep(1)

    raise RuntimeError(
        f"Timed out waiting for searchable gateway log in Loki. "
        f"LogQL: {query}. Last result: {last_error or 'no result'}"
    )


def run(
    gateway_url: str,
    loki_url: str,
    timeout_seconds: float,
) -> None:
    correlation_id = str(uuid4())
    request_id = f"loki-gateway-verification-{uuid4()}"

    with (
        httpx.Client(
            base_url=gateway_url,
            timeout=httpx.Timeout(10.0, connect=3.0),
        ) as gateway,
        httpx.Client(
            base_url=loki_url,
            timeout=httpx.Timeout(5.0, connect=2.0),
        ) as loki,
    ):
        health = gateway.get("/_simulator/healthz")
        health.raise_for_status()
        check(health.json().get("status") == "ok", "Gateway health check failed")

        loki_ready = loki.get("/ready")
        loki_ready.raise_for_status()

        started_ns = time.time_ns()
        response = gateway.get(
            "/inventory/7",
            headers={
                "X-Correlation-ID": correlation_id,
                "X-Simulation-Request-ID": request_id,
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
            response.headers.get("x-correlation-id") == correlation_id,
            "Gateway response did not preserve the supplied correlation ID",
        )
        policy_revision = response.headers.get("x-gateway-policy-revision")
        check(policy_revision is not None, "Missing gateway policy revision")

        count, lines = find_gateway_log(
            loki,
            correlation_id=correlation_id,
            started_ns=started_ns,
            timeout_seconds=timeout_seconds,
        )

    print(
        "gateway request: "
        f"HTTP {response.status_code}, "
        f"decision=forwarded, policy_revision={policy_revision}"
    )
    print(f"correlation_id: {correlation_id}")
    print(f"simulation_request_id: {request_id}")
    print(
        "Loki search: matched gateway-simulator log by correlation_id, "
        "non-empty trace_id/span_id, route_id, and decision"
    )
    print(f"Loki returned {count} matching log entries")
    print(f"record: {lines[0]}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify that a live gateway request is searchable in Loki by "
            "correlation ID and OpenTelemetry trace/span context."
        )
    )
    parser.add_argument(
        "--gateway-url",
        default="http://127.0.0.1:18004",
        help="Gateway host URL (default: http://127.0.0.1:18004)",
    )
    parser.add_argument(
        "--loki-url",
        default="http://127.0.0.1:13100",
        help="Loki host URL (default: http://127.0.0.1:13100)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=45.0,
        help="Maximum time to wait for OTLP ingestion (default: 45)",
    )
    args = parser.parse_args()

    try:
        check(args.timeout_seconds > 0, "--timeout-seconds must be positive")
        run(
            args.gateway_url.rstrip("/"),
            args.loki_url.rstrip("/"),
            args.timeout_seconds,
        )
    except (httpx.HTTPError, RuntimeError, ValueError, KeyError) as exc:
        print(f"Gateway Loki verification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
