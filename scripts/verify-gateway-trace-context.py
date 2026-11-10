#!/usr/bin/env python3
"""Verify gateway -> Order API trace continuity in a running Compose stack.

The script sends a request with caller-supplied W3C trace context and a business
correlation ID, then checks Jaeger's stored trace for the gateway server span,
gateway HTTP client span, and Order API server span.
"""

from __future__ import annotations

import argparse
import json
import secrets
import time
import urllib.error
import urllib.request
import uuid
from typing import Any


def get_json(url: str, *, headers: dict[str, str] | None = None) -> dict[str, Any]:
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def span_tags(span: dict[str, Any]) -> dict[str, Any]:
    return {tag["key"]: tag.get("value") for tag in span.get("tags", [])}


def parent_span_ids(span: dict[str, Any]) -> set[str]:
    return {
        reference["spanID"]
        for reference in span.get("references", [])
        if reference.get("refType") == "CHILD_OF"
    }


def find_trace(
    jaeger_url: str,
    trace_id: str,
    correlation_id: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    trace_url = f"{jaeger_url.rstrip('/')}/api/traces/{trace_id}"
    last_problem = "trace has not arrived"

    while time.monotonic() < deadline:
        try:
            payload = get_json(trace_url)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_problem = str(exc)
            time.sleep(1)
            continue

        for trace in payload.get("data", []):
            if trace.get("traceID") != trace_id:
                continue

            processes = trace.get("processes", {})
            spans = trace.get("spans", [])
            services = {
                process.get("serviceName")
                for process in processes.values()
            }
            if not {"gateway-simulator", "order-api"}.issubset(services):
                last_problem = f"trace only contains services: {sorted(services)}"
                continue

            spans_by_service = [
                (
                    span,
                    processes.get(span.get("processID"), {}).get("serviceName"),
                    span_tags(span),
                )
                for span in spans
            ]
            gateway_servers = [
                span
                for span, service, tags in spans_by_service
                if service == "gateway-simulator"
                and tags.get("span.kind") == "server"
                and tags.get("app.correlation_id") == correlation_id
            ]
            order_servers = [
                span
                for span, service, tags in spans_by_service
                if service == "order-api"
                and tags.get("span.kind") == "server"
                and tags.get("app.correlation_id") == correlation_id
            ]
            gateway_clients = [
                span
                for span, service, tags in spans_by_service
                if service == "gateway-simulator"
                and tags.get("span.kind") == "client"
            ]

            for order_server in order_servers:
                for gateway_client in gateway_clients:
                    if gateway_client["spanID"] not in parent_span_ids(order_server):
                        continue
                    if gateway_server_parent := parent_span_ids(gateway_client):
                        for gateway_server in gateway_servers:
                            if gateway_server["spanID"] in gateway_server_parent:
                                return {
                                    "trace": trace,
                                    "gateway_server": gateway_server,
                                    "gateway_client": gateway_client,
                                    "order_server": order_server,
                                    "services": services,
                                }

            last_problem = (
                "trace exists but the gateway -> Order API parent/child chain "
                "or correlation-ID span tags are incomplete"
            )

        time.sleep(1)

    raise RuntimeError(
        f"Jaeger did not record the expected trace within "
        f"{timeout_seconds:.0f}s: {last_problem}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gateway-url",
        default="http://127.0.0.1:18004",
        help="Gateway Simulator base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--jaeger-url",
        default="http://127.0.0.1:16686",
        help="Jaeger Query base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--order-id",
        default="7",
        help="Existing order/inventory ID to read (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help="Maximum seconds to wait for trace export (default: %(default)s)",
    )
    args = parser.parse_args()

    trace_id = secrets.token_hex(16)
    caller_span_id = secrets.token_hex(8)
    correlation_id = str(uuid.uuid4())
    headers = {
        "traceparent": f"00-{trace_id}-{caller_span_id}-01",
        "tracestate": "verification=jaeger-check",
        "X-Correlation-ID": correlation_id,
    }

    request = urllib.request.Request(
        f"{args.gateway_url.rstrip('/')}/orders/{args.order_id}",
        headers=headers,
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response_body = response.read().decode("utf-8")
        if response.status != 200:
            raise RuntimeError(
                f"Gateway request returned HTTP {response.status}: {response_body}"
            )

    result = find_trace(
        args.jaeger_url,
        trace_id,
        correlation_id,
        args.timeout,
    )
    print(f"HTTP request: GET /orders/{args.order_id} -> 200")
    print(f"Trace ID: {trace_id}")
    print(f"Correlation ID: {correlation_id}")
    print(
        "Verified parent/child chain: "
        f"gateway server ({result['gateway_server']['spanID']}) -> "
        f"gateway HTTP client ({result['gateway_client']['spanID']}) -> "
        f"Order API server ({result['order_server']['spanID']})"
    )
    print(f"Jaeger services in trace: {', '.join(sorted(result['services']))}")


if __name__ == "__main__":
    main()
