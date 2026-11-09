from __future__ import annotations

from opentelemetry.metrics import Meter
from opentelemetry.sdk.metrics.view import (
    ExplicitBucketHistogramAggregation,
    View,
)

HTTP_METHODS = frozenset(
    {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE", "CONNECT"}
)
SIMULATION_OUTCOMES = frozenset(
    {
        "forwarded",
        "rate_limited",
        "overloaded",
        "injected_timeout",
        "injected_connection_failure",
        "injected_http_failure",
        "upstream_timeout",
        "upstream_unavailable",
        "success",
        "redirect",
        "client_error",
        "server_error",
    }
)


def metric_attributes(
    method: str, status_code: int, simulation_outcome: str | None
) -> dict[str, str | int]:
    """Return bounded metric dimensions; never include request-specific values."""
    normalized_method = method.upper()
    if normalized_method not in HTTP_METHODS:
        normalized_method = "_OTHER"

    outcome = simulation_outcome or ""
    if outcome not in SIMULATION_OUTCOMES:
        if 200 <= status_code < 300:
            outcome = "success"
        elif 300 <= status_code < 400:
            outcome = "redirect"
        elif 400 <= status_code < 500:
            outcome = "client_error"
        else:
            outcome = "server_error"

    return {
        "http.request.method": normalized_method,
        "http.response.status_code": status_code,
        "gateway.simulation.outcome": outcome,
    }


GATEWAY_REQUEST_DURATION_BUCKETS_SECONDS = (
    0.001,  # 1 ms
    0.005,  # 5 ms
    0.010,  # 10 ms
    0.025,  # 25 ms
    0.050,  # 50 ms
    0.100,  # 100 ms
    0.250,  # 250 ms
    0.500,  # 500 ms
    1.0,
    2.5,
    5.0,
)
GATEWAY_METRIC_VIEWS = (
    View(
        instrument_name="gateway.request.duration",
        meter_name="app.gateway_simulator",
        aggregation=ExplicitBucketHistogramAggregation(
            boundaries=GATEWAY_REQUEST_DURATION_BUCKETS_SECONDS,
        ),
    ),
)


class GatewayMetrics:
    """Low-cardinality request metrics for the gateway data plane."""

    def __init__(self, meter: Meter) -> None:
        self._requests = meter.create_counter(
            "gateway.requests",
            unit="{request}",
            description="Number of requests handled by the gateway data plane.",
        )
        self._duration = meter.create_histogram(
            "gateway.request.duration",
            unit="s",
            description="End-to-end gateway data-plane request duration.",
        )

    def record(
        self,
        *,
        method: str,
        status_code: int,
        simulation_outcome: str | None,
        duration_seconds: float,
    ) -> None:
        attributes = metric_attributes(method, status_code, simulation_outcome)
        self._requests.add(1, attributes)
        self._duration.record(duration_seconds, attributes)
