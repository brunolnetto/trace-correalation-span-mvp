from __future__ import annotations

from opentelemetry.metrics import Meter


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
