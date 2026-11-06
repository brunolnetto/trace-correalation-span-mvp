from __future__ import annotations

import atexit
import json
import logging
import os
from contextvars import ContextVar
from datetime import UTC, datetime
from uuid import UUID, uuid4

from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import (
    TracerProvider,
)
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    ParentBased,
    TraceIdRatioBased,
)
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

CORRELATION_ID_HEADER = b"x-correlation-id"
correlation_id_context: ContextVar[str | None] = ContextVar(
    "correlation_id",
    default=None,
)


class CorrelationIdMiddleware:
    """Validate or create a correlation ID and bind it to the request context."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        raw_value = headers.get(CORRELATION_ID_HEADER, b"")

        correlation_id = self._parse_or_generate(raw_value)
        token = correlation_id_context.set(correlation_id)

        active_span = trace.get_current_span()
        if active_span.is_recording():
            active_span.set_attribute("app.correlation_id", correlation_id)

        async def send_with_correlation_id(message):
            if message["type"] == "http.response.start":
                response_headers = list(message.get("headers", []))
                response_headers.append(
                    (CORRELATION_ID_HEADER, correlation_id.encode("ascii"))
                )
                message = {**message, "headers": response_headers}
            await send(message)

        try:
            await self.app(scope, receive, send_with_correlation_id)
        finally:
            correlation_id_context.reset(token)

    @staticmethod
    def _parse_or_generate(raw_value: bytes) -> str:
        if raw_value and len(raw_value) <= 64:
            try:
                return str(UUID(raw_value.decode("ascii")))
            except (UnicodeDecodeError, ValueError, AttributeError):
                pass
        return str(uuid4())


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record with active trace context."""

    def __init__(self, service_name: str):
        super().__init__()
        self.service_name = service_name
        self._standard_fields = set(logging.makeLogRecord({}).__dict__)

    def format(self, record: logging.LogRecord) -> str:
        span_context = trace.get_current_span().get_span_context()
        trace_id = (
            format(span_context.trace_id, "032x")
            if span_context.is_valid
            else None
        )
        span_id = (
            format(span_context.span_id, "016x")
            if span_context.is_valid
            else None
        )

        attributes = {
            key: value
            for key, value in record.__dict__.items()
            if key not in self._standard_fields
            and key not in {"message", "asctime", "service"}
            and not key.startswith("_")
        }

        payload = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": self.service_name,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": correlation_id_context.get(),
            "trace_id": trace_id,
            "span_id": span_id,
        }
        if attributes:
            payload["attributes"] = attributes
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(service_name: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(service_name))

    root_logger = logging.getLogger()
    root_logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    root_logger.handlers.clear()
    root_logger.addHandler(handler)


def configure_tracing(service_name: str) -> TracerProvider:
    sampling_ratio = float(os.getenv("OTEL_TRACES_SAMPLER_ARG", "1.0"))
    if not 0.0 <= sampling_ratio <= 1.0:
        raise ValueError("OTEL_TRACES_SAMPLER_ARG must be between 0.0 and 1.0")

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": os.getenv("SERVICE_VERSION", "0.1.0"),
            "deployment.environment.name": os.getenv(
                "ENVIRONMENT", "development"
            ),
        }
    )
    provider = TracerProvider(
        resource=resource,
        sampler=ParentBased(root=TraceIdRatioBased(sampling_ratio)),
    )
    exporter = OTLPSpanExporter(
        endpoint=os.getenv(
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "http://localhost:4318/v1/traces",
        ),
        timeout=5,
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    set_global_textmap(TraceContextTextMapPropagator())
    atexit.register(provider.shutdown)
    return provider


def configure_observability(app: FastAPI, service_name: str) -> None:
    """Configure JSON logs, W3C tracing, and automatic FastAPI/HTTPX spans."""
    configure_logging(service_name)
    configure_tracing(service_name)

    # Register correlation middleware before instrumentation so the
    # OpenTelemetry server span is active while it executes.
    app.add_middleware(CorrelationIdMiddleware)
    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()
