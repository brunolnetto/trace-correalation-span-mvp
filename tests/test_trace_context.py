from opentelemetry import context, trace
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    TraceState,
)
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)


def test_w3c_trace_context_round_trip():
    expected_trace_id = int("1" * 32, 16)
    expected_span_id = int("2" * 16, 16)
    span_context = SpanContext(
        trace_id=expected_trace_id,
        span_id=expected_span_id,
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
        trace_state=TraceState(),
    )
    token = context.attach(
        trace.set_span_in_context(NonRecordingSpan(span_context))
    )
    carrier = {}
    propagator = TraceContextTextMapPropagator()

    try:
        propagator.inject(carrier)
    finally:
        context.detach(token)

    assert carrier["traceparent"] == (
        f"00-{'1' * 32}-{'2' * 16}-01"
    )

    extracted = propagator.extract(carrier)
    extracted_context = trace.get_current_span(extracted).get_span_context()

    assert extracted_context.trace_id == expected_trace_id
    assert extracted_context.span_id == expected_span_id
    assert extracted_context.is_remote is True
