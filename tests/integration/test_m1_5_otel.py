"""M1.5 — Automatic Observability testable criteria.

Uses InMemorySpanExporter to capture and validate OTEL spans.
Console fallback tests use capsys to verify print output.
"""

from __future__ import annotations

import asyncio

import pytest

from civitas.messages import Message, _new_span_id
from civitas.observability.tracer import Tracer

_HAS_OTEL = False
try:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    _HAS_OTEL = True
except ImportError:
    pass


def create_test_tracer() -> tuple[Tracer, InMemorySpanExporter]:
    """Create an OTEL-backed Tracer that records spans in memory (standalone provider)."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    tracer = Tracer.__new__(Tracer)
    tracer._span_queue = None
    tracer._use_otel = True
    tracer._console_fallback = False
    tracer._provider = provider
    tracer._otel_tracer = provider.get_tracer("civitas.test", "0.1.0")
    return tracer, exporter


# ------------------------------------------------------------------
# OTEL span tests (require opentelemetry-sdk)
# ------------------------------------------------------------------


@pytest.mark.skipif(not _HAS_OTEL, reason="opentelemetry-sdk not installed")
async def test_send_receive_spans_have_enrichment():
    """Send/receive spans include civitas.sender, civitas.recipient attributes."""
    tracer, exporter = create_test_tracer()

    msg = Message(
        type="test_msg",
        sender="agent_a",
        recipient="agent_b",
        trace_id="abc123",
        span_id=_new_span_id(),
    )

    send_span = tracer.start_send_span(msg)
    send_span.end()

    recv_span = tracer.start_receive_span(msg)
    recv_span.end()

    spans = exporter.get_finished_spans()
    assert len(spans) == 2

    # Check send span attributes
    send_attrs = dict(spans[0].attributes)
    assert send_attrs["civitas.sender"] == "agent_a"
    assert send_attrs["civitas.recipient"] == "agent_b"
    assert send_attrs["civitas.message_type"] == "test_msg"

    # Check receive span attributes
    recv_attrs = dict(spans[1].attributes)
    assert recv_attrs["civitas.sender"] == "agent_a"
    assert recv_attrs["civitas.recipient"] == "agent_b"


@pytest.mark.skipif(not _HAS_OTEL, reason="opentelemetry-sdk not installed")
async def test_llm_span_has_token_counts():
    """LLM call generates a span with token counts, latency, and cost."""
    tracer, exporter = create_test_tracer()

    span = tracer.start_llm_span("test-model", trace_id="trace1")
    await asyncio.sleep(0.01)  # ensure measurable latency
    tracer.end_llm_span(span, tokens_in=10, tokens_out=20, cost_usd=0.001)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = dict(spans[0].attributes)
    assert attrs["llm.model"] == "test-model"
    assert attrs["llm.tokens_in"] == 10
    assert attrs["llm.tokens_out"] == 20
    assert attrs["llm.cost_usd"] == 0.001
    assert "llm.latency_ms" in attrs
    assert attrs["llm.latency_ms"] >= 5  # at least ~10ms


@pytest.mark.skipif(not _HAS_OTEL, reason="opentelemetry-sdk not installed")
async def test_tool_span_has_result_status():
    """Tool invocation generates a span with tool.result_status."""
    tracer, exporter = create_test_tracer()

    span = tracer.start_tool_span("web_search", trace_id="trace2")
    tracer.end_tool_span(span, status="ok")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1

    attrs = dict(spans[0].attributes)
    assert attrs["tool.name"] == "web_search"
    assert attrs["tool.result_status"] == "ok"
    assert "tool.latency_ms" in attrs


@pytest.mark.skipif(not _HAS_OTEL, reason="opentelemetry-sdk not installed")
async def test_tool_span_error_status():
    """Tool span captures error status."""
    tracer, exporter = create_test_tracer()

    span = tracer.start_tool_span("failing_tool", trace_id="trace3")
    tracer.end_tool_span(span, status="error")

    spans = exporter.get_finished_spans()
    attrs = dict(spans[0].attributes)
    assert attrs["tool.result_status"] == "error"


# ------------------------------------------------------------------
# Console fallback tests (no OTEL required)
# ------------------------------------------------------------------


def _make_console_tracer() -> Tracer:
    """Create a Tracer with console fallback (no OTEL)."""
    tracer = Tracer.__new__(Tracer)
    tracer._span_queue = None
    tracer._use_otel = False
    tracer._otel_tracer = None
    tracer._provider = None
    tracer._console_fallback = True
    return tracer


async def test_console_fallback_send_format(caplog):
    """Console exporter logs 'sender -> recipient: type' for sends."""
    tracer = _make_console_tracer()

    msg = Message(
        type="test_msg",
        sender="agent_a",
        recipient="agent_b",
        trace_id="abc123",
        span_id=_new_span_id(),
    )
    with caplog.at_level("DEBUG", logger="civitas.observability.tracer"):
        span = tracer.start_send_span(msg)
        span.end()

    assert "agent_a -> agent_b: test_msg" in caplog.text


async def test_console_fallback_llm_format(caplog):
    """Console exporter logs LLM summary line."""
    tracer = _make_console_tracer()

    with caplog.at_level("DEBUG", logger="civitas.observability.tracer"):
        span = tracer.start_llm_span("claude-test", trace_id="trace123")
        tracer.end_llm_span(span, tokens_in=100, tokens_out=50, cost_usd=0.005)

    assert "[llm]" in caplog.text
    assert "claude-test" in caplog.text
    assert "100" in caplog.text
    assert "50" in caplog.text
    assert "0.0050" in caplog.text


async def test_console_fallback_tool_format(caplog):
    """Console exporter logs tool summary line."""
    tracer = _make_console_tracer()

    with caplog.at_level("DEBUG", logger="civitas.observability.tracer"):
        span = tracer.start_tool_span("web_search")
        tracer.end_tool_span(span, status="ok")

    assert "[tool]" in caplog.text
    assert "web_search" in caplog.text
    assert "ok" in caplog.text
