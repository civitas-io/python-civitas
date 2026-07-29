"""Unit tests for observability primitives: SpanQueue, ExportBackend, OTELAgent, Span, Tracer."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock

import pytest

from civitas.observability.export_backend import ConsoleBackend, FanOutBackend
from civitas.observability.otel_agent import run_otel_agent
from civitas.observability.span_queue import SpanData, SpanQueue
from civitas.observability.tracer import Span, Tracer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _span(name: str = "test.span", status: str = "ok", error: str | None = None) -> SpanData:
    return SpanData(
        name=name,
        trace_id="trace-1",
        span_id="span-1",
        parent_span_id=None,
        start_time=1000.0,
        end_time=1001.5,
        attributes={"key": "val"},
        status=status,
        error_message=error,
    )


class _RecordingBackend:
    """In-memory export backend for testing OTELAgent."""

    def __init__(self, raise_on_export: bool = False) -> None:
        self.exported: list[list[SpanData]] = []
        self.shutdown_called = False
        self.raise_on_export = raise_on_export

    async def export(self, spans: list[SpanData]) -> None:
        if self.raise_on_export:
            raise RuntimeError("export failure")
        self.exported.append(spans)

    async def shutdown(self) -> None:
        self.shutdown_called = True


# ---------------------------------------------------------------------------
# SpanQueue
# ---------------------------------------------------------------------------


async def test_span_queue_put_and_get() -> None:
    """put_nowait() enqueues a span; get() returns it."""
    q = SpanQueue()
    s = _span()
    q.put_nowait(s)
    result = await q.get()
    assert result is s


async def test_span_queue_get_nowait() -> None:
    """get_nowait() returns a span without awaiting."""
    q = SpanQueue()
    s = _span()
    q.put_nowait(s)
    result = q.get_nowait()
    assert result is s


async def test_span_queue_empty_and_qsize() -> None:
    """empty() and qsize() reflect queue state correctly."""
    q = SpanQueue()
    assert q.empty() is True
    assert q.qsize() == 0

    q.put_nowait(_span())
    assert q.empty() is False
    assert q.qsize() == 1

    await q.get()
    assert q.empty() is True
    assert q.qsize() == 0


async def test_span_queue_overflow_drops_oldest() -> None:
    """When queue is full, put_nowait() drops the oldest span and enqueues the new one."""
    q = SpanQueue(maxsize=2)
    first = _span("first")
    second = _span("second")
    third = _span("third")

    q.put_nowait(first)
    q.put_nowait(second)
    # Queue is now full — adding third should drop first
    q.put_nowait(third)

    assert q.qsize() == 2
    got1 = q.get_nowait()
    got2 = q.get_nowait()
    # first was dropped; second and third remain
    assert got1.name == "second"
    assert got2.name == "third"


# ---------------------------------------------------------------------------
# ConsoleBackend
# ---------------------------------------------------------------------------


async def test_console_backend_ok_span(caplog: pytest.LogCaptureFixture) -> None:
    """ConsoleBackend logs an INFO line for a successful span."""
    backend = ConsoleBackend()
    with caplog.at_level(logging.INFO, logger="civitas.observability.export_backend"):
        await backend.export([_span("agent.handle", status="ok")])
    assert any("agent.handle" in r.message and "1500.0ms" in r.message for r in caplog.records)


async def test_console_backend_error_span(caplog: pytest.LogCaptureFixture) -> None:
    """ConsoleBackend logs ERROR details in the span line."""
    backend = ConsoleBackend()
    with caplog.at_level(logging.INFO, logger="civitas.observability.export_backend"):
        await backend.export([_span("agent.handle", status="error", error="boom")])
    assert any("ERROR" in r.message and "boom" in r.message for r in caplog.records)


async def test_console_backend_shutdown() -> None:
    """ConsoleBackend.shutdown() is a no-op — must not raise."""
    await ConsoleBackend().shutdown()


async def test_console_backend_empty_batch() -> None:
    """ConsoleBackend.export() with empty list does not raise."""
    await ConsoleBackend().export([])


# ---------------------------------------------------------------------------
# FanOutBackend
# ---------------------------------------------------------------------------


async def test_fanout_exports_to_all_backends() -> None:
    """FanOutBackend calls export() on every child backend."""
    a, b = _RecordingBackend(), _RecordingBackend()
    fan = FanOutBackend([a, b])
    spans = [_span()]
    await fan.export(spans)
    assert a.exported == [spans]
    assert b.exported == [spans]


async def test_fanout_continues_after_export_error(caplog: pytest.LogCaptureFixture) -> None:
    """If one backend raises during export, FanOutBackend logs and continues to the next."""
    bad = _RecordingBackend(raise_on_export=True)
    good = _RecordingBackend()
    fan = FanOutBackend([bad, good])
    spans = [_span()]
    with caplog.at_level(logging.ERROR, logger="civitas.observability.export_backend"):
        await fan.export(spans)
    assert good.exported == [spans]
    assert any("raised during export" in r.message for r in caplog.records)


async def test_fanout_shutdown_calls_all_backends() -> None:
    """FanOutBackend.shutdown() calls shutdown() on every child backend."""
    a, b = _RecordingBackend(), _RecordingBackend()
    await FanOutBackend([a, b]).shutdown()
    assert a.shutdown_called is True
    assert b.shutdown_called is True


async def test_fanout_shutdown_continues_after_error(caplog: pytest.LogCaptureFixture) -> None:
    """If one backend raises during shutdown, FanOutBackend logs and continues."""

    class _BadShutdown:
        async def export(self, spans: list[SpanData]) -> None:
            pass

        async def shutdown(self) -> None:
            raise RuntimeError("shutdown failure")

    good = _RecordingBackend()
    fan = FanOutBackend([_BadShutdown(), good])
    with caplog.at_level(logging.ERROR, logger="civitas.observability.export_backend"):
        await fan.shutdown()
    assert good.shutdown_called is True
    assert any("raised during shutdown" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# OTELAgent
# ---------------------------------------------------------------------------


async def test_otel_agent_drains_spans() -> None:
    """run_otel_agent exports spans placed on the queue."""
    q = SpanQueue()
    backend = _RecordingBackend()
    for i in range(3):
        q.put_nowait(_span(f"span-{i}"))

    task = asyncio.create_task(run_otel_agent(q, backend, batch_size=10, flush_interval=0.05))
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    exported_flat = [s for batch in backend.exported for s in batch]
    assert len(exported_flat) == 3


async def test_otel_agent_flushes_on_timeout() -> None:
    """run_otel_agent flushes a partial batch after flush_interval."""
    q = SpanQueue()
    backend = _RecordingBackend()
    q.put_nowait(_span("only-one"))

    task = asyncio.create_task(run_otel_agent(q, backend, batch_size=50, flush_interval=0.05))
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    exported_flat = [s for batch in backend.exported for s in batch]
    assert any(s.name == "only-one" for s in exported_flat)


async def test_otel_agent_batch_flush_at_batch_size() -> None:
    """run_otel_agent flushes immediately when batch_size is reached."""
    q = SpanQueue()
    backend = _RecordingBackend()
    batch_size = 5
    for i in range(batch_size):
        q.put_nowait(_span(f"s{i}"))

    task = asyncio.create_task(
        run_otel_agent(q, backend, batch_size=batch_size, flush_interval=10.0)
    )
    # Give event loop time to drain the queue and flush
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    exported_flat = [s for batch in backend.exported for s in batch]
    assert len(exported_flat) >= batch_size


async def test_otel_agent_cancellation_drains_remaining() -> None:
    """On cancellation, run_otel_agent drains leftover spans before calling shutdown."""
    q = SpanQueue()
    backend = _RecordingBackend()

    task = asyncio.create_task(run_otel_agent(q, backend, batch_size=50, flush_interval=60.0))
    # Let the agent start its wait, then enqueue spans and cancel
    await asyncio.sleep(0.02)
    for i in range(4):
        q.put_nowait(_span(f"late-{i}"))

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert backend.shutdown_called is True
    exported_flat = [s for batch in backend.exported for s in batch]
    assert len(exported_flat) == 4


async def test_otel_agent_export_error_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    """If backend.export() raises, the error is logged and the loop continues."""
    q = SpanQueue()
    backend = _RecordingBackend(raise_on_export=True)
    q.put_nowait(_span())

    task = asyncio.create_task(run_otel_agent(q, backend, batch_size=10, flush_interval=0.05))
    with caplog.at_level(logging.ERROR, logger="civitas.observability.otel_agent"):
        await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert any("export failed" in r.message for r in caplog.records)


async def test_otel_agent_shutdown_called_on_cancel() -> None:
    """backend.shutdown() is always called when run_otel_agent is cancelled."""
    q = SpanQueue()
    backend = _RecordingBackend()

    task = asyncio.create_task(run_otel_agent(q, backend, batch_size=10, flush_interval=60.0))
    await asyncio.sleep(0.02)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert backend.shutdown_called is True


# ---------------------------------------------------------------------------
# Span — idempotent end, push to queue, set_error with otel span
# ---------------------------------------------------------------------------


def test_span_end_is_idempotent() -> None:
    """Calling span.end() twice does not change end_time or push twice (line 71)."""
    q = SpanQueue()
    span = Span("test", "t1", "s1", _span_queue=q)
    span.end()
    first_end = span.end_time
    span.end()
    assert span.end_time == first_end
    assert q.qsize() == 1  # only one push despite two end() calls


def test_span_end_pushes_to_queue() -> None:
    """Ending a span with a queue attached enqueues a SpanData (lines 77, 81-85)."""
    q = SpanQueue()
    span = Span("push.me", "t1", "s1", _span_queue=q)
    assert q.empty()
    span.end()
    assert q.qsize() == 1
    got = q.get_nowait()
    assert got.name == "push.me"
    assert got.status == "ok"


def test_span_end_pushes_error_status_to_queue() -> None:
    """Error spans are pushed with status='error' and error_message set."""
    q = SpanQueue()
    span = Span("fail.span", "t1", "s1", _span_queue=q)
    span.set_error(RuntimeError("boom"))
    span.end()
    got = q.get_nowait()
    assert got.status == "error"
    assert got.error_message == "boom"


def test_span_set_error_with_otel_span() -> None:
    """set_error() calls record_exception on the underlying OTEL span (line 65->exit)."""
    span = Span("test", "t1", "s1")
    mock_otel_span = MagicMock()
    span._otel_span = mock_otel_span

    span.set_error(ValueError("oops"))

    mock_otel_span.record_exception.assert_called_once()


# ---------------------------------------------------------------------------
# Tracer — flush, flush no-op when OTEL inactive
# ---------------------------------------------------------------------------


def test_tracer_flush_calls_provider_force_flush() -> None:
    """Tracer.flush() delegates to provider.force_flush() when OTEL is active (line 333)."""
    tracer = Tracer()
    if not tracer._use_otel:
        pytest.skip("opentelemetry-sdk not installed")

    mock_provider = MagicMock()
    tracer._provider = mock_provider
    tracer.flush()
    mock_provider.force_flush.assert_called_once()


def test_tracer_flush_noop_without_otel() -> None:
    """Tracer.flush() is a no-op when _use_otel is False (branch 337->exit)."""
    tracer = Tracer()
    tracer._use_otel = False
    tracer._provider = None
    tracer.flush()  # must not raise


def test_tracer_new_span_id_returns_hex_string() -> None:
    """Tracer.new_span_id() returns a non-empty hex string (line 333)."""
    tracer = Tracer()
    sid = tracer.new_span_id()
    assert isinstance(sid, str)
    assert len(sid) > 0


# ---------------------------------------------------------------------------
# Tracer -- OTEL parent-context linkage (v0.9.3, A1)
#
# Confirmed LIVE (not assumed) before this fix: Tracer._make_span() called
# self._otel_tracer.start_span(name, attributes=...) with no `context=` at
# all -- every real OTEL span became its own isolated root trace with
# `parent_id: null`, regardless of civitas's own correct trace_id/span_id/
# parent_span_id bookkeeping on Span/Message. A real Jaeger/Grafana view
# (docs/observability.md Mode 3) showed every send/recv/llm/tool span as a
# disconnected single-span "trace" -- no tree, no request-flow view, the one
# thing distributed tracing exists to do. Confirmed fixed with a real
# 2-OS-process ZMQ round trip (ask() -> frontend -> worker_a/worker_b over a
# real ZMQ proxy) during development; these tests pin the underlying
# mechanism so a regression is caught at unit-test speed.
# ---------------------------------------------------------------------------


def test_otel_span_links_to_real_parent_context() -> None:
    """A span created with a valid parent_span_id becomes a real OTEL CHILD
    span -- same trace_id, correct parent_id -- not an isolated root."""
    tracer = Tracer()
    if not tracer._use_otel:
        pytest.skip("opentelemetry-sdk not installed")

    root = tracer.start_span("root", trace_id="a" * 32)
    root.end()
    child = tracer.start_span("child", trace_id=root.trace_id, parent_span_id=root.span_id)
    child.end()

    assert child._otel_span is not None
    child_ctx = child._otel_span.get_span_context()
    assert child._otel_span.parent is not None
    assert child._otel_span.parent.trace_id == child_ctx.trace_id
    # v0.9.3 (A1): root's caller-given trace_id ('aaa...') is itself an OTEL
    # ROOT span (no parent) so OTEL mints its own trace_id for it (a
    # documented, accepted limitation -- see _otel_parent_context()'s
    # docstring). What matters here: the CHILD correctly landed in the SAME
    # OTEL trace as root's REAL (not the originally-requested) trace_id, and
    # points at root's REAL span_id as its parent -- proving the linkage
    # mechanism itself, independent of that separate, accepted limitation.
    assert child.trace_id == root.trace_id
    assert format(child_ctx.trace_id, "032x") == root.trace_id
    assert child._otel_span.parent.span_id == int(root.span_id, 16)


def test_otel_span_without_parent_is_a_real_root() -> None:
    """No parent_span_id -> a genuine OTEL root span (parent is None), not
    an error -- this is the expected shape for the first message of a flow."""
    tracer = Tracer()
    if not tracer._use_otel:
        pytest.skip("opentelemetry-sdk not installed")

    span = tracer.start_span("root-only")
    span.end()
    assert span._otel_span is not None
    assert span._otel_span.parent is None


def test_otel_span_malformed_parent_id_falls_back_to_root_without_raising() -> None:
    """A non-hex trace_id/parent_span_id (e.g. a hand-built test Message)
    must never crash send()/receive() -- fall back to a fresh OTEL root."""
    tracer = Tracer()
    if not tracer._use_otel:
        pytest.skip("opentelemetry-sdk not installed")

    span = tracer.start_span("weird", trace_id="not-hex-at-all", parent_span_id="also-not-hex")
    span.end()  # must not raise
    assert span._otel_span is not None
    assert span._otel_span.parent is None


def test_span_trace_and_span_id_become_otel_authoritative_when_otel_active() -> None:
    """When OTEL is active, Span.trace_id/span_id are overwritten with
    OTEL's OWN real, minted IDs (MessageBus.route()/request() then copy
    these back onto the outgoing Message -- see test_bus.py) -- otherwise a
    downstream hop's parent reference would point at an ID OTEL never
    actually emitted, silently breaking the very linkage this fix restores.
    """
    tracer = Tracer()
    if not tracer._use_otel:
        pytest.skip("opentelemetry-sdk not installed")

    span = tracer.start_span("x", trace_id="b" * 32)
    real_ctx = span._otel_span.get_span_context()
    assert span.trace_id == format(real_ctx.trace_id, "032x")
    assert span.span_id == format(real_ctx.span_id, "016x")


# ---------------------------------------------------------------------------
# Tracer — span_queue makes Path A/Path B mutually exclusive (FD-09)
# ---------------------------------------------------------------------------


def test_tracer_with_span_queue_skips_direct_provider() -> None:
    """Providing span_queue skips the direct TracerProvider path entirely."""
    queue = SpanQueue()
    tracer = Tracer(span_queue=queue)
    assert tracer._use_otel is False
    assert tracer._provider is None
    assert tracer._otel_tracer is None
    assert tracer._console_fallback is False


def test_tracer_without_span_queue_unchanged() -> None:
    """Providing no span_queue behaves exactly as before (default path)."""
    tracer = Tracer()
    assert tracer._span_queue is None
    # _console_fallback / _use_otel depend on whether opentelemetry-sdk is
    # installed in this environment — just confirm the queue path was skipped.
    assert tracer._use_otel or tracer._console_fallback


def test_tracer_with_span_queue_pushes_completed_spans() -> None:
    """Spans still flow to the queue when the direct provider path is skipped."""
    queue = SpanQueue()
    tracer = Tracer(span_queue=queue)
    span = tracer.start_span("civitas.test", trace_id="t1")
    span.end()
    assert queue.qsize() == 1


# ---------------------------------------------------------------------------
# Tracer — console fallback log output
# ---------------------------------------------------------------------------


def test_end_llm_span_console_fallback_logs(caplog: pytest.LogCaptureFixture) -> None:
    """end_llm_span() emits a debug log line when _console_fallback is True."""
    tracer = Tracer()
    tracer._console_fallback = True
    span = tracer.start_llm_span("gpt-4", trace_id="trace-llm")
    with caplog.at_level(logging.DEBUG, logger="civitas.observability.tracer"):
        tracer.end_llm_span(span, tokens_in=10, tokens_out=20, cost_usd=0.001)
    assert any("[llm]" in r.message for r in caplog.records)


def test_end_tool_span_console_fallback_logs(caplog: pytest.LogCaptureFixture) -> None:
    """end_tool_span() emits a debug log line when _console_fallback is True."""
    tracer = Tracer()
    tracer._console_fallback = True
    span = tracer.start_tool_span("calculator", trace_id="trace-tool")
    with caplog.at_level(logging.DEBUG, logger="civitas.observability.tracer"):
        tracer.end_tool_span(span, status="ok")
    assert any("[tool]" in r.message for r in caplog.records)
