"""Tracer — automatic span creation for message send/receive, LLM calls, and tool invocations.

Uses OpenTelemetry SDK when installed; falls back to a built-in print-based
ConsoleExporter when OTEL is not available. The rest of the runtime does not
know which backend is active.

When a SpanQueue is provided, completed spans are also pushed to it for
consumption by OTELAgent (async export without blocking the message loop).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from civitas.config import settings
from civitas.messages import Message, _new_span_id
from civitas.observability.span_queue import SpanData, SpanQueue

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Span abstraction — thin wrapper so callers don't depend on OTEL types
# ---------------------------------------------------------------------------


class Span:
    """Lightweight span that works with or without OTEL."""

    def __init__(
        self,
        name: str,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None = None,
        attributes: dict[str, Any] | None = None,
        _span_queue: SpanQueue | None = None,
    ) -> None:
        self.name = name
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self.attributes: dict[str, Any] = attributes or {}
        self.start_time = time.time()
        self.end_time: float | None = None
        self._otel_span: Any = None  # holds real OTEL span if available
        self._span_queue = _span_queue
        self._ended = False  # F08-4: guard against double-end

    def set_attribute(self, key: str, value: Any) -> None:
        """Set an attribute on this span."""
        self.attributes[key] = value
        if self._otel_span is not None:
            self._otel_span.set_attribute(key, value)

    def set_error(self, exc: BaseException) -> None:
        """Record an exception on this span."""
        self.attributes["error"] = True
        self.attributes["error.type"] = type(exc).__name__
        self.attributes["error.message"] = str(exc)
        if self._otel_span is not None:
            self._otel_span.record_exception(exc)

    def end(self) -> None:
        """Mark this span as finished. Idempotent — safe to call multiple times."""
        if self._ended:
            return
        self._ended = True
        self.end_time = time.time()
        if self._otel_span is not None:
            self._otel_span.end()
        if self._span_queue is not None:
            self._push_to_queue()

    def _push_to_queue(self) -> None:
        """Push completed span to SpanQueue for async export."""
        status = "error" if self.attributes.get("error") else "ok"
        error_msg = self.attributes.get("error.message") if status == "error" else None
        self._span_queue.put_nowait(  # type: ignore[union-attr]
            SpanData(
                name=self.name,
                trace_id=self.trace_id,
                span_id=self.span_id,
                parent_span_id=self.parent_span_id,
                start_time=self.start_time,
                end_time=self.end_time or time.time(),
                attributes=dict(self.attributes),
                status=status,
                error_message=error_msg,
            )
        )


# ---------------------------------------------------------------------------
# Tracer
# ---------------------------------------------------------------------------

# Try to import OTEL; set a flag for runtime use
_HAS_OTEL = False
try:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,  # F08-2: for OTLP
        SimpleSpanProcessor,  # for console fallback
    )
    from opentelemetry.sdk.trace.export import (
        ConsoleSpanExporter as OTELConsoleSpanExporter,
    )
    from opentelemetry.trace import (
        NonRecordingSpan,
        SpanContext,
        TraceFlags,
    )
    from opentelemetry.trace import set_span_in_context as _otel_set_span_in_context

    _HAS_OTEL = True
except ImportError:
    pass


def _otel_parent_context(trace_id: str, parent_span_id: str | None) -> Any:
    """Build a real OTEL parent Context from civitas's own trace/span IDs.

    v0.9.3 (A1). Returns ``None`` (root span, OTEL mints its own trace_id) when
    there's no parent to link to, or when either ID isn't valid OTEL-shaped
    hex -- civitas's own trace_id can legitimately be `""` for a message that
    started a flow with no prior context (e.g. an agent's own ``on_start()``),
    which is not an error, just "nothing to extract". Never raises.

    Known limitation, deliberately not engineered around: a caller-supplied
    non-empty trace_id with NO parent_span_id is discarded in favor of a
    fresh OTEL-minted one when OTEL is active. OTEL's own SpanContext model
    has no way to express "root span, but honor this specific trace_id" via
    the public API -- a context with an invalid (zero) span_id is rejected
    outright by the SDK (confirmed empirically), not partially honored. No
    real call site in this codebase hits this today (every caller derives
    trace_id and parent_span_id together, from the same message/span), so
    this is accepted rather than worked around.
    """
    if not _HAS_OTEL or not parent_span_id or not trace_id:
        return None
    try:
        span_context = SpanContext(
            trace_id=int(trace_id, 16),
            span_id=int(parent_span_id, 16),
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
    except (ValueError, TypeError):
        # Malformed/foreign IDs (e.g. hand-built Message in a test) -- start
        # a fresh OTEL root trace rather than crash the caller's send/receive.
        return None
    return _otel_set_span_in_context(NonRecordingSpan(span_context))


class Tracer:
    """Creates and enriches spans for Civitas operations.

    Behaviour depends on the environment:
    - opentelemetry-sdk installed + OTEL_EXPORTER_OTLP_ENDPOINT set -> OTLP exporter
    - opentelemetry-sdk installed, no endpoint -> OTEL ConsoleSpanExporter
    - opentelemetry-sdk not installed -> built-in print-based console output

    When span_queue is provided, completed spans are additionally pushed to the
    queue for consumption by OTELAgent (async, non-blocking export path).
    """

    def __init__(self, span_queue: SpanQueue | None = None) -> None:
        self._span_queue = span_queue
        self._use_otel = False
        self._otel_tracer: Any = None
        self._provider: Any = None  # F08-1/F08-5: instance-scoped provider
        self._console_fallback = span_queue is None

        if span_queue is not None:
            # Path B (SpanQueue -> OTELAgent -> ExportBackend) is authoritative
            # when a queue is supplied — the caller owns starting the OTELAgent
            # that drains it. Building a direct TracerProvider here too would
            # export every span twice with no coordination between them (FD-09).
            return

        if _HAS_OTEL:
            provider = TracerProvider()
            endpoint = settings.otel_endpoint

            if endpoint:
                try:
                    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                        OTLPSpanExporter,
                    )

                    exporter = OTLPSpanExporter(endpoint=endpoint)
                    # F08-2: BatchSpanProcessor exports in background thread —
                    # avoids blocking the event loop during OTLP network I/O.
                    provider.add_span_processor(BatchSpanProcessor(exporter))
                except ImportError:
                    provider.add_span_processor(SimpleSpanProcessor(OTELConsoleSpanExporter()))
            else:
                provider.add_span_processor(SimpleSpanProcessor(OTELConsoleSpanExporter()))

            # F08-1: store provider as instance attr; do NOT set the global
            self._provider = provider
            self._otel_tracer = provider.get_tracer("civitas", "0.1.0")
            self._use_otel = True
            self._console_fallback = False
        else:
            # F08-7: warn at startup if OTEL not available so operators notice
            logger.warning(
                "[Tracer] opentelemetry-sdk not installed — using console fallback; "
                "install opentelemetry-sdk for structured tracing"
            )

    # ------------------------------------------------------------------
    # Internal — span factory
    # ------------------------------------------------------------------

    def _make_span(
        self,
        name: str,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        attributes: dict[str, Any] | None,
    ) -> Span:
        span = Span(
            name=name,
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            attributes=attributes or {},
            _span_queue=self._span_queue,
        )
        if self._use_otel and self._otel_tracer is not None:
            # v0.9.3 (A1): without an explicit parent Context, every OTEL span
            # becomes its OWN root trace with a random OTEL-assigned trace_id
            # and `parent_id: null` -- confirmed live, not assumed -- meaning
            # a real Jaeger/Grafana view (docs/observability.md Mode 3) showed
            # every send/recv/llm/tool span as a disconnected single-span
            # "trace", never a tree, regardless of civitas's own correct
            # trace_id/parent_span_id bookkeeping on Span/Message. Reconstruct
            # that parent as a real (non-recording, "remote") OTEL SpanContext
            # from civitas's own IDs -- the standard extracted-context pattern
            # every OTEL propagator uses -- so OTEL groups and nests spans the
            # same way civitas already does internally.
            otel_context = _otel_parent_context(trace_id, parent_span_id)
            otel_span = self._otel_tracer.start_span(
                name, context=otel_context, attributes=span.attributes
            )
            span._otel_span = otel_span
            # OTEL mints its own trace_id/span_id when starting a span --
            # there is no public API to force a specific one. Make that
            # REAL id authoritative for civitas's own bookkeeping too,
            # since MessageBus.route()/request() (v0.9.3, A1) copy
            # span.trace_id/span_id back onto the outgoing Message before
            # it hits the wire -- without this, the id a downstream hop's
            # handle_span/recv_span would try to parent to would never
            # match any span OTEL actually emitted, breaking the very
            # linkage this fix exists to restore.
            real_ctx = otel_span.get_span_context()
            span.trace_id = format(real_ctx.trace_id, "032x")
            span.span_id = format(real_ctx.span_id, "016x")
        return span

    # ------------------------------------------------------------------
    # Span creation
    # ------------------------------------------------------------------

    def start_send_span(self, message: Message) -> Span:
        """Create a span for an outbound message send."""
        span = self._make_span(
            name=f"send {message.type}",
            trace_id=message.trace_id,
            span_id=message.span_id,
            parent_span_id=message.parent_span_id,
            attributes={
                "civitas.sender": message.sender,
                "civitas.recipient": message.recipient,
                "civitas.message_type": message.type,
                "civitas.message_id": message.id,
            },
        )
        if self._console_fallback:
            ts = time.strftime("%H:%M:%S", time.localtime(span.start_time))
            ms = f"{span.start_time % 1:.3f}"[1:]
            logger.debug(  # F08-7: debug so operators can opt in
                "[%s%s] %s -> %s: %s",
                ts,
                ms,
                message.sender,
                message.recipient,
                message.type,
            )
        return span

    def start_receive_span(self, message: Message) -> Span:
        """Create a span for an inbound message receive."""
        return self._make_span(
            name=f"recv {message.type}",
            trace_id=message.trace_id,
            span_id=_new_span_id(),
            parent_span_id=message.span_id,
            attributes={
                "civitas.sender": message.sender,
                "civitas.recipient": message.recipient,
                "civitas.message_type": message.type,
                "civitas.message_id": message.id,
            },
        )

    def start_span(
        self,
        name: str,
        trace_id: str = "",
        parent_span_id: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> Span:
        """Create a general-purpose span (for agent lifecycle, LLM calls, etc)."""
        return self._make_span(
            name=name,
            trace_id=trace_id or os.urandom(16).hex(),
            span_id=_new_span_id(),
            parent_span_id=parent_span_id,
            attributes=attributes or {},
        )

    def start_llm_span(
        self,
        model: str,
        trace_id: str,  # F08-3: required — callers must supply a trace_id
        parent_span_id: str | None = None,
    ) -> Span:
        """Create a span for an LLM call. Call end_llm_span() after the call completes."""
        return self.start_span(
            name=f"llm.chat {model}",
            trace_id=trace_id,
            parent_span_id=parent_span_id,
            attributes={"llm.model": model},
        )

    def end_llm_span(
        self,
        span: Span,
        *,
        tokens_in: int = 0,
        tokens_out: int = 0,
        cost_usd: float = 0.0,
    ) -> None:
        """Enrich and close an LLM span with response metrics."""
        span.set_attribute("llm.tokens_in", tokens_in)
        span.set_attribute("llm.tokens_out", tokens_out)
        span.set_attribute("llm.cost_usd", cost_usd)
        latency_ms = (time.time() - span.start_time) * 1000
        span.set_attribute("llm.latency_ms", round(latency_ms, 2))
        span.end()
        if self._console_fallback:
            model = span.attributes.get("llm.model", "?")
            logger.debug(  # F08-7
                "  [llm] %s: %din/%dout $%.4f %.0fms",
                model,
                tokens_in,
                tokens_out,
                cost_usd,
                latency_ms,
            )

    def start_tool_span(
        self,
        tool_name: str,
        trace_id: str = "",
        parent_span_id: str | None = None,
    ) -> Span:
        """Create a span for a tool invocation."""
        return self.start_span(
            name=f"tool.execute {tool_name}",
            trace_id=trace_id,
            parent_span_id=parent_span_id,
            attributes={"tool.name": tool_name},
        )

    def end_tool_span(
        self,
        span: Span,
        *,
        status: str = "ok",
    ) -> None:
        """Enrich and close a tool span with result status."""
        span.set_attribute("tool.result_status", status)
        latency_ms = (time.time() - span.start_time) * 1000
        span.set_attribute("tool.latency_ms", round(latency_ms, 2))
        span.end()
        if self._console_fallback:
            tool_name = span.attributes.get("tool.name", "?")
            logger.debug("  [tool] %s: %s %.0fms", tool_name, status, latency_ms)  # F08-7

    def new_trace_id(self) -> str:
        """Generate a new trace ID (32-hex-char)."""
        return os.urandom(16).hex()

    def new_span_id(self) -> str:
        """Generate a new span ID (16-hex-char)."""
        return _new_span_id()

    def flush(self) -> None:
        """Force-export any pending spans via this tracer's provider."""
        if self._use_otel and self._provider is not None:  # F08-5: use instance provider
            self._provider.force_flush()
