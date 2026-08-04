"""Observability — tracing, metrics, and span management."""

from __future__ import annotations

from civitas.observability.export_backend import ConsoleBackend, ExportBackend, FanOutBackend
from civitas.observability.otel_agent import run_otel_agent
from civitas.observability.span_queue import SpanData, SpanQueue
from civitas.observability.span_store import (
    CostBucket,
    InMemorySpanStore,
    MessageRateBucket,
    SpanRecord,
    SpanStore,
    normalize_span,
)
from civitas.observability.tracer import Span, Tracer

__all__ = [
    "ConsoleBackend",
    "CostBucket",
    "ExportBackend",
    "FanOutBackend",
    "InMemorySpanStore",
    "MessageRateBucket",
    "normalize_span",
    "run_otel_agent",
    "Span",
    "SpanData",
    "SpanQueue",
    "SpanRecord",
    "SpanStore",
    "Tracer",
]
