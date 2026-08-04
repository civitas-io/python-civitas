"""SpanStore — the durable, queryable telemetry-store seam (B4).

`ExportBackend` (``export_backend.py``) is the general write contract for
*anything* that receives spans (Console, OTel, FanOut — write-only). `SpanStore`
**extends** it for backends you can also *query*: cost/rate over time, per-agent/
per-model breakdowns, a recent-span feed, and per-trace drill-down.

One protocol per storage backend means the read and write sides share a single
schema definition and cannot silently drift (the old split
``SQLiteBackend``/``SQLiteQueryEngine`` pair could). ``normalize_span()`` lives
here as blessed, dependency-free public API: every store — ``SQLiteSpanStore``
in core, a future ``PostgresSpanStore`` in civitas-contrib — imports it rather
than reimplementing the SpanData→columns mapping.

Design: ``docs/design/spanstore-and-contrib-boundary.md``. This module has no
third-party dependency (no aiosqlite) — the protocol, the dataclasses, the
normalization, and the in-memory reference/test-double store all live here so
they import cleanly without the ``telemetry`` extra.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Protocol, runtime_checkable

from civitas.observability.export_backend import ExportBackend
from civitas.observability.span_queue import SpanData


@dataclasses.dataclass
class CostBucket:
    """One time bucket's LLM cost/token totals, broken down by agent+model."""

    bucket_start: float
    agent_name: str | None
    model: str | None
    total_cost_usd: float
    total_tokens_in: int
    total_tokens_out: int


@dataclasses.dataclass
class MessageRateBucket:
    """One time bucket's message-handling count for one agent."""

    bucket_start: float
    agent_name: str | None
    message_count: int


@dataclasses.dataclass
class SpanRecord:
    """One span/event row surfaced by ``recent_spans``/``spans_in_trace``.

    The promoted hot columns + timing/trace identity, NOT the raw
    ``attributes`` blob (which a store may keep for deeper drill-down).
    """

    name: str
    trace_id: str
    span_id: str
    parent_span_id: str | None
    start_time: float
    end_time: float
    status: str
    error_message: str | None
    agent_name: str | None
    llm_model: str | None
    llm_tokens_in: int | None
    llm_tokens_out: int | None
    llm_cost_usd: float | None

    @property
    def duration_ms(self) -> float:
        return (self.end_time - self.start_time) * 1000.0


def normalize_span(span: SpanData) -> dict[str, Any]:
    """Map a SpanData's attributes onto the promoted columns (public API, B4).

    Different span KINDS use different attribute keys for "which agent is this
    about" — there is no single universal key. Checked in order, first match
    wins; a span matching none gets ``agent_name=None``, not a guess. The
    returned key set (``agent_name``, ``llm_model``, ``llm_tokens_in``,
    ``llm_tokens_out``, ``llm_cost_usd``) is part of the ``SpanStore`` contract:
    every backend promotes exactly these. See ``telemetry-native.md`` §4 for the
    normalization table this implements.
    """
    attrs = span.attributes
    agent_name: str | None = None
    llm_model: str | None = None
    llm_tokens_in: int | None = None
    llm_tokens_out: int | None = None
    llm_cost_usd: float | None = None

    if span.name.startswith("civitas.agent."):
        agent_name = attrs.get("civitas.agent.name")
    elif span.name == "civitas.llm.chat":
        # AgentProcess.llm_span() -- the ergonomic API real agent code calls.
        # civitas.agent.name is added to this span's attributes at emit time.
        agent_name = attrs.get("civitas.agent.name")
        llm_model = attrs.get("civitas.llm.model")
        llm_tokens_in = attrs.get("civitas.llm.tokens_in")
        llm_tokens_out = attrs.get("civitas.llm.tokens_out")
        llm_cost_usd = attrs.get("civitas.llm.cost_usd")
    elif span.name.startswith("llm.chat "):
        # Tracer.start_llm_span()/end_llm_span() -- a lower-level, standalone
        # API called on a bare Tracer with no AgentProcess/"self" context, so
        # it has no agent identity to attach; agent_name stays None (not a guess).
        llm_model = attrs.get("llm.model")
        llm_tokens_in = attrs.get("llm.tokens_in")
        llm_tokens_out = attrs.get("llm.tokens_out")
        llm_cost_usd = attrs.get("llm.cost_usd")
    elif span.name.startswith(("send ", "recv ")):
        agent_name = attrs.get("civitas.sender")
    elif span.name.startswith("tool.execute"):
        agent_name = attrs.get("civitas.sender")
    elif span.name == "supervisor.restart":
        agent_name = attrs.get("civitas.child")

    return {
        "agent_name": agent_name,
        "llm_model": llm_model,
        "llm_tokens_in": llm_tokens_in,
        "llm_tokens_out": llm_tokens_out,
        "llm_cost_usd": llm_cost_usd,
    }


@runtime_checkable
class SpanStore(ExportBackend, Protocol):
    """A durable, queryable telemetry store (B4).

    Extends ``ExportBackend`` (``export``/``shutdown``) with the read surface,
    so one implementation owns both sides over a single schema. ``@runtime_
    checkable`` (like ``StateStore``) so callers can feature-detect the query
    surface with ``isinstance(backend, SpanStore)``.
    """

    async def cost_over_time(
        self, since: float, until: float, bucket_seconds: int = 86400
    ) -> list[CostBucket]: ...

    async def message_rate_over_time(
        self, since: float, until: float, bucket_seconds: int = 3600
    ) -> list[MessageRateBucket]: ...

    async def cost_by_agent(self, since: float, until: float) -> dict[str, float]: ...

    async def cost_by_model(self, since: float, until: float) -> dict[str, float]: ...

    async def recent_spans(
        self, since: float, until: float, limit: int = 200
    ) -> list[SpanRecord]: ...

    async def spans_in_trace(
        self, trace_id: str, since: float, until: float
    ) -> list[SpanRecord]: ...


def _bucket_start(t: float, bucket_seconds: int) -> float:
    """Truncate a timestamp to its bucket start -- matches SQLite's
    ``CAST(start_time / bucket AS INTEGER) * bucket`` exactly (floor for the
    non-negative timestamps this store ever sees)."""
    return int(t / bucket_seconds) * bucket_seconds


class InMemorySpanStore:
    """A dependency-free ``SpanStore`` — reference impl + test double (B4).

    Holds normalized spans in a list; every query is plain Python aggregation
    with the SAME semantics as ``SQLiteSpanStore``'s SQL. Its purpose is to
    prove the ``SpanStore`` seam is genuinely backend-agnostic (the query test
    suite runs against both) and to give tests a fast store with no ``aiosqlite``
    / disk. It is the telemetry analogue of ``InMemoryStateStore``. Bounded by
    ``maxsize`` (drops oldest); not intended as a production store.
    """

    def __init__(self, maxsize: int = 100_000) -> None:
        self._maxsize = maxsize
        self._spans: list[SpanRecord] = []

    async def export(self, spans: list[SpanData]) -> None:
        for span in spans:
            n = normalize_span(span)
            self._spans.append(
                SpanRecord(
                    name=span.name,
                    trace_id=span.trace_id,
                    span_id=span.span_id,
                    parent_span_id=span.parent_span_id,
                    start_time=span.start_time,
                    end_time=span.end_time,
                    status=span.status,
                    error_message=span.error_message,
                    agent_name=n["agent_name"],
                    llm_model=n["llm_model"],
                    llm_tokens_in=n["llm_tokens_in"],
                    llm_tokens_out=n["llm_tokens_out"],
                    llm_cost_usd=n["llm_cost_usd"],
                )
            )
        if len(self._spans) > self._maxsize:
            self._spans = self._spans[-self._maxsize :]

    async def shutdown(self) -> None:
        self._spans.clear()

    async def cost_over_time(
        self, since: float, until: float, bucket_seconds: int = 86400
    ) -> list[CostBucket]:
        groups: dict[tuple[float, str | None, str | None], list[float | int]] = {}
        for s in self._spans:
            if s.llm_cost_usd is None or not (since <= s.start_time <= until):
                continue
            key = (_bucket_start(s.start_time, bucket_seconds), s.agent_name, s.llm_model)
            acc = groups.setdefault(key, [0.0, 0, 0])
            acc[0] += s.llm_cost_usd
            acc[1] += s.llm_tokens_in or 0  # SUM ignores NULL, matching SQL
            acc[2] += s.llm_tokens_out or 0
        buckets = [
            CostBucket(
                bucket_start=k[0],
                agent_name=k[1],
                model=k[2],
                total_cost_usd=v[0],
                total_tokens_in=int(v[1]),
                total_tokens_out=int(v[2]),
            )
            for k, v in groups.items()
        ]
        buckets.sort(key=lambda b: b.bucket_start)
        return buckets

    async def message_rate_over_time(
        self, since: float, until: float, bucket_seconds: int = 3600
    ) -> list[MessageRateBucket]:
        counts: dict[tuple[float, str | None], int] = {}
        for s in self._spans:
            if s.name != "civitas.agent.handle" or not (since <= s.start_time <= until):
                continue
            key = (_bucket_start(s.start_time, bucket_seconds), s.agent_name)
            counts[key] = counts.get(key, 0) + 1
        buckets = [
            MessageRateBucket(bucket_start=k[0], agent_name=k[1], message_count=v)
            for k, v in counts.items()
        ]
        buckets.sort(key=lambda b: b.bucket_start)
        return buckets

    async def cost_by_agent(self, since: float, until: float) -> dict[str, float]:
        out: dict[str, float] = {}
        for s in self._spans:
            if s.llm_cost_usd is None or s.agent_name is None:
                continue
            if not (since <= s.start_time <= until):
                continue
            out[s.agent_name] = out.get(s.agent_name, 0.0) + s.llm_cost_usd
        return out

    async def cost_by_model(self, since: float, until: float) -> dict[str, float]:
        out: dict[str, float] = {}
        for s in self._spans:
            if s.llm_cost_usd is None or s.llm_model is None:
                continue
            if not (since <= s.start_time <= until):
                continue
            out[s.llm_model] = out.get(s.llm_model, 0.0) + s.llm_cost_usd
        return out

    async def recent_spans(self, since: float, until: float, limit: int = 200) -> list[SpanRecord]:
        rows = [s for s in self._spans if since <= s.start_time <= until]
        rows.sort(key=lambda s: s.start_time, reverse=True)
        return rows[: int(limit)]

    async def spans_in_trace(self, trace_id: str, since: float, until: float) -> list[SpanRecord]:
        rows = [s for s in self._spans if s.trace_id == trace_id and since <= s.start_time <= until]
        rows.sort(key=lambda s: s.start_time)
        return rows
