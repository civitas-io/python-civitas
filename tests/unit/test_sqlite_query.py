"""Unit tests for civitas/observability/sqlite_query.py (v0.9.3.x, Track B, B2).

See docs/design/telemetry-native.md for the design conversation. Real,
against-actual-files coverage (not mocked aiosqlite) — matching this
project's "verify against the real thing" standard.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from civitas.observability.span_queue import SpanData
from civitas.observability.sqlite_backend import SQLiteBackend, window_index
from civitas.observability.sqlite_query import SQLiteQueryEngine


def _llm_span(agent: str, model: str, cost: float, start_time: float) -> SpanData:
    return SpanData(
        name="civitas.llm.chat",
        trace_id="a" * 32,
        span_id="1" * 16,
        parent_span_id=None,
        start_time=start_time,
        end_time=start_time + 1.0,
        attributes={
            "civitas.agent.name": agent,
            "civitas.llm.model": model,
            "civitas.llm.tokens_in": 100,
            "civitas.llm.tokens_out": 50,
            "civitas.llm.cost_usd": cost,
        },
        status="ok",
    )


def _handle_span(agent: str, start_time: float) -> SpanData:
    return SpanData(
        name="civitas.agent.handle",
        trace_id="b" * 32,
        span_id="2" * 16,
        parent_span_id=None,
        start_time=start_time,
        end_time=start_time + 0.1,
        attributes={"civitas.agent.name": agent},
        status="ok",
    )


async def test_cost_by_agent_aggregates_within_one_window(tmp_path: Path):
    now = time.time()
    backend = SQLiteBackend(db_dir=str(tmp_path))
    await backend.export(
        [
            _llm_span("chatty", "gpt-4o", 0.01, now),
            _llm_span("chatty", "claude", 0.02, now),
            _llm_span("other", "gpt-4o", 0.05, now),
        ]
    )
    await backend.shutdown()

    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    result = await engine.cost_by_agent(now - 10, now + 10)
    assert result == {"chatty": pytest.approx(0.03), "other": pytest.approx(0.05)}


async def test_cost_by_model_aggregates_across_agents(tmp_path: Path):
    now = time.time()
    backend = SQLiteBackend(db_dir=str(tmp_path))
    await backend.export(
        [
            _llm_span("a", "gpt-4o", 0.01, now),
            _llm_span("b", "gpt-4o", 0.02, now),
            _llm_span("a", "claude", 0.03, now),
        ]
    )
    await backend.shutdown()

    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    result = await engine.cost_by_model(now - 10, now + 10)
    assert result == {"gpt-4o": pytest.approx(0.03), "claude": pytest.approx(0.03)}


async def test_cost_over_time_buckets_by_agent_and_model(tmp_path: Path):
    now = time.time()
    backend = SQLiteBackend(db_dir=str(tmp_path))
    await backend.export([_llm_span("chatty", "gpt-4o", 0.01, now)])
    await backend.shutdown()

    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    buckets = await engine.cost_over_time(now - 10, now + 10, bucket_seconds=3600)
    assert len(buckets) == 1
    bucket = buckets[0]
    assert bucket.agent_name == "chatty"
    assert bucket.model == "gpt-4o"
    assert bucket.total_cost_usd == pytest.approx(0.01)
    assert bucket.total_tokens_in == 100
    assert bucket.total_tokens_out == 50


async def test_message_rate_over_time_counts_handle_spans_not_every_kind(tmp_path: Path):
    """Counts civitas.agent.handle spans specifically -- matching A2's
    Prometheus civitas_messages_handled_total semantic -- not every span
    kind (send/recv spans would double-count the same logical message)."""
    now = time.time()
    backend = SQLiteBackend(db_dir=str(tmp_path))
    await backend.export(
        [
            _handle_span("chatty", now),
            _handle_span("chatty", now),
            _llm_span("chatty", "gpt-4o", 0.01, now),  # NOT a handle span -- not counted
        ]
    )
    await backend.shutdown()

    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    buckets = await engine.message_rate_over_time(now - 10, now + 10, bucket_seconds=3600)
    assert len(buckets) == 1
    assert buckets[0].agent_name == "chatty"
    assert buckets[0].message_count == 2


async def test_cost_by_agent_excludes_rows_with_no_agent_identity(tmp_path: Path):
    """The lower-level Tracer.start_llm_span() API has no agent identity to
    attach (sqlite_backend.py's normalize_span()) -- a dict keyed by agent
    name can't meaningfully represent "no agent", so those rows are
    excluded, not surfaced as a bogus None key."""
    now = time.time()
    raw_tracer_span = SpanData(
        name="llm.chat gpt-4o",
        trace_id="c" * 32,
        span_id="3" * 16,
        parent_span_id=None,
        start_time=now,
        end_time=now + 1,
        attributes={"llm.model": "gpt-4o", "llm.cost_usd": 0.01},
        status="ok",
    )
    backend = SQLiteBackend(db_dir=str(tmp_path))
    await backend.export([raw_tracer_span, _llm_span("chatty", "gpt-4o", 0.02, now)])
    await backend.shutdown()

    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    result = await engine.cost_by_agent(now - 10, now + 10)
    assert result == {"chatty": pytest.approx(0.02)}
    assert None not in result


async def test_query_across_a_real_window_boundary_merges_correctly(tmp_path: Path):
    """The actual reason B2 exists as a separate ticket from B1: a time
    range spanning two of SQLiteBackend's window files must merge into ONE
    correct total via a real SQLite ATTACH DATABASE query, not silently
    only see one file's worth of data."""
    window_days = 30
    window_seconds = window_days * 86400
    current_idx = window_index(time.time(), window_days)
    boundary = (current_idx + 1) * window_seconds

    backend = SQLiteBackend(db_dir=str(tmp_path), window_days=window_days, retention_windows=1000)
    await backend.export(
        [
            _llm_span("chatty", "gpt-4o", 0.01, boundary - 10),
            _llm_span("chatty", "gpt-4o", 0.02, boundary + 10),
        ]
    )
    files = backend.list_window_files()
    assert len(files) == 2  # confirms this genuinely tests cross-window behavior
    await backend.shutdown()

    engine = SQLiteQueryEngine(db_dir=str(tmp_path), window_days=window_days)
    result = await engine.cost_by_agent(boundary - 100, boundary + 100)
    assert result == {"chatty": pytest.approx(0.03)}


async def test_bucket_straddling_a_window_boundary_merges_into_one_row(tmp_path: Path):
    """cost_over_time()'s double GROUP BY exists specifically for this case
    -- without the outer re-aggregation, a bucket whose spans landed in two
    different window files would come back as two separate rows instead of
    one correctly-merged one.

    A window boundary (a multiple of window_days*86400) is ALWAYS also a
    24h-bucket boundary, so a 1-day bucket_seconds could never straddle a
    window file boundary -- window_days=1 with a 2-day (172800s) bucket, and
    an explicitly ODD day-count boundary (so the 2-day bucket's own boundary
    does NOT coincide with the window boundary), is what actually produces
    the cross-window-same-bucket case this test needs.
    """
    window_days = 1
    window_seconds = window_days * 86400
    bucket_seconds = 172800
    boundary_day = window_index(time.time(), window_days) + 1
    if boundary_day % 2 == 0:
        boundary_day += 1
    boundary = boundary_day * window_seconds

    backend = SQLiteBackend(db_dir=str(tmp_path), window_days=window_days, retention_windows=100000)
    # Both timestamps round down to the same bucket_seconds-sized bucket,
    # but land in DIFFERENT window files (one just before, one just after
    # the window boundary) -- verified directly before writing this test.
    await backend.export(
        [
            _llm_span("chatty", "gpt-4o", 0.01, boundary - 5),
            _llm_span("chatty", "gpt-4o", 0.02, boundary + 5),
        ]
    )
    assert len(backend.list_window_files()) == 2
    await backend.shutdown()

    engine = SQLiteQueryEngine(db_dir=str(tmp_path), window_days=window_days)
    buckets = await engine.cost_over_time(
        boundary - 100, boundary + 100, bucket_seconds=bucket_seconds
    )
    same_bucket = [b for b in buckets if b.agent_name == "chatty" and b.model == "gpt-4o"]
    assert len(same_bucket) == 1, "must merge into ONE row, not two separate per-file rows"
    assert same_bucket[0].total_cost_usd == pytest.approx(0.03)


async def test_no_window_files_in_range_returns_empty_without_opening_a_connection(tmp_path: Path):
    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    result = await engine.cost_by_agent(time.time() - 10, time.time() + 10)
    assert result == {}


async def test_files_in_range_excludes_windows_outside_the_requested_range(tmp_path: Path):
    now = time.time()
    far_future = now + 400 * 86400  # well outside a default 30-day window
    backend = SQLiteBackend(db_dir=str(tmp_path), retention_windows=100000)
    await backend.export([_llm_span("chatty", "gpt-4o", 0.01, far_future)])
    await backend.shutdown()

    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    result = await engine.cost_by_agent(now - 10, now + 10)  # NOT the far-future range
    assert result == {}


def _span(name: str, trace: str, start_time: float, agent: str | None = None) -> SpanData:
    # NB: agent_name is only PROMOTED to a column for civitas.agent.*/llm.chat
    # span names (normalize_span) -- arbitrary-named spans here get agent_name
    # None, which is correct. These feed/drill-down tests assert on ordering and
    # trace identity, not agent promotion (covered by the cost_by_agent tests).
    attrs: dict = {}
    if agent is not None:
        attrs["civitas.agent.name"] = agent
    return SpanData(
        name=name,
        trace_id=trace,
        span_id=f"{start_time:016.0f}"[:16],
        parent_span_id=None,
        start_time=start_time,
        end_time=start_time + 0.05,
        attributes=attrs,
        status="ok",
    )


async def test_recent_spans_returns_newest_first_bounded_by_limit(tmp_path: Path):
    """v0.10.1: the log/event feed -- individual span rows, newest first, capped."""
    now = time.time()
    backend = SQLiteBackend(db_dir=str(tmp_path))
    await backend.export([_span(f"evt-{i}", "t" * 32, now + i, agent="w") for i in range(5)])
    await backend.shutdown()

    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    rows = await engine.recent_spans(now - 10, now + 100, limit=3)
    assert [r.name for r in rows] == ["evt-4", "evt-3", "evt-2"]  # newest first, limit 3
    assert rows[0].duration_ms == pytest.approx(50.0)


async def test_spans_in_trace_returns_that_trace_oldest_first(tmp_path: Path):
    """v0.10.1 (§13 drill-down): every span in trace X, chronological."""
    now = time.time()
    backend = SQLiteBackend(db_dir=str(tmp_path))
    await backend.export(
        [
            _span("root", "trace-A" + "0" * 25, now, agent="orch"),
            _span("child", "trace-A" + "0" * 25, now + 1, agent="worker"),
            _span("noise", "trace-B" + "0" * 25, now, agent="other"),
        ]
    )
    await backend.shutdown()

    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    rows = await engine.spans_in_trace("trace-A" + "0" * 25, now - 10, now + 10)
    assert [r.name for r in rows] == ["root", "child"]  # only trace-A, oldest first
    assert all(r.trace_id == "trace-A" + "0" * 25 for r in rows)


async def test_recent_spans_empty_when_no_files(tmp_path: Path):
    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    assert await engine.recent_spans(time.time() - 10, time.time() + 10) == []
