"""Unit tests for civitas/observability/sqlite_backend.py (v0.9.3.x, Track B, B1).

See docs/design/telemetry-native.md for the full design and decision log.
Integration-level "does a real Runtime actually produce correct rows in a
real .db file" coverage lives in
tests/integration/test_sqlite_backend_integration.py.
"""

from __future__ import annotations

import time
from pathlib import Path

import aiosqlite

from civitas.observability.span_queue import SpanData
from civitas.observability.sqlite_backend import (
    SQLiteBackend,
    index_from_filename,
    normalize_span,
    window_filename,
    window_index,
)


def _span(name: str, attributes: dict, start_time: float | None = None) -> SpanData:
    ts = start_time if start_time is not None else time.time()
    return SpanData(
        name=name,
        trace_id="a" * 32,
        span_id="1" * 16,
        parent_span_id=None,
        start_time=ts,
        end_time=ts + 1.0,
        attributes=attributes,
        status="ok",
    )


# ---------------------------------------------------------------------------
# normalize_span — design doc §4's normalization table
# ---------------------------------------------------------------------------


def test_normalize_agent_lifecycle_span():
    span = _span("civitas.agent.start", {"civitas.agent.name": "worker_a"})
    result = normalize_span(span)
    assert result["agent_name"] == "worker_a"
    assert result["llm_model"] is None


def test_normalize_llm_span_from_agentprocess_llm_span_extracts_all_fields():
    """AgentProcess.llm_span() -- the actually-used, ergonomic API real agent
    code calls (confirmed via examples/dashboard_demo/agents.py). Its span
    name is `civitas.llm.chat` with `civitas.llm.*` attributes -- NOT the
    same shape as Tracer.start_llm_span()'s raw `llm.chat {model}` (below).
    civitas.agent.name was ADDED to this span's real attributes (civitas/
    process.py) after discovering, while writing this normalization
    function, that no civitas.llm.chat span had EVER carried any agent
    identity in either OTEL/Jaeger export (Track A, already shipped) or any
    storage backend -- a real, silent gap, fixed at the root.
    """
    span = _span(
        "civitas.llm.chat",
        {
            "civitas.agent.name": "chatty",
            "civitas.llm.model": "gpt-4o",
            "civitas.llm.tokens_in": 100,
            "civitas.llm.tokens_out": 50,
            "civitas.llm.cost_usd": 0.01,
        },
    )
    result = normalize_span(span)
    assert result["agent_name"] == "chatty"
    assert result["llm_model"] == "gpt-4o"
    assert result["llm_tokens_in"] == 100
    assert result["llm_tokens_out"] == 50
    assert result["llm_cost_usd"] == 0.01


def test_normalize_llm_span_from_raw_tracer_api_has_no_agent_identity():
    """Tracer.start_llm_span()/end_llm_span() -- a lower-level, standalone
    API (examples/research_assistant.py) called directly on a bare Tracer
    with no AgentProcess/"self" context at all. Its span name is
    `llm.chat {model}` with plain `llm.*` attributes (no `civitas.` prefix)
    -- a genuinely DIFFERENT shape from civitas.llm.chat above, and
    architecturally has no agent identity to extract (unlike
    civitas.llm.chat, which DOES have self.name available and was missing
    it as a real oversight). agent_name=None here is correct, not a gap.
    """
    span = _span(
        "llm.chat gpt-4o",
        {
            "llm.model": "gpt-4o",
            "llm.tokens_in": 100,
            "llm.tokens_out": 50,
            "llm.cost_usd": 0.01,
        },
    )
    result = normalize_span(span)
    assert result["agent_name"] is None
    assert result["llm_model"] == "gpt-4o"
    assert result["llm_tokens_in"] == 100
    assert result["llm_tokens_out"] == 50
    assert result["llm_cost_usd"] == 0.01


def test_normalize_send_span():
    span = _span("send message", {"civitas.sender": "frontend", "civitas.recipient": "worker_a"})
    result = normalize_span(span)
    assert result["agent_name"] == "frontend"


def test_normalize_recv_span():
    span = _span("recv message", {"civitas.sender": "frontend", "civitas.recipient": "worker_a"})
    result = normalize_span(span)
    assert result["agent_name"] == "frontend"


def test_normalize_tool_span():
    span = _span(
        "tool.execute web_search", {"civitas.sender": "researcher", "tool.name": "web_search"}
    )
    result = normalize_span(span)
    assert result["agent_name"] == "researcher"


def test_normalize_supervisor_restart_uses_child_not_supervisor():
    """The restarted CHILD is "which agent this is about", not the supervisor
    doing the restarting (design doc §4's explicit note)."""
    span = _span("supervisor.restart", {"civitas.supervisor": "root", "civitas.child": "flaky"})
    result = normalize_span(span)
    assert result["agent_name"] == "flaky"


def test_normalize_unmatched_span_is_null_not_a_guess():
    """A span matching none of the known kinds gets agent_name=None -- never
    a wrong guess (design doc §4)."""
    span = _span("some.custom.span", {"whatever": "value"})
    result = normalize_span(span)
    assert result["agent_name"] is None
    assert result["llm_model"] is None


def test_normalize_llm_span_missing_attrs_is_null_not_zero():
    """A malformed/incomplete llm span shouldn't fabricate 0s."""
    span = _span("civitas.llm.chat", {"civitas.agent.name": "chatty"})
    result = normalize_span(span)
    assert result["llm_tokens_in"] is None
    assert result["llm_cost_usd"] is None


# ---------------------------------------------------------------------------
# window_index / window_filename — round-trip and boundary behavior
# ---------------------------------------------------------------------------


def test_window_index_is_epoch_anchored_not_calendar_month():
    window_seconds = 30 * 86400
    assert window_index(0.0, 30) == 0
    assert window_index(window_seconds - 1, 30) == 0
    assert window_index(window_seconds, 30) == 1


def test_window_filename_is_human_readable_start_date():
    assert window_filename(0, 30) == "civitas_spans_1970-01-01.db"


def test_window_filename_round_trips_through_index_from_filename():
    """index_from_filename() is a module-level function (not a SQLiteBackend
    method) so SQLiteQueryEngine (B2) can enumerate window files without
    reaching into SQLiteBackend's internals."""
    for idx in (0, 1, 5, 100, 12345):
        filename = window_filename(idx, 30)
        assert index_from_filename(filename, 30) == idx


def test_index_from_filename_rejects_malformed_names():
    assert index_from_filename("not_a_span_file.db", 30) is None
    assert index_from_filename("civitas_spans_not-a-date.db", 30) is None


# ---------------------------------------------------------------------------
# SQLiteBackend.export() — real writes, real files, real queries
# ---------------------------------------------------------------------------


async def test_export_creates_a_real_queryable_file(tmp_path: Path):
    backend = SQLiteBackend(db_dir=str(tmp_path), window_days=30, retention_windows=6)
    now = time.time()
    span = _span(
        "civitas.llm.chat",
        {
            "civitas.agent.name": "chatty",
            "civitas.llm.model": "gpt-4o",
            "civitas.llm.tokens_in": 100,
            "civitas.llm.tokens_out": 50,
            "civitas.llm.cost_usd": 0.01,
        },
        start_time=now,
    )
    await backend.export([span])

    files = backend.list_window_files()
    assert len(files) == 1
    await backend.shutdown()

    conn = await aiosqlite.connect(str(files[0]))
    try:
        cursor = await conn.execute(
            "SELECT name, agent_name, llm_model, llm_tokens_in, llm_cost_usd FROM spans"
        )
        rows = await cursor.fetchall()
    finally:
        await conn.close()
    assert rows == [("civitas.llm.chat", "chatty", "gpt-4o", 100, 0.01)]


async def test_export_preserves_full_attributes_json_for_drill_down(tmp_path: Path):
    backend = SQLiteBackend(db_dir=str(tmp_path), window_days=30)
    now = time.time()
    span = _span(
        "send message",
        {"civitas.sender": "a", "civitas.recipient": "b", "custom.tag": "x"},
        start_time=now,
    )
    await backend.export([span])
    files = backend.list_window_files()
    await backend.shutdown()

    conn = await aiosqlite.connect(str(files[0]))
    try:
        cursor = await conn.execute("SELECT attributes_json FROM spans")
        (raw,) = await cursor.fetchone()
    finally:
        await conn.close()
    import json

    assert json.loads(raw) == {"civitas.sender": "a", "civitas.recipient": "b", "custom.tag": "x"}


async def test_export_empty_list_is_a_noop(tmp_path: Path):
    backend = SQLiteBackend(db_dir=str(tmp_path))
    await backend.export([])
    assert backend.list_window_files() == []


async def test_shutdown_closes_all_connections_idempotently(tmp_path: Path):
    backend = SQLiteBackend(db_dir=str(tmp_path))
    await backend.export([_span("send x", {"civitas.sender": "a"})])
    await backend.shutdown()
    await backend.shutdown()  # must not raise


# ---------------------------------------------------------------------------
# Window rollover
# ---------------------------------------------------------------------------


async def test_spans_on_either_side_of_a_window_boundary_land_in_two_files(tmp_path: Path):
    window_days = 30
    window_seconds = window_days * 86400
    # Anchor near "now" so retention (relative to real time.time()) doesn't
    # sweep these away — pick a boundary far enough in the future.
    current_idx = window_index(time.time(), window_days)
    boundary = (current_idx + 1) * window_seconds

    backend = SQLiteBackend(db_dir=str(tmp_path), window_days=window_days, retention_windows=1000)
    before = _span("send x", {"civitas.sender": "a"}, start_time=boundary - 1)
    after = _span("send y", {"civitas.sender": "a"}, start_time=boundary)
    await backend.export([before, after])

    files = backend.list_window_files()
    assert len(files) == 2
    await backend.shutdown()


# ---------------------------------------------------------------------------
# Retention sweep — deletes whole FILES, not rows (design doc §3)
# ---------------------------------------------------------------------------


async def test_retention_sweep_removes_only_files_older_than_the_window(tmp_path: Path):
    window_days = 1
    backend = SQLiteBackend(db_dir=str(tmp_path), window_days=window_days, retention_windows=2)

    now = time.time()
    window_seconds = window_days * 86400
    current_idx = window_index(now, window_days)

    # A span from "now" (kept) and one from far enough in the past to be
    # outside the retention window (removed).
    recent = _span("send x", {"civitas.sender": "a"}, start_time=now)
    ancient = _span(
        "send y", {"civitas.sender": "a"}, start_time=(current_idx - 10) * window_seconds
    )

    await backend.export([recent, ancient])
    files_after = {p.name for p in backend.list_window_files()}

    assert window_filename(current_idx, window_days) in files_after
    assert window_filename(current_idx - 10, window_days) not in files_after
    await backend.shutdown()


async def test_a_span_written_directly_into_an_already_expired_window_is_swept_immediately(
    tmp_path: Path,
):
    """Retention runs after EVERY export() call, including the one that just
    wrote the ancient span -- it doesn't linger until some later export
    happens to trigger a sweep. Also proves the still-open aiosqlite
    connection gets closed before the file is deleted (Windows in
    particular refuses to delete an open file)."""
    window_days = 1
    backend = SQLiteBackend(db_dir=str(tmp_path), window_days=window_days, retention_windows=1)
    window_seconds = window_days * 86400
    current_idx = window_index(time.time(), window_days)

    ancient_idx = current_idx - 5
    ancient = _span("send x", {"civitas.sender": "a"}, start_time=ancient_idx * window_seconds)
    await backend.export([ancient])

    assert ancient_idx not in backend._connections
    assert window_filename(ancient_idx, window_days) not in {
        p.name for p in backend.list_window_files()
    }
    await backend.shutdown()


async def test_retention_keeps_a_window_still_within_the_retained_range(tmp_path: Path):
    """The counterpart to the immediate-sweep test above -- a window that IS
    still within retention_windows of "now" must survive its own export."""
    window_days = 1
    backend = SQLiteBackend(db_dir=str(tmp_path), window_days=window_days, retention_windows=6)
    now = time.time()

    recent = _span("send x", {"civitas.sender": "a"}, start_time=now)
    await backend.export([recent])

    current_idx = window_index(now, window_days)
    assert current_idx in backend._connections
    assert window_filename(current_idx, window_days) in {
        p.name for p in backend.list_window_files()
    }
    await backend.shutdown()
