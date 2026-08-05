"""SpanStore protocol conformance + normalize_span contract (B4).

The parametrized ``store`` fixture runs the SAME query assertions against both
``SQLiteSpanStore`` (disk, aiosqlite) and ``InMemorySpanStore`` (pure Python) --
that cross-backend equivalence is what proves the ``SpanStore`` seam is genuinely
backend-agnostic, not SQLite-shaped. See
``docs/design/spanstore-and-contrib-boundary.md``.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from civitas.observability import (
    InMemorySpanStore,
    SpanStore,
    normalize_span,
)
from civitas.observability.span_queue import SpanData
from civitas.observability.sqlite_backend import SQLiteSpanStore

# Anchored to the current hour boundary: recent enough that SQLiteSpanStore's
# retention sweep (drops window files older than 6 windows) keeps it, and aligned
# so NOW+5..NOW+20 share one minute bucket while NOW+65..NOW+70 fall in the next.
NOW = float((int(time.time()) // 3600) * 3600)


def _llm(agent: str, model: str, cost: float, t: float, tin: int = 100, tout: int = 50) -> SpanData:
    return SpanData(
        name="civitas.llm.chat",
        trace_id="a" * 32,
        span_id=f"{int(t):016d}",
        parent_span_id=None,
        start_time=t,
        end_time=t + 1.0,
        attributes={
            "civitas.agent.name": agent,
            "civitas.llm.model": model,
            "civitas.llm.tokens_in": tin,
            "civitas.llm.tokens_out": tout,
            "civitas.llm.cost_usd": cost,
        },
        status="ok",
    )


def _handle(agent: str, t: float, trace: str = "b" * 32) -> SpanData:
    return SpanData(
        name="civitas.agent.handle",
        trace_id=trace,
        span_id=f"h{int(t):015d}",
        parent_span_id=None,
        start_time=t,
        end_time=t + 0.1,
        attributes={"civitas.agent.name": agent},
        status="ok",
    )


_SPANS = [
    _llm("chatty", "gpt-4o", 0.01, NOW + 10),
    _llm("chatty", "gpt-4o", 0.02, NOW + 70),  # next 1-min bucket
    _llm("chatty", "claude", 0.05, NOW + 15),
    _llm("other", "gpt-4o", 0.03, NOW + 20),
    _handle("chatty", NOW + 5),
    _handle("chatty", NOW + 8),
    _handle("other", NOW + 65),  # next bucket
]


async def _populate(store: SpanStore) -> None:
    await store.export(list(_SPANS))


@pytest.fixture(params=["sqlite", "memory"])
async def store(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[SpanStore]:
    if request.param == "sqlite":
        s: SpanStore = SQLiteSpanStore(db_dir=str(tmp_path), window_days=30)
    else:
        s = InMemorySpanStore()
    await _populate(s)
    yield s
    await s.shutdown()


# --- protocol conformance (runs against BOTH stores) ----------------------


@pytest.mark.asyncio
async def test_store_satisfies_protocol(store: SpanStore) -> None:
    assert isinstance(store, SpanStore)


@pytest.mark.asyncio
async def test_cost_by_agent(store: SpanStore) -> None:
    result = await store.cost_by_agent(NOW, NOW + 100)
    assert result == {"chatty": pytest.approx(0.08), "other": pytest.approx(0.03)}


@pytest.mark.asyncio
async def test_cost_by_model(store: SpanStore) -> None:
    result = await store.cost_by_model(NOW, NOW + 100)
    assert result == {"gpt-4o": pytest.approx(0.06), "claude": pytest.approx(0.05)}


@pytest.mark.asyncio
async def test_cost_over_time_buckets_by_minute(store: SpanStore) -> None:
    buckets = await store.cost_over_time(NOW, NOW + 100, bucket_seconds=60)
    key = lambda b: (b.bucket_start, b.agent_name or "", b.model or "")  # noqa: E731
    got = {
        (b.bucket_start, b.agent_name, b.model): b.total_cost_usd for b in sorted(buckets, key=key)
    }
    b0 = int((NOW + 10) / 60) * 60
    b1 = int((NOW + 70) / 60) * 60
    assert got[(b0, "chatty", "gpt-4o")] == pytest.approx(0.01)
    assert got[(b0, "chatty", "claude")] == pytest.approx(0.05)
    assert got[(b0, "other", "gpt-4o")] == pytest.approx(0.03)
    assert got[(b1, "chatty", "gpt-4o")] == pytest.approx(0.02)


@pytest.mark.asyncio
async def test_message_rate_over_time(store: SpanStore) -> None:
    buckets = await store.message_rate_over_time(NOW, NOW + 100, bucket_seconds=60)
    got = {(b.bucket_start, b.agent_name): b.message_count for b in buckets}
    b0 = int((NOW + 5) / 60) * 60
    b1 = int((NOW + 65) / 60) * 60
    assert got[(b0, "chatty")] == 2
    assert got[(b1, "other")] == 1


@pytest.mark.asyncio
async def test_recent_spans_newest_first_limit(store: SpanStore) -> None:
    rows = await store.recent_spans(NOW, NOW + 100, limit=3)
    assert len(rows) == 3
    starts = [r.start_time for r in rows]
    assert starts == sorted(starts, reverse=True)  # newest first


@pytest.mark.asyncio
async def test_spans_in_trace_oldest_first(store: SpanStore) -> None:
    rows = await store.spans_in_trace("a" * 32, NOW, NOW + 100)
    assert all(r.trace_id == "a" * 32 for r in rows)  # only the llm trace
    assert [r.start_time for r in rows] == sorted(r.start_time for r in rows)  # ASC


# --- cross-backend equivalence (the seam is real, not SQLite-shaped) ------


@pytest.mark.asyncio
async def test_sqlite_and_memory_agree(tmp_path: Path) -> None:
    sql = SQLiteSpanStore(db_dir=str(tmp_path))
    mem = InMemorySpanStore()
    await _populate(sql)
    await _populate(mem)
    try:
        assert await sql.cost_by_agent(NOW, NOW + 100) == await mem.cost_by_agent(NOW, NOW + 100)
        assert await sql.cost_by_model(NOW, NOW + 100) == await mem.cost_by_model(NOW, NOW + 100)

        def norm(bs):  # order-insensitive within a bucket
            return sorted((b.bucket_start, b.agent_name, b.model, b.total_cost_usd) for b in bs)

        assert norm(await sql.cost_over_time(NOW, NOW + 100, 60)) == norm(
            await mem.cost_over_time(NOW, NOW + 100, 60)
        )
        assert [(r.name, r.start_time) for r in await sql.recent_spans(NOW, NOW + 100)] == [
            (r.name, r.start_time) for r in await mem.recent_spans(NOW, NOW + 100)
        ]
    finally:
        await sql.shutdown()
        await mem.shutdown()


# --- normalize_span contract (public API, one test per span-kind) ---------


@pytest.mark.parametrize(
    ("name", "attrs", "expected_agent"),
    [
        ("civitas.agent.handle", {"civitas.agent.name": "a"}, "a"),
        ("civitas.llm.chat", {"civitas.agent.name": "a"}, "a"),
        ("llm.chat gpt-4o", {"llm.model": "gpt-4o"}, None),  # no agent identity
        ("send foo", {"civitas.sender": "s"}, "s"),
        ("recv foo", {"civitas.sender": "s"}, "s"),
        ("tool.execute", {"civitas.sender": "s"}, "s"),
        ("supervisor.restart", {"civitas.child": "c"}, "c"),
        ("some.unknown.span", {"civitas.agent.name": "a"}, None),  # unmatched -> None
    ],
)
def test_normalize_span_agent_by_kind(name: str, attrs: dict, expected_agent: str | None) -> None:
    span = SpanData(
        name=name,
        trace_id="t",
        span_id="s",
        parent_span_id=None,
        start_time=NOW,
        end_time=NOW,
        attributes=attrs,
    )
    assert normalize_span(span)["agent_name"] == expected_agent


def test_normalize_span_promotes_llm_columns() -> None:
    n = normalize_span(_llm("a", "gpt-4o", 0.5, NOW, tin=7, tout=3))
    assert n == {
        "agent_name": "a",
        "llm_model": "gpt-4o",
        "llm_tokens_in": 7,
        "llm_tokens_out": 3,
        "llm_cost_usd": 0.5,
    }


# --- InMemorySpanStore specifics ------------------------------------------


@pytest.mark.asyncio
async def test_in_memory_store_bounds_by_maxsize() -> None:
    s = InMemorySpanStore(maxsize=3)
    for i in range(10):
        await s.export([_handle("a", NOW + i)])
    rows = await s.recent_spans(NOW, NOW + 100, limit=100)
    assert len(rows) == 3  # only the 3 newest retained
    assert {r.start_time for r in rows} == {NOW + 7, NOW + 8, NOW + 9}


# --- back-compat aliases ---------------------------------------------------


def test_backcompat_aliases_resolve_to_span_store() -> None:
    from civitas.observability.sqlite_backend import SQLiteBackend, SQLiteSpanStore
    from civitas.observability.sqlite_query import (
        CostBucket,
        MessageRateBucket,
        SpanRecord,
        SQLiteQueryEngine,
    )

    assert SQLiteBackend is SQLiteSpanStore
    assert SQLiteQueryEngine is SQLiteSpanStore
    # dataclasses still importable from the old read-side path
    assert CostBucket.__name__ == "CostBucket"
    assert MessageRateBucket.__name__ == "MessageRateBucket"
    assert SpanRecord.__name__ == "SpanRecord"
