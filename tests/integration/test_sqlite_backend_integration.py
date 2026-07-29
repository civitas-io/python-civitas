"""Integration test: a real Runtime with exporters=[SQLiteBackend(...)]
running real agents, verified by directly querying the actual .db file --
not mocking aiosqlite (v0.9.3.x, Track B, B1).

Matches this project's "verify against the real thing" standard (see
docs/design/telemetry-native.md §9): a fake/mock ExportBackend proves the
Runtime->OTELAgent wiring works (already covered by
tests/unit/test_runtime.py::TestExportersWiring); this test proves
SQLiteBackend ITSELF, wired into a real Runtime, produces correct rows a
real query can read back.
"""

from __future__ import annotations

from pathlib import Path

import aiosqlite
import pytest

from civitas import AgentProcess, Supervisor
from civitas.messages import Message
from civitas.observability.sqlite_backend import SQLiteBackend
from civitas.runtime import Runtime


class EchoAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"ok": True})


class ChattyAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        with self.llm_span("gpt-4o") as span:
            span.set_attribute("civitas.llm.tokens_in", 100)
            span.set_attribute("civitas.llm.tokens_out", 50)
            span.set_attribute("civitas.llm.cost_usd", 0.0123)
        return self.reply({"ok": True})


@pytest.mark.asyncio
async def test_real_runtime_with_sqlite_backend_produces_queryable_rows(tmp_path: Path):
    backend = SQLiteBackend(db_dir=str(tmp_path))
    agent = EchoAgent("echo")
    runtime = Runtime(supervisor=Supervisor("root", children=[agent]), exporters=[backend])
    await runtime.start()
    try:
        await runtime.ask("echo", {"q": 1})
    finally:
        await runtime.stop()  # drains any spans still queued before returning

    files = backend.list_window_files()
    assert len(files) == 1

    conn = await aiosqlite.connect(str(files[0]))
    try:
        cursor = await conn.execute("SELECT name, agent_name FROM spans WHERE agent_name = 'echo'")
        rows = await cursor.fetchall()
    finally:
        await conn.close()

    names = {row[0] for row in rows}
    assert "recv message" in names or "send reply" in names or "civitas.agent.handle" in names
    assert len(rows) > 0


@pytest.mark.asyncio
async def test_real_runtime_llm_cost_lands_in_the_promoted_column(tmp_path: Path):
    """The actual cost-tracking value proposition (docs/design/telemetry-native.md
    §1) -- a real LLM span's cost_usd must be queryable as a real SQL column,
    not buried in a JSON blob."""
    backend = SQLiteBackend(db_dir=str(tmp_path))
    agent = ChattyAgent("chatty")
    runtime = Runtime(supervisor=Supervisor("root", children=[agent]), exporters=[backend])
    await runtime.start()
    try:
        await runtime.ask("chatty", {"q": 1})
    finally:
        await runtime.stop()

    files = backend.list_window_files()
    conn = await aiosqlite.connect(str(files[0]))
    try:
        cursor = await conn.execute(
            "SELECT agent_name, llm_model, llm_tokens_in, llm_tokens_out, llm_cost_usd "
            "FROM spans WHERE llm_cost_usd IS NOT NULL"
        )
        rows = await cursor.fetchall()
    finally:
        await conn.close()

    assert len(rows) == 1
    agent_name, llm_model, tokens_in, tokens_out, cost_usd = rows[0]
    assert agent_name == "chatty"
    assert llm_model == "gpt-4o"
    assert tokens_in == 100
    assert tokens_out == 50
    assert cost_usd == 0.0123


@pytest.mark.asyncio
async def test_real_runtime_shutdown_closes_sqlite_connections_cleanly(tmp_path: Path):
    """runtime.stop() drains OTELAgent, which calls backend.shutdown() --
    confirms no dangling open aiosqlite connections/warnings after a normal
    Runtime shutdown."""
    backend = SQLiteBackend(db_dir=str(tmp_path))
    agent = EchoAgent("echo")
    runtime = Runtime(supervisor=Supervisor("root", children=[agent]), exporters=[backend])
    await runtime.start()
    await runtime.ask("echo", {"q": 1})
    await runtime.stop()

    assert backend._connections == {}
