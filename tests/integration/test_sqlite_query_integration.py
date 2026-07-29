"""Integration test: SQLiteQueryEngine queried against a REAL Runtime's
output, not synthetic SpanData (v0.9.3.x, Track B, B2).

Matches this project's "verify against the real thing" standard: B1's own
integration test proves a real Runtime produces correct rows; this test
proves the query layer built on top of B1's real output returns correct
aggregates.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from civitas import AgentProcess, Supervisor
from civitas.messages import Message
from civitas.observability.sqlite_backend import SQLiteBackend
from civitas.observability.sqlite_query import SQLiteQueryEngine
from civitas.runtime import Runtime


class ChattyAgent(AgentProcess):
    def __init__(self, name: str, cost_usd: float) -> None:
        super().__init__(name)
        self._cost_usd = cost_usd

    async def handle(self, message: Message) -> Message | None:
        with self.llm_span("gpt-4o") as span:
            span.set_attribute("civitas.llm.tokens_in", 100)
            span.set_attribute("civitas.llm.tokens_out", 50)
            span.set_attribute("civitas.llm.cost_usd", self._cost_usd)
        return self.reply({"ok": True})


@pytest.mark.asyncio
async def test_query_engine_aggregates_real_runtime_output(tmp_path: Path):
    backend = SQLiteBackend(db_dir=str(tmp_path))
    agent_a = ChattyAgent("agent_a", cost_usd=0.01)
    agent_b = ChattyAgent("agent_b", cost_usd=0.02)
    runtime = Runtime(
        supervisor=Supervisor("root", children=[agent_a, agent_b]),
        exporters=[backend],
    )
    await runtime.start()
    try:
        await runtime.ask("agent_a", {"q": 1})
        await runtime.ask("agent_a", {"q": 2})
        await runtime.ask("agent_b", {"q": 1})
    finally:
        await runtime.stop()  # drains queued spans before returning

    engine = SQLiteQueryEngine(db_dir=str(tmp_path))
    import time

    now = time.time()
    by_agent = await engine.cost_by_agent(now - 60, now + 60)
    assert by_agent["agent_a"] == pytest.approx(0.02)  # two calls at 0.01 each
    assert by_agent["agent_b"] == pytest.approx(0.02)  # one call at 0.02

    by_model = await engine.cost_by_model(now - 60, now + 60)
    assert by_model["gpt-4o"] == pytest.approx(0.04)

    rate_buckets = await engine.message_rate_over_time(now - 60, now + 60, bucket_seconds=3600)
    total_messages = sum(b.message_count for b in rate_buckets)
    assert total_messages == 3  # 3 handle() calls total across both agents
