"""M2.6 — Framework Adapters testable criteria.

Tests validate that LangGraph and OpenAI Agents SDK adapters wrap external
framework agents as AgentProcesses with supervision, tracing, and messaging.

Uses mock framework objects to avoid requiring langgraph/openai-agents deps.
"""

import logging
import sys
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

# Framework adapters moved to civitas-contrib in the core/contrib split (#40):
# these tests exercise the contrib integration and skip cleanly without it.
pytest.importorskip(
    "civitas_contrib.adapters.langgraph",
    reason="civitas-contrib not installed — skipping framework-adapter integration tests",
)

from civitas_contrib.adapters.crewai import CrewAIAgent
from civitas_contrib.adapters.langgraph import LangGraphAgent
from civitas_contrib.adapters.openai import OpenAIAgent

from civitas import AgentProcess, Runtime, Supervisor
from civitas.errors import ErrorAction
from civitas.messages import Message
from civitas.process import ProcessStatus
from tests.conftest import wait_for, wait_for_status  # noqa: F401 — used via wait_for_status

# ---------------------------------------------------------------------------
# Mock LangGraph compiled graph
# ---------------------------------------------------------------------------


class MockCompiledGraph:
    """Simulates a LangGraph CompiledGraph with ainvoke()."""

    def __init__(self, fail_on_first: bool = False) -> None:
        self.invoke_count = 0
        self._fail_on_first = fail_on_first

    async def ainvoke(self, input_data: dict) -> dict:
        self.invoke_count += 1
        if self._fail_on_first and self.invoke_count == 1:
            raise RuntimeError("LangGraph node failure")
        query = input_data.get("query", "unknown")
        return {"query": query, "result": f"Processed: {query}"}


# ---------------------------------------------------------------------------
# Mock OpenAI Agents SDK
# ---------------------------------------------------------------------------


@dataclass
class MockAgent:
    """Simulates an OpenAI Agents SDK Agent."""

    name: str
    instructions: str = ""


@dataclass
class MockRunResult:
    """Simulates the result of Runner.run()."""

    final_output: str
    new_items: list[Any] = None

    def __post_init__(self):
        if self.new_items is None:
            self.new_items = []


@dataclass
class MockHandoff:
    """Simulates an OpenAI handoff item."""

    agent: MockAgent
    input: str


# ---------------------------------------------------------------------------
# LangGraph adapter tests
# ---------------------------------------------------------------------------


async def test_langgraph_agent_runs_graph():
    """LangGraph compiled graph runs inside an Agency AgentProcess."""
    graph = MockCompiledGraph()
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[
                LangGraphAgent("lg_agent", graph=graph),
            ],
        )
    )
    await runtime.start()
    try:
        result = await runtime.ask("lg_agent", {"query": "quantum"})
        assert result.payload["result"] == "Processed: quantum"
        assert result.payload["query"] == "quantum"
        assert graph.invoke_count == 1
    finally:
        await runtime.stop()


async def test_langgraph_agent_multiple_invocations():
    """LangGraph agent handles multiple messages correctly."""
    graph = MockCompiledGraph()
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[
                LangGraphAgent("lg_agent", graph=graph),
            ],
        )
    )
    await runtime.start()
    try:
        r1 = await runtime.ask("lg_agent", {"query": "topic_a"})
        r2 = await runtime.ask("lg_agent", {"query": "topic_b"})
        assert r1.payload["result"] == "Processed: topic_a"
        assert r2.payload["result"] == "Processed: topic_b"
        assert graph.invoke_count == 2
    finally:
        await runtime.stop()


async def test_langgraph_supervisor_restarts_on_failure():
    """Supervisor restarts a failed LangGraph workflow correctly."""
    graph = MockCompiledGraph(fail_on_first=True)
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[LangGraphAgent("lg_crash", graph=graph)],
            strategy="ONE_FOR_ONE",
            max_restarts=3,
            backoff="CONSTANT",
            backoff_base=0.1,
        )
    )
    await runtime.start()
    try:
        # First invocation fails → agent crashes → supervisor restarts
        with pytest.raises(TimeoutError):
            await runtime.ask("lg_crash", {"query": "fail"}, timeout=1.0)

        lg_agent = runtime.get_agent("lg_crash")
        assert lg_agent is not None
        await wait_for_status(lg_agent, ProcessStatus.RUNNING, timeout=3.0)

        # Second invocation succeeds (graph.invoke_count > 1 now)
        result = await runtime.ask("lg_crash", {"query": "recover"}, timeout=5.0)
        assert result.payload["result"] == "Processed: recover"
    finally:
        await runtime.stop()


async def test_langgraph_error_escalates():
    """LangGraph adapter maps errors to ErrorAction.ESCALATE."""
    graph = MockCompiledGraph()
    agent = LangGraphAgent("lg_test", graph=graph)
    action = await agent.on_error(RuntimeError("test"), Message(sender="x", recipient="lg_test"))
    assert action == ErrorAction.ESCALATE


async def test_langgraph_non_dict_output():
    """LangGraph adapter wraps non-dict output in {'output': ...}."""

    class StringGraph:
        async def ainvoke(self, data):
            return "plain string result"

    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[
                LangGraphAgent("lg_str", graph=StringGraph()),
            ],
        )
    )
    await runtime.start()
    try:
        result = await runtime.ask("lg_str", {"query": "test"})
        assert result.payload == {"output": "plain string result"}
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# OpenAI Agents SDK adapter tests
# ---------------------------------------------------------------------------


async def test_openai_agent_runs():
    """OpenAI Agent runs inside an Agency AgentProcess."""
    # Mock the 'agents' module since it's not installed
    agents_module = MagicMock()
    mock_runner = MagicMock()
    mock_runner.run = AsyncMock(
        return_value=MockRunResult(
            final_output="RLHF is reinforcement learning from human feedback."
        )
    )
    agents_module.Runner = mock_runner
    sys.modules["agents"] = agents_module

    try:
        mock_agent = MockAgent(name="assistant", instructions="Be helpful")
        runtime = Runtime(
            supervisor=Supervisor(
                "root",
                children=[
                    OpenAIAgent("oai_agent", agent=mock_agent),
                ],
            )
        )
        await runtime.start()
        try:
            result = await runtime.ask("oai_agent", {"input": "What is RLHF?"})
            assert result.payload["output"] == "RLHF is reinforcement learning from human feedback."
            mock_runner.run.assert_called_once()
        finally:
            await runtime.stop()
    finally:
        del sys.modules["agents"]


async def test_openai_agent_handoff_maps_to_send():
    """OpenAI Agent handoffs map to Agency messages between processes."""
    agents_module = MagicMock()

    # Create a handoff target
    target_agent = MockAgent(name="specialist")

    mock_runner = MagicMock()
    mock_runner.run = AsyncMock(
        return_value=MockRunResult(
            final_output="Delegated to specialist",
            new_items=[MockHandoff(agent=target_agent, input="handle this")],
        )
    )
    agents_module.Runner = mock_runner
    sys.modules["agents"] = agents_module

    received_by_specialist: list[Message] = []

    class Specialist(AgentProcess):
        async def handle(self, message: Message) -> Message | None:
            received_by_specialist.append(message)
            return None

    try:
        mock_agent = MockAgent(name="router")
        runtime = Runtime(
            supervisor=Supervisor(
                "root",
                children=[
                    OpenAIAgent("oai_router", agent=mock_agent),
                    Specialist("specialist"),
                ],
            )
        )
        await runtime.start()
        try:
            result = await runtime.ask("oai_router", {"input": "route this"})
            assert result.payload["output"] == "Delegated to specialist"

            # Wait for the send() to be delivered
            await wait_for(lambda: len(received_by_specialist) == 1)
            assert len(received_by_specialist) == 1
            assert received_by_specialist[0].payload["input"] == "handle this"
        finally:
            await runtime.stop()
    finally:
        del sys.modules["agents"]


async def test_openai_error_escalates():
    """OpenAI adapter maps errors to ErrorAction.ESCALATE."""
    mock_agent = MockAgent(name="test")
    agent = OpenAIAgent("oai_test", agent=mock_agent)
    action = await agent.on_error(RuntimeError("test"), Message(sender="x", recipient="oai_test"))
    assert action == ErrorAction.ESCALATE


async def test_openai_missing_input_returns_error():
    """F10-2: missing 'input' key returns error reply instead of running with empty string."""
    agents_module = MagicMock()
    sys.modules["agents"] = agents_module

    try:
        runtime = Runtime(
            supervisor=Supervisor(
                "root", children=[OpenAIAgent("oai_noinput", agent=MockAgent(name="test"))]
            )
        )
        await runtime.start()
        try:
            result = await runtime.ask("oai_noinput", {"wrong_key": "value"})
            assert "error" in result.payload
            # Runner.run should NOT have been called
            agents_module.Runner.run.assert_not_called()
        finally:
            await runtime.stop()
    finally:
        del sys.modules["agents"]


async def test_openai_unregistered_handoff_logs_warning(caplog):
    """F10-1: handoff to unregistered agent logs warning instead of crashing."""
    mock_handoff_item = MagicMock()
    mock_handoff_item.agent = MagicMock()
    mock_handoff_item.agent.name = "nonexistent_agent"
    mock_handoff_item.input = "route this"

    run_result = MockRunResult(final_output="done")
    run_result.new_items = [mock_handoff_item]

    agents_module = MagicMock()
    mock_runner = MagicMock()
    mock_runner.run = AsyncMock(return_value=run_result)
    agents_module.Runner = mock_runner
    sys.modules["agents"] = agents_module

    try:
        runtime = Runtime(
            supervisor=Supervisor(
                "root", children=[OpenAIAgent("oai_handoff", agent=MockAgent(name="test"))]
            )
        )
        await runtime.start()
        try:
            with caplog.at_level(logging.WARNING, logger="civitas.adapters.openai"):
                result = await runtime.ask("oai_handoff", {"input": "trigger handoff"})
            # Agent should return its reply, not crash
            assert result.payload["output"] == "done"
            assert "nonexistent_agent" in caplog.text
            assert "not registered" in caplog.text
        finally:
            await runtime.stop()
    finally:
        del sys.modules["agents"]


async def test_langgraph_input_schema_coercion():
    """F10-4: input_schema coerces payload before ainvoke."""

    class MyState:
        def __init__(self, query: str) -> None:
            self.query = query

    class SchemaGraph:
        async def ainvoke(self, data: object) -> dict:
            assert isinstance(data, MyState)
            return {"result": f"got: {data.query}"}

    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[LangGraphAgent("lg_schema", graph=SchemaGraph(), input_schema=MyState)],
        )
    )
    await runtime.start()
    try:
        result = await runtime.ask("lg_schema", {"query": "hello"})
        assert result.payload["result"] == "got: hello"
    finally:
        await runtime.stop()


def test_crewai_stub_raises_not_implemented():
    """F10-5: CrewAIAgent raises NotImplementedError on instantiation."""
    with pytest.raises(NotImplementedError, match="not yet implemented"):
        CrewAIAgent("test")


# ---------------------------------------------------------------------------
# Cross-adapter tests
# ---------------------------------------------------------------------------


async def test_langgraph_and_openai_coexist():
    """Both adapters can run in the same supervision tree."""
    agents_module = MagicMock()
    mock_runner = MagicMock()
    mock_runner.run = AsyncMock(return_value=MockRunResult(final_output="OpenAI says hi"))
    agents_module.Runner = mock_runner
    sys.modules["agents"] = agents_module

    try:
        graph = MockCompiledGraph()
        mock_agent = MockAgent(name="oai")
        runtime = Runtime(
            supervisor=Supervisor(
                "root",
                children=[
                    LangGraphAgent("lg", graph=graph),
                    OpenAIAgent("oai", agent=mock_agent),
                ],
            )
        )
        await runtime.start()
        try:
            r1 = await runtime.ask("lg", {"query": "test"})
            r2 = await runtime.ask("oai", {"input": "hello"})
            assert r1.payload["result"] == "Processed: test"
            assert r2.payload["output"] == "OpenAI says hi"
        finally:
            await runtime.stop()
    finally:
        del sys.modules["agents"]
