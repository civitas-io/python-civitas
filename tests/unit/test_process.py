"""Unit tests for AgentProcess, Mailbox, and message loop behaviour."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from civitas.errors import ErrorAction
from civitas.messages import Message
from civitas.observability.tracer import Span
from civitas.process import AgentProcess, Mailbox, ProcessStatus
from tests.conftest import wait_for

# ---------------------------------------------------------------------------
# Mailbox tests
# ---------------------------------------------------------------------------


async def test_mailbox_put_get_fifo():
    """Normal messages are delivered in FIFO order."""
    mb = Mailbox()
    m1 = Message(type="a")
    m2 = Message(type="b")
    await mb.put(m1)
    await mb.put(m2)
    assert (await mb.get()).type == "a"
    assert (await mb.get()).type == "b"


async def test_mailbox_priority_served_first():
    """Priority messages are served before normal messages."""
    mb = Mailbox()
    normal = Message(type="normal", priority=0)
    high = Message(type="high", priority=1)
    await mb.put(normal)
    await mb.put(high)
    assert (await mb.get()).type == "high"
    assert (await mb.get()).type == "normal"


async def test_mailbox_empty_check():
    """empty() reflects both queues."""
    mb = Mailbox()
    assert mb.empty()
    await mb.put(Message(type="x"))
    assert not mb.empty()
    await mb.get()
    assert mb.empty()


async def test_mailbox_priority_queue_bounded():
    """Priority queue has a finite bound (F02-2)."""
    mb = Mailbox(maxsize=10)
    # Priority queue maxsize is 100 — verify it has a bound by checking it exists
    assert mb._priority_queue.maxsize == 100


async def test_mailbox_expired_message_discarded(caplog: Any) -> None:
    """A message past its ttl is discarded, not delivered (F01-3)."""
    mb = Mailbox()
    expired = Message(type="expired", timestamp=0.0, ttl=1.0)  # elapsed long ago
    fresh = Message(type="fresh")
    await mb.put(expired)
    await mb.put(fresh)
    with caplog.at_level("WARNING"):
        result = await mb.get()
    assert result.type == "fresh"
    assert any("discarding expired message" in r.message for r in caplog.records)


async def test_mailbox_no_ttl_never_expires():
    """A message with ttl=None (the default) is never discarded (F01-3)."""
    mb = Mailbox()
    msg = Message(type="no-ttl", timestamp=0.0)
    await mb.put(msg)
    assert (await mb.get()).type == "no-ttl"


async def test_mailbox_ttl_not_yet_elapsed_delivered():
    """A message within its ttl window is delivered normally (F01-3)."""
    import time

    mb = Mailbox()
    msg = Message(type="fresh", timestamp=time.time(), ttl=60.0)
    await mb.put(msg)
    assert (await mb.get()).type == "fresh"


# ---------------------------------------------------------------------------
# ProcessStatus — SUSPENDED re-introduced fully-wired (F02-6)
# ---------------------------------------------------------------------------


def test_suspended_present_in_enum():
    """SUSPENDED is in ProcessStatus (F02-6: removed as dead API, re-added fully wired)."""
    names = [s.name for s in ProcessStatus]
    assert "SUSPENDED" in names


def test_expected_states_present():
    """All expected states are present — the 5 originals plus SUSPENDED (F02-6)."""
    names = {s.name for s in ProcessStatus}
    assert names == {"INITIALIZING", "RUNNING", "SUSPENDED", "STOPPING", "STOPPED", "CRASHED"}


# ---------------------------------------------------------------------------
# Lifecycle hooks
# ---------------------------------------------------------------------------


class TrackingAgent(AgentProcess):
    """Agent that records lifecycle events and received messages."""

    def __init__(self, name: str = "tracker") -> None:
        super().__init__(name)
        self.events: list[str] = []
        self.received: list[Message] = []

    async def on_start(self) -> None:
        self.events.append("start")

    async def handle(self, message: Message) -> Message | None:
        self.events.append(f"handle:{message.type}")
        self.received.append(message)
        return None

    async def on_stop(self) -> None:
        self.events.append("stop")


async def _start_and_stop(agent: AgentProcess) -> None:
    await agent._start()
    await agent._stop()


async def test_on_start_called_before_first_message():
    """on_start() is called once before handle()."""
    agent = TrackingAgent()
    await _start_and_stop(agent)
    assert "start" in agent.events
    assert agent.events.index("start") == 0


async def test_on_stop_called_on_graceful_shutdown():
    """on_stop() is called after graceful shutdown (F02-1)."""
    agent = TrackingAgent()
    await _start_and_stop(agent)
    assert "stop" in agent.events
    assert agent.status == ProcessStatus.STOPPED


async def test_on_stop_called_when_on_start_raises():
    """on_stop() runs even when on_start() raises (F11-5).

    The message loop never starts in this case, so _start() runs the
    equivalent cleanup itself before re-raising the original exception.
    """

    class CrashingAgent(AgentProcess):
        def __init__(self) -> None:
            super().__init__("crasher")
            self.stop_called = False

        async def on_start(self) -> None:
            raise RuntimeError("on_start crash")

        async def on_stop(self) -> None:
            self.stop_called = True

    agent = CrashingAgent()
    # on_start crash propagates — _start() should raise
    with pytest.raises(RuntimeError, match="on_start crash"):
        await agent._start()
    assert agent.stop_called, "on_stop must be called even when on_start() raises"
    assert agent.status == ProcessStatus.CRASHED


async def test_on_stop_called_on_crash():
    """on_stop() is always called — even when the agent crashes (F02-1)."""

    # Second scenario: crash during handle()
    class HandleCrashAgent(AgentProcess):
        def __init__(self) -> None:
            super().__init__("handle_crasher")
            self.stop_called = False

        async def handle(self, message: Message) -> None:
            raise RuntimeError("handle crash")

        async def on_error(self, error: Exception, message: Message) -> ErrorAction:
            return ErrorAction.ESCALATE

        async def on_stop(self) -> None:
            self.stop_called = True

    agent2 = HandleCrashAgent()
    await agent2._start()
    await agent2._mailbox.put(Message(type="trigger"))
    # Wait for the loop to crash
    if agent2._task is not None:
        try:
            await asyncio.wait_for(agent2._task, timeout=2.0)
        except (TimeoutError, RuntimeError):
            pass
    assert agent2.stop_called, "on_stop must be called even when agent crashes"
    assert agent2.status == ProcessStatus.CRASHED


async def test_status_transitions():
    """Status follows INITIALIZING → RUNNING → STOPPING → STOPPED."""
    agent = TrackingAgent()
    assert agent.status == ProcessStatus.INITIALIZING
    await agent._start()
    assert agent.status == ProcessStatus.RUNNING
    await agent._stop()
    assert agent.status == ProcessStatus.STOPPED


# ---------------------------------------------------------------------------
# ErrorAction
# ---------------------------------------------------------------------------


async def test_retry_redelivers_message():
    """RETRY puts the message back in the mailbox (F02-3)."""

    class RetryAgent(AgentProcess):
        def __init__(self) -> None:
            super().__init__("retrier", max_retries=2)
            self.attempts: list[int] = []

        async def handle(self, message: Message) -> None:
            self.attempts.append(message.attempt)
            raise ValueError("transient")

        async def on_error(self, error: Exception, message: Message) -> ErrorAction:
            if message.attempt < 2:
                return ErrorAction.RETRY
            return ErrorAction.SKIP

    agent = RetryAgent()
    await agent._start()
    await agent._mailbox.put(Message(type="work"))
    await wait_for(lambda: len(agent.attempts) >= 2)
    assert agent.status == ProcessStatus.RUNNING  # SKIP kept it running
    await agent._stop()


async def test_retry_increments_attempt():
    """RETRY increments message.attempt on each re-delivery."""

    class AttemptLogger(AgentProcess):
        def __init__(self) -> None:
            super().__init__("attempt_logger", max_retries=3)
            self.seen_attempts: list[int] = []

        async def handle(self, message: Message) -> None:
            self.seen_attempts.append(message.attempt)
            if message.attempt < 2:
                raise ValueError("retry me")

        async def on_error(self, error: Exception, message: Message) -> ErrorAction:
            return ErrorAction.RETRY

    agent = AttemptLogger()
    await agent._start()
    await agent._mailbox.put(Message(type="work"))
    await wait_for(lambda: 1 in agent.seen_attempts)
    assert 0 in agent.seen_attempts
    await agent._stop()


async def test_retry_limit_escalates_after_max():
    """Exceeding max_retries escalates instead of looping forever (F02-3)."""

    class AlwaysFailAgent(AgentProcess):
        def __init__(self) -> None:
            super().__init__("always_fail", max_retries=2)

        async def handle(self, message: Message) -> None:
            raise ValueError("always fails")

        async def on_error(self, error: Exception, message: Message) -> ErrorAction:
            return ErrorAction.RETRY

    agent = AlwaysFailAgent()
    await agent._start()
    await agent._mailbox.put(Message(type="work"))
    if agent._task is not None:
        try:
            await asyncio.wait_for(agent._task, timeout=2.0)
        except (TimeoutError, ValueError):
            pass
    assert agent.status in (ProcessStatus.CRASHED, ProcessStatus.STOPPED)


async def test_skip_discards_message():
    """SKIP discards the failed message and continues processing."""

    class SkipAgent(AgentProcess):
        def __init__(self) -> None:
            super().__init__("skipper")
            self.processed: list[str] = []

        async def handle(self, message: Message) -> None:
            if message.type == "bad":
                raise ValueError("skip me")
            self.processed.append(message.type)

        async def on_error(self, error: Exception, message: Message) -> ErrorAction:
            return ErrorAction.SKIP

    agent = SkipAgent()
    await agent._start()
    await agent._mailbox.put(Message(type="bad"))
    await agent._mailbox.put(Message(type="good"))
    await wait_for(lambda: "good" in agent.processed)
    assert "good" in agent.processed
    assert agent.status == ProcessStatus.RUNNING
    await agent._stop()


async def test_stop_error_action_stops_gracefully():
    """STOP error action transitions to STOPPING."""

    class StopOnErrorAgent(AgentProcess):
        def __init__(self) -> None:
            super().__init__("stopper")

        async def handle(self, message: Message) -> None:
            raise ValueError("stop please")

        async def on_error(self, error: Exception, message: Message) -> ErrorAction:
            return ErrorAction.STOP

    agent = StopOnErrorAgent()
    await agent._start()
    await agent._mailbox.put(Message(type="trigger"))
    await wait_for(lambda: agent.status in (ProcessStatus.STOPPING, ProcessStatus.STOPPED))


async def test_escalate_crashes_process():
    """ESCALATE sets status to CRASHED and propagates the exception."""

    class EscalateAgent(AgentProcess):
        def __init__(self) -> None:
            super().__init__("escalater")

        async def handle(self, message: Message) -> None:
            raise RuntimeError("escalate me")

        async def on_error(self, error: Exception, message: Message) -> ErrorAction:
            return ErrorAction.ESCALATE

    agent = EscalateAgent()
    await agent._start()
    await agent._mailbox.put(Message(type="trigger"))
    if agent._task is not None:
        try:
            await asyncio.wait_for(agent._task, timeout=2.0)
        except (TimeoutError, RuntimeError):
            pass
    assert agent.status == ProcessStatus.CRASHED


# ---------------------------------------------------------------------------
# Messaging
# ---------------------------------------------------------------------------


def test_reply_outside_handle_raises():
    """reply() raises RuntimeError when called outside of handle()."""
    agent = TrackingAgent()
    with pytest.raises(RuntimeError, match="outside of handle"):
        agent.reply({"type": "reply"})


async def test_send_requires_bus():
    """send() raises RuntimeError when bus is not injected."""
    agent = TrackingAgent()
    with pytest.raises(RuntimeError, match="not wired"):
        await agent.send("someone", {})


async def test_emit_outside_handle_raises():
    """emit() raises RuntimeError when called outside of handle() (v0.9.1 top-up)."""
    agent = TrackingAgent()
    with pytest.raises(RuntimeError, match="outside of handle"):
        await agent.emit({"chunk": 1})


async def test_end_stream_outside_handle_raises():
    """end_stream() raises RuntimeError when called outside of handle()."""
    agent = TrackingAgent()
    with pytest.raises(RuntimeError, match="outside of handle"):
        await agent.end_stream()


# ---------------------------------------------------------------------------
# Configurable shutdown timeout (F02-10)
# ---------------------------------------------------------------------------


def test_configurable_shutdown_timeout():
    """shutdown_timeout param is stored on the agent."""
    agent = AgentProcess("myagent", shutdown_timeout=5.0)
    assert agent._shutdown_timeout == 5.0


def test_default_shutdown_timeout():
    """Default shutdown timeout is 30 seconds."""
    agent = AgentProcess("myagent")
    assert agent._shutdown_timeout == 30.0


def test_llm_span_no_tracer_yields_dummy():
    """llm_span() yields a dummy Span when no tracer is attached."""
    agent = TrackingAgent()
    with agent.llm_span("claude-sonnet") as span:
        assert isinstance(span, Span)


def test_tool_span_no_tracer_yields_dummy():
    """tool_span() yields a dummy Span when no tracer is attached."""
    agent = TrackingAgent()
    with agent.tool_span("web_search") as span:
        assert isinstance(span, Span)


def test_llm_span_with_tracer_records_attributes_and_parents(monkeypatch: Any) -> None:
    """With a real tracer, llm_span() creates a span parented to the current
    handle span (v0.9.1 coverage top-up), carrying the model + extra attrs.

    v0.9.3 (A1): trace_id/span_id fixtures must be real OTEL-shaped hex (32/16
    hex chars, matching _new_span_id()/os.urandom(16).hex()'s real output
    shape) -- since Tracer._make_span() now builds a genuine OTEL parent
    Context from these values (fixing OTEL spans never linking to each other
    at all, confirmed live -- docs/milestones.md v0.9.3 A1), a non-hex
    placeholder like the old "trace-1"/"msg-span" strings has no valid parent
    to extract and gets silently replaced with a fresh OTEL-minted trace_id
    (a DELIBERATE, documented fallback, not a bug) -- which would make this
    test assert on an OTEL implementation detail instead of civitas's own
    parent-linkage contract. handle_span is built exactly like
    AgentProcess._dispatch() really builds it (trace_id + parent_span_id
    together, from the current message) so the fixture actually exercises
    the real fix instead of a case it doesn't apply to.
    """
    from civitas.observability.tracer import Tracer

    agent = TrackingAgent()
    agent._tracer = Tracer()
    agent._current_message = Message(
        type="m",
        sender="x",
        trace_id="a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
        span_id="1122334455667788",
    )
    handle_span = agent._tracer.start_span(
        "civitas.agent.handle",
        trace_id=agent._current_message.trace_id,
        parent_span_id=agent._current_message.span_id,
    )
    agent._current_handle_span = handle_span

    with agent.llm_span("claude-sonnet", tokens_in=1200) as span:
        assert span.trace_id == "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
        assert span.parent_span_id == handle_span.span_id
        assert span.attributes["civitas.llm.model"] == "claude-sonnet"
        assert span.attributes["tokens_in"] == 1200


def test_llm_span_with_tracer_no_current_handle_span_falls_back_to_message() -> None:
    """Outside handle()'s own span (e.g. a background task), llm_span() still
    parents to the current message's span if one is set.

    v0.9.3 (A1): real hex fixtures -- see the docstring on
    test_llm_span_with_tracer_records_attributes_and_parents for why.
    """
    from civitas.observability.tracer import Tracer

    agent = TrackingAgent()
    agent._tracer = Tracer()
    agent._current_message = Message(
        type="m",
        sender="x",
        trace_id="b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5",
        span_id="2233445566778899",
    )

    with agent.llm_span("claude-sonnet") as span:
        assert span.trace_id == "b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5"
        assert span.parent_span_id == "2233445566778899"


def test_llm_span_records_exception_and_reraises() -> None:
    """An exception inside the llm_span() block sets the span's error and
    propagates unchanged."""
    from civitas.observability.tracer import Tracer

    agent = TrackingAgent()
    agent._tracer = Tracer()

    with pytest.raises(ValueError, match="boom"):
        with agent.llm_span("claude-sonnet"):
            raise ValueError("boom")


def test_llm_span_reports_usage_to_metrics_sink() -> None:
    """v0.9.1 (D-DASH-5, closes FD-01): a span that reports usage produces
    exactly one llm_call() with the right values and model."""
    from civitas.observability.tracer import Tracer

    agent = TrackingAgent()
    agent._tracer = Tracer()
    agent._metrics = MagicMock()

    with agent.llm_span("claude-sonnet") as span:
        span.set_attribute("civitas.llm.tokens_in", 100)
        span.set_attribute("civitas.llm.tokens_out", 50)
        span.set_attribute("civitas.llm.cost_usd", 0.02)

    agent._metrics.llm_call.assert_called_once_with("tracker", 100, 50, 0.02, model="claude-sonnet")


def test_llm_span_reports_nothing_when_no_usage_set() -> None:
    """v0.9.1 (D-DASH-5): a span that never reports usage produces ZERO
    llm_call()s — no spurious zero-cost entry for every LLM span ever opened."""
    from civitas.observability.tracer import Tracer

    agent = TrackingAgent()
    agent._tracer = Tracer()
    agent._metrics = MagicMock()

    with agent.llm_span("claude-sonnet"):
        pass  # never sets tokens_in/tokens_out/cost_usd

    agent._metrics.llm_call.assert_not_called()


def test_llm_span_reports_metrics_even_without_a_tracer() -> None:
    """v0.9.1 (D-DASH-5): metrics and tracing are independent concerns — a
    dashboard-only setup (metrics attached, no tracer configured) still gets
    cost/token tracking."""
    agent = TrackingAgent()
    assert agent._tracer is None
    agent._metrics = MagicMock()

    with agent.llm_span("claude-sonnet") as span:
        span.set_attribute("civitas.llm.tokens_in", 10)

    agent._metrics.llm_call.assert_called_once_with("tracker", 10, 0, 0.0, model="claude-sonnet")


def test_llm_span_reports_metrics_despite_exception() -> None:
    """v0.9.1 (D-DASH-5): metrics recording happens in the finally block —
    independent of the exception path already covered by set_error."""
    from civitas.observability.tracer import Tracer

    agent = TrackingAgent()
    agent._tracer = Tracer()
    agent._metrics = MagicMock()

    with pytest.raises(ValueError, match="boom"):
        with agent.llm_span("claude-sonnet") as span:
            span.set_attribute("civitas.llm.cost_usd", 0.01)
            raise ValueError("boom")

    agent._metrics.llm_call.assert_called_once_with("tracker", 0, 0, 0.01, model="claude-sonnet")


def test_llm_span_end_to_end_with_real_metrics_collector() -> None:
    """v0.9.1 (D-DASH-5): real MetricsCollector, not a mock — the actual FD-01
    close-out, proving the whole chain (span attribute -> llm_call -> snapshot
    -> last_model) works together, not just that the call was made."""
    from civitas.dashboard.collector import MetricsCollector

    agent = TrackingAgent()
    agent._metrics = MetricsCollector()

    with agent.llm_span("gpt-5") as span:
        span.set_attribute("civitas.llm.tokens_in", 200)
        span.set_attribute("civitas.llm.tokens_out", 80)
        span.set_attribute("civitas.llm.cost_usd", 0.05)

    metrics = agent._metrics.snapshot.agents["tracker"]
    assert metrics.tokens_in == 200
    assert metrics.tokens_out == 80
    assert metrics.cost_usd == 0.05
    assert metrics.last_model == "gpt-5"
    assert agent._metrics.snapshot.total_cost_usd == 0.05


def test_tool_span_with_tracer_records_attributes_and_parents() -> None:
    """tool_span() mirrors llm_span()'s tracer-present behavior.

    v0.9.3 (A1): real hex fixtures -- see the docstring on
    test_llm_span_with_tracer_records_attributes_and_parents for why.
    """
    from civitas.observability.tracer import Tracer

    agent = TrackingAgent()
    agent._tracer = Tracer()
    agent._current_message = Message(
        type="m",
        sender="x",
        trace_id="c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6",
        span_id="3344556677889900",
    )

    with agent.tool_span("web_search", query="civitas") as span:
        assert span.trace_id == "c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6"
        assert span.attributes["civitas.tool.name"] == "web_search"
        assert span.attributes["query"] == "civitas"


def test_tool_span_records_exception_and_reraises() -> None:
    from civitas.observability.tracer import Tracer

    agent = TrackingAgent()
    agent._tracer = Tracer()

    with pytest.raises(RuntimeError, match="tool boom"):
        with agent.tool_span("web_search"):
            raise RuntimeError("tool boom")


# ---------------------------------------------------------------------------
# handle() default implementation
# ---------------------------------------------------------------------------


async def test_handle_default_returns_none():
    """AgentProcess.handle() default implementation returns None (line 146)."""
    agent = AgentProcess("bare")
    result = await agent.handle(Message(type="ping"))
    assert result is None


# ---------------------------------------------------------------------------
# Checkpoint with store
# ---------------------------------------------------------------------------


async def test_checkpoint_with_store_persists_state():
    """checkpoint() calls store.set() when a store is configured (line 170)."""
    agent = TrackingAgent()
    mock_store = AsyncMock()
    agent.store = mock_store
    agent.state = {"key": "value"}

    await agent.checkpoint()

    mock_store.set.assert_awaited_once_with("tracker", {"key": "value"})


async def test_checkpoint_without_store_is_noop():
    """checkpoint() is a no-op when store is None (branch 169->exit)."""
    agent = TrackingAgent()
    # store is None by default
    await agent.checkpoint()  # must not raise


# ---------------------------------------------------------------------------
# send() and ask() with current_message context
# ---------------------------------------------------------------------------


async def test_send_propagates_trace_from_current_message():
    """send() uses trace_id/span_id from _current_message (lines 194-196)."""
    agent = TrackingAgent()
    mock_bus = MagicMock()
    mock_bus.route = AsyncMock()
    agent._bus = mock_bus

    # Set a current message to exercise the trace propagation branch
    agent._current_message = Message(
        type="incoming", sender="other", trace_id="trace-abc", span_id="span-xyz"
    )
    await agent.send("somewhere", {"data": 1})

    call_args = mock_bus.route.call_args[0][0]
    assert call_args.trace_id == "trace-abc"
    assert call_args.parent_span_id == "span-xyz"


async def test_send_without_current_message_uses_empty_trace():
    """send() with no _current_message uses empty trace (branch 194->198)."""
    agent = TrackingAgent()
    mock_bus = MagicMock()
    mock_bus.route = AsyncMock()
    agent._bus = mock_bus
    # _current_message is None by default

    await agent.send("somewhere", {"data": 1})

    call_args = mock_bus.route.call_args[0][0]
    assert call_args.trace_id == ""
    assert call_args.parent_span_id is None


async def test_ask_requires_bus():
    """ask() raises RuntimeError when bus is not injected (line 218)."""
    agent = TrackingAgent()
    with pytest.raises(RuntimeError, match="not wired"):
        await agent.ask("target", {})


async def test_ask_propagates_trace_from_current_message():
    """ask() uses trace_id/span_id from _current_message (lines 221-223)."""
    agent = TrackingAgent()
    mock_bus = MagicMock()
    reply_msg = Message(type="reply", sender="target", recipient="tracker")
    mock_bus.request = AsyncMock(return_value=reply_msg)
    agent._bus = mock_bus

    agent._current_message = Message(
        type="incoming", sender="other", trace_id="trace-ask", span_id="span-ask"
    )
    result = await agent.ask("target", {"q": 1})

    sent = mock_bus.request.call_args[0][0]
    assert sent.trace_id == "trace-ask"
    assert sent.parent_span_id == "span-ask"
    assert result is reply_msg


async def test_ask_without_current_message_uses_empty_trace():
    """ask() with no _current_message uses empty trace (branch 221->225)."""
    agent = TrackingAgent()
    mock_bus = MagicMock()
    reply_msg = Message(type="reply", sender="target", recipient="tracker")
    mock_bus.request = AsyncMock(return_value=reply_msg)
    agent._bus = mock_bus
    # _current_message is None by default

    await agent.ask("target", {"q": 1})

    sent = mock_bus.request.call_args[0][0]
    assert sent.trace_id == ""
    assert sent.parent_span_id is None


async def test_broadcast_requires_bus():
    """broadcast() raises RuntimeError when bus is not injected (line 241)."""
    agent = TrackingAgent()
    with pytest.raises(RuntimeError, match="not wired"):
        await agent.broadcast("*", {})


# ---------------------------------------------------------------------------
# Heartbeat auto-response
# ---------------------------------------------------------------------------


async def test_heartbeat_auto_response():
    """_agency.heartbeat messages receive an _agency.heartbeat_ack reply (lines 372-379)."""
    agent = TrackingAgent()
    mock_bus = MagicMock()
    mock_bus.route = AsyncMock()
    agent._bus = mock_bus

    await agent._start()
    hb = Message(
        type="_agency.heartbeat",
        sender="supervisor",
        recipient="tracker",
        reply_to="supervisor",
        correlation_id="hb-1",
    )
    await agent._mailbox.put(hb)
    await wait_for(lambda: mock_bus.route.called)

    routed = mock_bus.route.call_args[0][0]
    assert routed.type == "_agency.heartbeat_ack"
    assert routed.correlation_id == "hb-1"
    await agent._stop()


async def test_heartbeat_without_bus_continues_loop():
    """Heartbeat with no bus still continues (branch 371->380: bus is None)."""
    agent = TrackingAgent()
    # _bus intentionally left as None

    await agent._start()
    hb = Message(
        type="_agency.heartbeat",
        sender="supervisor",
        recipient="tracker",
        correlation_id="hb-2",
    )
    await agent._mailbox.put(hb)
    # Send a normal message after the heartbeat so we know the loop continued
    await agent._mailbox.put(Message(type="ping"))
    await wait_for(lambda: "handle:ping" in agent.events)
    await agent._stop()


async def test_stop_noop_when_never_started():
    """_stop() is a no-op when the agent was never started (branch 491->exit)."""
    agent = TrackingAgent()
    # _task is None, _status is INITIALIZING
    await agent._stop()
    # No exception — idempotent


# ---------------------------------------------------------------------------
# MetricsSink wiring (FD-01/FD-03)
# ---------------------------------------------------------------------------


class _FakeMetricsSink:
    """Records calls without depending on the dashboard's MetricsCollector."""

    def __init__(self) -> None:
        self.handled: list[tuple[str, float]] = []
        self.sent: list[str] = []
        self.errors: list[str] = []

    def message_handled(self, agent_name: str, latency_ms: float) -> None:
        self.handled.append((agent_name, latency_ms))

    def message_sent(self, agent_name: str) -> None:
        self.sent.append(agent_name)

    def agent_error(self, agent_name: str) -> None:
        self.errors.append(agent_name)

    def agent_restarted(self, agent_name: str, reason: str = "") -> None:
        pass


async def test_message_handled_recorded_on_success():
    """A successful handle() call records message_handled with latency (FD-01)."""
    agent = TrackingAgent()
    sink = _FakeMetricsSink()
    agent._metrics = sink

    await agent._start()
    await agent._mailbox.put(Message(type="ping"))
    await wait_for(lambda: len(sink.handled) >= 1)
    await agent._stop()

    name, latency_ms = sink.handled[0]
    assert name == "tracker"
    assert latency_ms >= 0.0


async def test_fake_sink_without_agent_status_changed_never_crashes():
    """v0.9.3.1: _FakeMetricsSink above does NOT implement
    agent_status_changed() -- deliberately, since it's NOT part of the
    MetricsSink Protocol (only message_handled/message_sent/agent_error/
    agent_restarted/llm_call are required). A real custom sink implementing
    only the required methods must keep working through every status
    transition an agent's full start/stop lifecycle produces, not crash the
    first time one occurs."""
    agent = TrackingAgent()
    sink = _FakeMetricsSink()
    agent._metrics = sink

    await agent._start()  # INITIALIZING -> RUNNING transitions happen here
    await agent._stop()  # RUNNING -> STOPPING -> STOPPED transitions happen here
    # No AttributeError anywhere above -- that's the whole assertion.


async def test_agent_status_changed_called_on_real_metrics_collector():
    """v0.9.3.1: MetricsCollector.agent_status_changed() -- defined since
    v0.9.1 but never called from anywhere in the runtime until now (found
    live: a plainly-running agent's exposed status came back "unknown",
    AgentMetrics's own never-overwritten default, while building the
    Prometheus /metrics route). Real AgentProcess lifecycle, real
    MetricsCollector -- not a hand-rolled fake -- proving the actual wiring,
    not just that _set_status() calls SOME method with SOME name."""
    from civitas.dashboard.collector import MetricsCollector
    from civitas.process import ProcessStatus

    agent = TrackingAgent()
    collector = MetricsCollector()
    agent._metrics = collector

    await agent._start()
    await wait_for(lambda: collector.snapshot.agents["tracker"].status == "RUNNING")
    assert agent.status == ProcessStatus.RUNNING

    await agent._stop()
    assert collector.snapshot.agents["tracker"].status == "STOPPED"


async def test_agent_error_and_message_handled_recorded_on_failure():
    """A raising handle() call records both agent_error and message_handled (FD-01)."""

    class AlwaysFailAgent(AgentProcess):
        def __init__(self) -> None:
            super().__init__("failer")

        async def handle(self, message: Message) -> None:
            raise ValueError("boom")

        async def on_error(self, error: Exception, message: Message) -> ErrorAction:
            return ErrorAction.SKIP

    agent = AlwaysFailAgent()
    sink = _FakeMetricsSink()
    agent._metrics = sink

    await agent._start()
    await agent._mailbox.put(Message(type="ping"))
    await wait_for(lambda: len(sink.errors) >= 1)
    assert sink.errors == ["failer"]
    assert len(sink.handled) == 1
    await agent._stop()


async def test_message_sent_recorded_on_send():
    """send() records message_sent for the sending agent (FD-01)."""
    agent = TrackingAgent()
    sink = _FakeMetricsSink()
    agent._metrics = sink
    mock_bus = MagicMock()
    mock_bus.route = AsyncMock()
    agent._bus = mock_bus

    await agent.send("other", {"key": "value"})

    assert sink.sent == ["tracker"]


async def test_message_sent_recorded_on_ask():
    """ask() records message_sent for the asking agent (FD-01)."""
    agent = TrackingAgent()
    sink = _FakeMetricsSink()
    agent._metrics = sink
    mock_bus = MagicMock()
    mock_bus.request = AsyncMock(return_value=Message(type="reply"))
    agent._bus = mock_bus

    await agent.ask("other", {"key": "value"})

    assert sink.sent == ["tracker"]


async def test_no_metrics_sink_does_not_raise():
    """AgentProcess works normally when no metrics sink is attached (default)."""
    agent = TrackingAgent()
    assert agent._metrics is None

    await agent._start()
    await agent._mailbox.put(Message(type="ping"))
    await wait_for(lambda: "handle:ping" in agent.events)
    await agent._stop()


# ---------------------------------------------------------------------------
# connect_mcp — MCP support lives in fabrica, not core (v0.9.1 top-up)
# ---------------------------------------------------------------------------


class _FakeConfig:
    def __init__(self, name: str = "srv") -> None:
        self.name = name


async def test_connect_mcp_without_fabrica_raises_configuration_error():
    """fabrica is not installed in core CI (by design — MCP is a fabrica-scope
    dependency, not a civitas extra); connect_mcp() must fail loud with
    install instructions, not an opaque ImportError."""
    from civitas.errors import ConfigurationError

    agent = TrackingAgent()
    with pytest.raises(ConfigurationError, match="pip install fabrica"):
        await agent.connect_mcp(_FakeConfig())


async def test_connect_mcp_disconnects_existing_client_and_tools_first():
    """Idempotent reconnect: an existing client for the same server name is
    torn down (deregistering its tools) before the (here: failing, no
    fabrica) reconnect attempt — the disconnect-then-reconnect ordering is
    real behavior, independent of fabrica being installed."""
    from civitas.errors import ConfigurationError
    from civitas.plugins.tools import ToolRegistry

    agent = TrackingAgent()
    agent.tools = ToolRegistry()

    class _FakeTool:
        name = "mcp://srv/lookup"
        schema: dict[str, object] = {}

        async def execute(self, **_kw: object) -> None:
            return None

    agent.tools.register(_FakeTool())

    disconnected = []

    class _FakeExistingClient:
        async def disconnect(self) -> None:
            disconnected.append(True)

    agent._mcp_clients["srv"] = _FakeExistingClient()

    with pytest.raises(ConfigurationError):
        await agent.connect_mcp(_FakeConfig("srv"))

    assert disconnected == [True]
    assert agent.tools.get("mcp://srv/lookup") is None  # deregistered by prefix


async def test_connect_mcp_swallows_disconnect_failure_on_existing_client():
    """A raising disconnect() on the OLD client must not prevent the
    reconnect attempt — it's swallowed, matching the docstring's 'idempotent'
    contract."""
    from civitas.errors import ConfigurationError

    agent = TrackingAgent()

    class _FailingExistingClient:
        async def disconnect(self) -> None:
            raise RuntimeError("already gone")

    agent._mcp_clients["srv"] = _FailingExistingClient()

    # Must reach the (fabrica-absent) ConfigurationError, not RuntimeError.
    with pytest.raises(ConfigurationError):
        await agent.connect_mcp(_FakeConfig("srv"))
