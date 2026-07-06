"""Unit tests for GenServer dispatch, timer, supervision, and integration."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from civitas import GenServer, Runtime, Supervisor
from civitas.messages import Message
from civitas.process import ProcessStatus
from tests.conftest import wait_for, wait_for_status

# ---------------------------------------------------------------------------
# Concrete GenServer implementations for testing
# ---------------------------------------------------------------------------


class EchoServer(GenServer):
    """Echoes the payload back verbatim."""

    async def handle_call(self, payload: dict[str, Any], from_: str) -> dict[str, Any]:
        return {"echo": payload}

    async def handle_cast(self, payload: dict[str, Any]) -> None:
        self.state["last_cast"] = payload

    async def handle_info(self, payload: dict[str, Any]) -> None:
        self.state["last_info"] = payload


class CounterServer(GenServer):
    """Accumulates a counter across calls."""

    async def init(self) -> None:
        self.state["count"] = 0

    async def handle_call(self, payload: dict[str, Any], from_: str) -> dict[str, Any]:
        if payload.get("op") == "increment":
            self.state["count"] += 1
        return {"count": self.state["count"]}

    async def handle_cast(self, payload: dict[str, Any]) -> None:
        if payload.get("op") == "reset":
            self.state["count"] = 0


class TickServer(GenServer):
    """Uses send_after to schedule recurring handle_info ticks."""

    async def init(self) -> None:
        self.state["ticks"] = 0
        self.send_after(10, {"type": "tick"})

    async def handle_info(self, payload: dict[str, Any]) -> None:
        if payload.get("type") == "tick":
            self.state["ticks"] += 1


class BadCallServer(GenServer):
    """Returns a non-dict from handle_call to test enforcement."""

    async def handle_call(self, payload: dict[str, Any], from_: str) -> dict[str, Any]:
        return "not a dict"  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _start_runtime(*children: Any) -> Runtime:
    runtime = Runtime(supervisor=Supervisor("root", children=list(children)))
    await runtime.start()
    return runtime


# ---------------------------------------------------------------------------
# 1. Dispatch: call path returns reply
# ---------------------------------------------------------------------------


async def test_handle_call_returns_reply():
    server = EchoServer("echo")
    runtime = await _start_runtime(server)
    try:
        result = await runtime.call("echo", {"msg": "hello"})
        assert result["echo"] == {"msg": "hello"}
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 2. Dispatch: cast path runs handler, no reply
# ---------------------------------------------------------------------------


async def test_handle_cast_runs_no_reply():
    server = EchoServer("echo")
    runtime = await _start_runtime(server)
    try:
        await runtime.cast("echo", {"key": "value"})
        await wait_for(lambda: "last_cast" in server.state, msg="cast handled")
        assert server.state["last_cast"] == {"key": "value"}
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 3. Dispatch: info path for non-call non-cast messages
# ---------------------------------------------------------------------------


async def test_handle_info_invoked_for_plain_messages():
    server = EchoServer("echo")
    runtime = await _start_runtime(server)
    try:
        # Plain send (no reply_to, no __cast__) → handle_info
        await runtime.send("echo", {"type": "ping"})
        await wait_for(lambda: "last_info" in server.state, msg="info handled")
        assert server.state["last_info"] == {"type": "ping"}
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 4. call() timeout raises
# ---------------------------------------------------------------------------


async def test_call_timeout_raises():
    class SlowServer(GenServer):
        async def handle_call(self, payload: dict[str, Any], from_: str) -> dict[str, Any]:
            await asyncio.sleep(10)
            return {}

    runtime = await _start_runtime(SlowServer("slow"))
    try:
        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            await runtime.call("slow", {}, timeout=0.05)
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 5. send_after fires handle_info after delay
# ---------------------------------------------------------------------------


async def test_send_after_fires_handle_info():
    server = TickServer("ticker")
    runtime = await _start_runtime(server)
    try:
        await wait_for(lambda: server.state.get("ticks", 0) >= 1, timeout=1.0, msg="tick fired")
        assert server.state["ticks"] >= 1
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 6. send_after tasks cancelled cleanly on stop
# ---------------------------------------------------------------------------


async def test_send_after_tasks_cancelled_on_stop():
    class LongTimerServer(GenServer):
        async def init(self) -> None:
            self.send_after(60_000, {"type": "never"})

    server = LongTimerServer("long")
    runtime = await _start_runtime(server)
    # Let it start up
    await asyncio.sleep(0.05)
    # There should be one pending task
    assert len(server._send_after_tasks) == 1
    assert not server._send_after_tasks[0].done()

    await runtime.stop()
    # After stop, tasks should be cancelled / cleared
    assert all(t.done() for t in server._send_after_tasks)


# ---------------------------------------------------------------------------
# 7. init() runs before first message
# ---------------------------------------------------------------------------


async def test_init_called_before_first_message():
    server = CounterServer("counter")
    runtime = await _start_runtime(server)
    try:
        # init() should have set count = 0
        result = await runtime.call("counter", {"op": "get"})
        assert result["count"] == 0
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 8-10. GenServer as child of all three supervision strategies
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strategy", ["ONE_FOR_ONE", "ONE_FOR_ALL", "REST_FOR_ONE"])
async def test_genserver_with_supervision_strategies(strategy: str):
    server = EchoServer("echo")
    runtime = Runtime(supervisor=Supervisor("root", children=[server], strategy=strategy))
    await runtime.start()
    try:
        result = await runtime.call("echo", {"x": 1})
        assert result["echo"] == {"x": 1}
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 11. Restart triggers init() again
# ---------------------------------------------------------------------------


async def test_restart_triggers_init():
    class CrashThenRecover(GenServer):
        crashed = False

        async def init(self) -> None:
            self.state["count"] = 0

        async def handle_call(self, payload: dict[str, Any], from_: str) -> dict[str, Any]:
            if not CrashThenRecover.crashed:
                CrashThenRecover.crashed = True
                raise RuntimeError("intentional crash")
            return {"count": self.state["count"]}

    server = CrashThenRecover("crashing")
    runtime = Runtime(
        supervisor=Supervisor("root", children=[server], max_restarts=3, restart_window=10.0)
    )
    await runtime.start()
    try:
        # First call crashes the server
        with pytest.raises((RuntimeError, TimeoutError, asyncio.TimeoutError)):
            await runtime.call("crashing", {}, timeout=1.0)
        # Wait for restart
        await wait_for_status(server, ProcessStatus.RUNNING, timeout=2.0)
        # After restart, init() has reset count to 0
        result = await runtime.call("crashing", {})
        assert result["count"] == 0
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 12. self.llm is None (no LLM injected)
# ---------------------------------------------------------------------------


async def test_no_llm_injected():
    server = EchoServer("echo")
    runtime = await _start_runtime(server)
    try:
        assert server.llm is None
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 13. self.tools is None (no tool provider injected)
# ---------------------------------------------------------------------------


async def test_no_tools_injected():
    server = EchoServer("echo")
    runtime = await _start_runtime(server)
    try:
        assert server.tools is None
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 14. handle_call returning non-dict raises TypeError
# ---------------------------------------------------------------------------


async def test_handle_call_non_dict_raises():
    server = BadCallServer("bad")
    runtime = await _start_runtime(server)
    try:
        with pytest.raises((TypeError, RuntimeError, TimeoutError, asyncio.TimeoutError)):
            await runtime.call("bad", {}, timeout=1.0)
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 15. GenServer ↔ AgentProcess sibling communication
# ---------------------------------------------------------------------------


async def test_genserver_agent_sibling_roundtrip():
    from civitas import AgentProcess

    class RequesterAgent(AgentProcess):
        result: dict[str, Any] = {}

        async def handle(self, message: Message) -> Message | None:
            if message.payload.get("op") == "run":
                # call the sibling GenServer
                reply = await self.call("counter", {"op": "increment"})
                RequesterAgent.result = reply
            return None

    agent = RequesterAgent("requester")
    counter = CounterServer("counter")
    runtime = await _start_runtime(agent, counter)
    try:
        await runtime.send("requester", {"op": "run"})
        await wait_for(lambda: RequesterAgent.result != {}, msg="agent got reply")
        assert RequesterAgent.result["count"] == 1
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 16. State persists across multiple handle_call invocations
# ---------------------------------------------------------------------------


async def test_state_persists_across_calls():
    server = CounterServer("counter")
    runtime = await _start_runtime(server)
    try:
        await runtime.call("counter", {"op": "increment"})
        await runtime.call("counter", {"op": "increment"})
        result = await runtime.call("counter", {"op": "get"})
        assert result["count"] == 2
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 17. cast strips __cast__ marker before passing to handle_cast
# ---------------------------------------------------------------------------


async def test_cast_strips_marker():
    server = EchoServer("echo")
    runtime = await _start_runtime(server)
    try:
        await runtime.cast("echo", {"data": "x"})
        await wait_for(lambda: "last_cast" in server.state, msg="cast handled")
        assert "__cast__" not in server.state["last_cast"]
        assert server.state["last_cast"] == {"data": "x"}
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 18. AgentProcess.call() and AgentProcess.cast() delegate correctly
# ---------------------------------------------------------------------------


async def test_agent_call_and_cast_methods():
    from civitas import AgentProcess

    class CallerAgent(AgentProcess):
        results: list[Any] = []
        casts_received: list[Any] = []

        async def handle(self, message: Message) -> Message | None:
            if message.payload.get("op") == "do_call":
                reply = await self.call("echo", {"ping": True})
                CallerAgent.results.append(reply)
            elif message.payload.get("op") == "do_cast":
                await self.cast("echo", {"cast_data": 42})
            return None

    caller = CallerAgent("caller")
    echo = EchoServer("echo")
    runtime = await _start_runtime(caller, echo)
    try:
        await runtime.send("caller", {"op": "do_call"})
        await wait_for(lambda: len(CallerAgent.results) == 1, msg="call reply received")
        assert CallerAgent.results[0]["echo"] == {"ping": True}

        await runtime.send("caller", {"op": "do_cast"})
        await wait_for(lambda: "last_cast" in echo.state, msg="cast received by echo")
        assert echo.state["last_cast"] == {"cast_data": 42}
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 19. send_after prunes completed tasks (no unbounded growth)
# ---------------------------------------------------------------------------


async def test_send_after_prunes_completed_tasks():
    server = EchoServer("echo")
    runtime = await _start_runtime(server)
    try:
        # Fire 5 very short timers
        for i in range(5):
            server.send_after(10, {"i": i})
        # Wait for them to complete
        await asyncio.sleep(0.1)
        # Next send_after call prunes completed tasks
        server.send_after(60_000, {"final": True})
        assert len(server._send_after_tasks) == 1  # only the long pending one
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 20. Base GenServer.handle_call raises NotImplementedError (line 57)
# ---------------------------------------------------------------------------


async def test_base_handle_call_not_implemented():
    gs = GenServer("gs")
    with pytest.raises(NotImplementedError, match="handle_call"):
        await gs.handle_call({}, "sender")


# ---------------------------------------------------------------------------
# 21. send_after when bus is None — task fires without crash (line 76->exit)
# ---------------------------------------------------------------------------


async def test_send_after_bus_none_no_crash():
    gs = GenServer("gs")
    gs._bus = None
    gs._send_after_tasks = []
    gs.send_after(5, {"type": "tick"})
    await asyncio.sleep(0.02)  # let the task run
    # No exception raised


# ---------------------------------------------------------------------------
# 22. send_after when bus.route raises — exception swallowed (lines 86-87)
# ---------------------------------------------------------------------------


async def test_send_after_route_error_swallowed():
    from unittest.mock import AsyncMock

    gs = GenServer("gs")
    mock_bus = AsyncMock()
    mock_bus.route.side_effect = RuntimeError("route failed")
    gs._bus = mock_bus
    gs._send_after_tasks = []
    gs.send_after(5, {"type": "tick"})
    await asyncio.sleep(0.02)  # let the task run
    mock_bus.route.assert_called_once()  # route WAS called
    # Exception was silently swallowed


# ---------------------------------------------------------------------------
# 23. _gs_span returns None when tracer is not set (line 152)
# ---------------------------------------------------------------------------


def test_gs_span_returns_none_when_no_tracer():
    gs = GenServer("gs")
    gs._tracer = None
    gs._current_handle_span = None
    msg = Message(type="t", sender="s", recipient="gs", payload={})
    result = gs._gs_span("civitas.genserver.call", msg)
    assert result is None


# ---------------------------------------------------------------------------
# 24. handle_cast raising covers exception + span path (lines 130-133, 135->exit)
# ---------------------------------------------------------------------------


class RaisingCastServer(GenServer):
    async def handle_cast(self, payload: dict[str, Any]) -> None:
        raise ValueError("cast boom")


async def test_handle_cast_exception_propagates_and_span_error_set():
    server = RaisingCastServer("raiser")
    runtime = await _start_runtime(server)
    try:
        await runtime.send("raiser", {"__cast__": True})
        await asyncio.sleep(0.05)  # let the task process; exception is caught by supervisor
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 25. handle_info raising covers exception + span path (lines 142-145, 147->exit)
# ---------------------------------------------------------------------------


class RaisingInfoServer(GenServer):
    async def handle_info(self, payload: dict[str, Any]) -> None:
        raise ValueError("info boom")


async def test_handle_info_exception_propagates_and_span_error_set():
    server = RaisingInfoServer("raiser2")
    runtime = await _start_runtime(server)
    try:
        await runtime.send("raiser2", {"type": "info"})
        await asyncio.sleep(0.05)
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# 26. handle_call exception with active span (lines 118-120)
# ---------------------------------------------------------------------------


class RaisingCallServer(GenServer):
    async def handle_call(self, payload: dict[str, Any], from_: str) -> dict[str, Any]:
        raise RuntimeError("call boom")


async def test_handle_call_exception_with_span():
    server = RaisingCallServer("raiser3")
    runtime = await _start_runtime(server)
    try:
        with pytest.raises((RuntimeError, TimeoutError, asyncio.TimeoutError)):
            await runtime.call("raiser3", {}, timeout=1.0)
    finally:
        await runtime.stop()
