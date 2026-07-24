"""M1.2 — Supervised Agent testable criteria.

Each test maps to one bullet in the M1.2 milestone.
"""

import time

from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message
from civitas.process import ProcessStatus
from tests.conftest import wait_for, wait_for_status

# ------------------------------------------------------------------
# Test agents
# ------------------------------------------------------------------


class AlwaysCrashAgent(AgentProcess):
    """Crashes on every message."""

    async def handle(self, message: Message) -> Message | None:
        raise ValueError("boom")


class CrashOnceAgent(AgentProcess):
    """Crashes on the first message EVER (per name), works after restart.

    v0.9.0 D1a: restart builds a FRESH incarnation, so instance variables
    reset — "crashed once" must live outside the instance (class-level, per
    name) to survive the restart. The original instance-flag version was
    depending on the exact undocumented behavior D1a removed. Tests reset
    `CrashOnceAgent.crashed` explicitly.
    """

    crashed: dict[str, bool] = {}

    async def handle(self, message: Message) -> Message | None:
        if not type(self).crashed.get(self.name):
            type(self).crashed[self.name] = True
            raise ValueError("first-time crash")
        return self.reply({"status": "ok", "msg": message.payload.get("text", "")})


class CountingAgent(AgentProcess):
    """Tracks how many messages it has handled across restarts."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.handled = 0

    async def handle(self, message: Message) -> Message | None:
        self.handled += 1
        return self.reply({"handled": self.handled})


class TrackingAgent(AgentProcess):
    """Records start count ACROSS incarnations to detect restarts (D1a-safe:
    class-level per-name counter; tests reset `TrackingAgent.starts`)."""

    starts: dict[str, int] = {}

    async def on_start(self) -> None:
        type(self).starts[self.name] = type(self).starts.get(self.name, 0) + 1

    async def handle(self, message: Message) -> Message | None:
        return self.reply({"starts": type(self).starts.get(self.name, 0)})


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


async def test_supervisor_detects_agent_crash():
    """Supervisor detects agent crash (unhandled exception in handle())."""
    CrashOnceAgent.crashed = {}
    agent = CrashOnceAgent("crasher")
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[agent],
            max_restarts=3,
            backoff="CONSTANT",
            backoff_base=0.01,
        )
    )
    await runtime.start()
    try:
        # Send a message that triggers the crash
        await runtime.send("crasher", {"text": "trigger"})
        await wait_for_status(agent, ProcessStatus.CRASHED)  # old ref stays CRASHED
        # Q1/D1a: RUNNING is observable only on the CURRENT incarnation.
        await wait_for(lambda: runtime.get_agent("crasher").status == ProcessStatus.RUNNING)
        assert CrashOnceAgent.crashed["crasher"] is True  # confirms it did crash
    finally:
        await runtime.stop()


async def test_one_for_one_restarts_only_failed_agent():
    """ONE_FOR_ONE strategy restarts only the failed agent."""
    CrashOnceAgent.crashed = {}
    crasher = CrashOnceAgent("crasher")
    TrackingAgent.starts = {}
    healthy = TrackingAgent("healthy")

    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            strategy="ONE_FOR_ONE",
            children=[crasher, healthy],
            max_restarts=3,
            backoff="CONSTANT",
            backoff_base=0.01,
        )
    )
    await runtime.start()
    try:
        # Healthy agent started once
        r = await runtime.ask("healthy", {})
        assert r.payload["starts"] == 1

        # Trigger crash in crasher
        await runtime.send("crasher", {"text": "trigger"})
        await wait_for_status(crasher, ProcessStatus.CRASHED)
        await wait_for(  # Q1: current incarnation
            lambda: runtime.get_agent("crasher").status == ProcessStatus.RUNNING
        )

        # Crasher restarted (fresh incarnation), healthy still only started once
        assert runtime.get_agent("crasher").status == ProcessStatus.RUNNING
        r = await runtime.ask("healthy", {})
        assert r.payload["starts"] == 1
    finally:
        await runtime.stop()


async def test_one_for_all_restarts_all_siblings():
    """ONE_FOR_ALL strategy restarts all siblings."""
    CrashOnceAgent.crashed = {}
    crasher = CrashOnceAgent("crasher")
    TrackingAgent.starts = {}
    sibling = TrackingAgent("sibling")

    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            strategy="ONE_FOR_ALL",
            children=[crasher, sibling],
            max_restarts=3,
            backoff="CONSTANT",
            backoff_base=0.01,
        )
    )
    await runtime.start()
    try:
        r = await runtime.ask("sibling", {})
        assert r.payload["starts"] == 1

        # Trigger crash — should restart ALL children
        await runtime.send("crasher", {"text": "trigger"})
        await wait_for(
            lambda: TrackingAgent.starts.get("sibling", 0) == 2, msg="sibling starts == 2"
        )

        # Sibling should have been restarted (start_count == 2)
        r = await runtime.ask("sibling", {})
        assert r.payload["starts"] == 2
    finally:
        await runtime.stop()


async def test_rest_for_one_restarts_failed_and_downstream():
    """REST_FOR_ONE strategy restarts the failed agent and downstream siblings."""
    TrackingAgent.starts = {}
    upstream = TrackingAgent("upstream")
    CrashOnceAgent.crashed = {}
    crasher = CrashOnceAgent("crasher")
    downstream = TrackingAgent("downstream")

    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            strategy="REST_FOR_ONE",
            children=[upstream, crasher, downstream],
            max_restarts=3,
            backoff="CONSTANT",
            backoff_base=0.01,
        )
    )
    await runtime.start()
    try:
        r_up = await runtime.ask("upstream", {})
        r_down = await runtime.ask("downstream", {})
        assert r_up.payload["starts"] == 1
        assert r_down.payload["starts"] == 1

        # Crash the middle agent — downstream should restart, upstream should not
        await runtime.send("crasher", {"text": "trigger"})
        await wait_for(
            lambda: TrackingAgent.starts.get("downstream", 0) == 2, msg="downstream starts == 2"
        )

        r_up = await runtime.ask("upstream", {})
        r_down = await runtime.ask("downstream", {})
        assert r_up.payload["starts"] == 1  # upstream untouched
        assert r_down.payload["starts"] == 2  # downstream restarted
    finally:
        await runtime.stop()


async def test_restart_counter_increments():
    """Restart counter increments correctly."""
    agent = AlwaysCrashAgent("crasher")
    sup = Supervisor(
        "root",
        children=[agent],
        max_restarts=5,
        restart_window=60.0,
        backoff="CONSTANT",
        backoff_base=0.01,
    )
    runtime = Runtime(supervisor=sup)
    await runtime.start()
    try:
        # Trigger 3 crashes — wait for each restart cycle before sending next
        for i in range(3):
            await runtime.send("crasher", {"text": "trigger"})
            await wait_for(  # Q1: each cycle produces a new incarnation
                lambda n=i: sup._restart_counts.get("crasher", 0) >= n + 1
            )
            await wait_for(lambda: runtime.get_agent("crasher").status == ProcessStatus.RUNNING)

        assert sup._restart_counts.get("crasher", 0) >= 3
    finally:
        await runtime.stop()


async def test_max_restarts_triggers_escalation():
    """Max restarts limit triggers escalation."""
    agent = AlwaysCrashAgent("crasher")
    sup = Supervisor(
        "root",
        children=[agent],
        max_restarts=2,
        restart_window=60.0,
        backoff="CONSTANT",
        backoff_base=0.01,
    )
    runtime = Runtime(supervisor=sup)
    await runtime.start()
    try:
        # Trigger enough crashes to exceed max_restarts
        # Wait for RUNNING after each cycle until max is hit, then STOPPED
        for _ in range(4):
            await runtime.send("crasher", {"text": "trigger"})
            try:
                await wait_for(
                    lambda: runtime.get_agent("crasher").status == ProcessStatus.RUNNING,
                    timeout=2.0,
                )
            except TimeoutError:
                break  # agent hit max_restarts and stopped — expected

        # After exceeding max_restarts, the current incarnation stays CRASHED
        await wait_for(lambda: runtime.get_agent("crasher").status == ProcessStatus.CRASHED)
    finally:
        await runtime.stop()


async def test_backoff_delay_applied():
    """Backoff delay is applied between restarts."""
    CrashOnceAgent.crashed = {}
    agent = CrashOnceAgent("crasher")
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[agent],
            max_restarts=3,
            backoff="CONSTANT",
            backoff_base=0.2,  # 200ms delay
        )
    )
    await runtime.start()
    try:
        t0 = time.monotonic()
        await runtime.send("crasher", {"text": "trigger"})
        # Phase 1: wait for the agent to leave RUNNING (crash)
        await wait_for(lambda: agent.status != ProcessStatus.RUNNING, timeout=2.0)
        # Phase 2: the fresh incarnation comes up after the backoff delay (Q1)
        await wait_for(
            lambda: runtime.get_agent("crasher").status == ProcessStatus.RUNNING, timeout=3.0
        )
        elapsed = time.monotonic() - t0

        # Restart should have taken at least the backoff delay
        assert elapsed >= 0.2
    finally:
        await runtime.stop()


async def test_restarted_agent_receives_subsequent_messages():
    """Restarted agent receives subsequent messages normally."""
    CrashOnceAgent.crashed = {}
    agent = CrashOnceAgent("crasher")
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[agent],
            max_restarts=3,
            backoff="CONSTANT",
            backoff_base=0.01,
        )
    )
    await runtime.start()
    try:
        # Trigger crash
        await runtime.send("crasher", {"text": "trigger"})
        await wait_for(  # Q1: current incarnation
            lambda: runtime.get_agent("crasher").status == ProcessStatus.RUNNING
        )

        # Agent should be back and functional
        result = await runtime.ask("crasher", {"text": "hello after restart"})
        assert result.payload["status"] == "ok"
        assert result.payload["msg"] == "hello after restart"
    finally:
        await runtime.stop()
