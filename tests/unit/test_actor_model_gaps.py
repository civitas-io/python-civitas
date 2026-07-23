"""Regression tests tracking actor-model gaps in the supervision core.

Fixed gaps keep their tests here as plain regressions (v0.8.0 PR1 flipped
#28/#29/#30). Remaining gaps stay ``xfail(strict=True)``: the suite is green
while the bug exists, and a fix flips the test to XPASS — failing the run and
forcing the marker's removal, so the tracking can never go stale.

Findings catalog: docs/design/supervision-hardening.md
Still open here: #31 (A6, heartbeat priority — PR2) and A1 (restart
semantics — PR3 flips the state test; the instance-var test waits for the
v0.9 fresh-instance restart).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from civitas.messages import Message
from civitas.process import AgentProcess, ProcessStatus
from civitas.runtime import Runtime
from civitas.supervisor import Supervisor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _wait_for(predicate, timeout: float = 3.0, interval: float = 0.02) -> bool:
    """Poll ``predicate`` until true or ``timeout`` elapses."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


class CrashOnCommand(AgentProcess):
    """Crashes (ESCALATE) on {"cmd": "boom"}; replies otherwise."""

    capabilities = ["gap.probe"]

    async def handle(self, message: Message) -> Message | None:
        if message.payload.get("cmd") == "boom":
            raise RuntimeError("boom")
        return self.reply({"ok": True})


class DirtyInstanceAgent(AgentProcess):
    """Corrupts an instance variable, then crashes."""

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        self.poison = False

    async def handle(self, message: Message) -> Message | None:
        if message.payload.get("cmd") == "poison":
            self.poison = True
            raise RuntimeError("corrupted instance")
        return self.reply({"poison": self.poison})


class DirtyStateAgent(AgentProcess):
    """Corrupts self.state (never checkpointed), then crashes."""

    async def handle(self, message: Message) -> Message | None:
        if message.payload.get("cmd") == "poison":
            self.state["corrupt"] = True
            raise RuntimeError("corrupted state")
        return self.reply({"corrupt": self.state.get("corrupt", False)})


# ---------------------------------------------------------------------------
# A1 — restart must produce a fresh actor (let-it-crash discards dirty state)
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="A1 / design D1(a): restart reuses the same AgentProcess instance — "
    "instance variables survive the crash. Scheduled for the v0.9 fresh-instance "
    "restart (child spec is already captured by __new__ since v0.8.0 PR3)",
    strict=True,
)
async def test_restart_resets_instance_variables():
    agent = DirtyInstanceAgent("dirty")
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("dirty", {"cmd": "poison"})
        # Wait until the supervisor has observed the crash, then for recovery —
        # checking RUNNING alone races the crash and passes spuriously.
        assert await _wait_for(lambda: root._restart_counts.get("dirty", 0) >= 1)
        assert await _wait_for(lambda: agent.status == ProcessStatus.RUNNING)
        reply = await runtime.ask("dirty", {"cmd": "check"}, timeout=2.0)
        # OTP semantics: a restarted actor is a fresh actor — corrupted
        # in-memory state must not survive the crash that it caused.
        assert reply.payload["poison"] is False
    finally:
        await runtime.stop()


async def test_restart_resets_uncheckpointed_state():
    agent = DirtyStateAgent("dirty_state")
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("dirty_state", {"cmd": "poison"})
        assert await _wait_for(lambda: root._restart_counts.get("dirty_state", 0) >= 1)
        assert await _wait_for(lambda: agent.status == ProcessStatus.RUNNING)
        reply = await runtime.ask("dirty_state", {"cmd": "check"}, timeout=2.0)
        assert reply.payload["corrupt"] is False
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# A2 / #28 — escalation must restart the escalated subtree under ONE_FOR_ONE
# ---------------------------------------------------------------------------


async def test_escalation_restarts_subtree_under_one_for_one():
    worker = CrashOnCommand("worker")
    # max_restarts=0: the first crash immediately exhausts the child budget
    # and escalates to root.
    child_sup = Supervisor("child_sup", children=[worker], max_restarts=0, backoff_base=0.01)
    root = Supervisor(
        "root",
        children=[child_sup],
        strategy="ONE_FOR_ONE",
        max_restarts=5,
        backoff_base=0.01,
    )
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("worker", {"cmd": "boom"})
        # Wait until ROOT has observed the escalation (root's counter is never
        # cleared; child_sup's own counters are wiped by the H1 subtree restart,
        # so waiting on those would race the restart).
        assert await _wait_for(lambda: root._restart_counts.get("child_sup", 0) >= 1)
        # OTP semantics: root restarts the escalated child_sup, which restarts
        # its children — the worker must come back RUNNING.
        assert await _wait_for(lambda: worker.status == ProcessStatus.RUNNING), (
            f"escalated subtree was never restarted — worker stayed {worker.status.value}"
        )
        # Fresh incarnation, fresh budget (H1): without the clear, the next
        # crash would instantly re-escalate on the still-exhausted window.
        assert len(child_sup._restart_timestamps) == 0
        assert child_sup._restart_counts == {}
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# A3 / #29 — capabilities must survive a crash-restart
# ---------------------------------------------------------------------------


async def test_capabilities_survive_restart():
    worker = CrashOnCommand("cap_worker")
    root = Supervisor("root", children=[worker], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        found = runtime._registry.find_by_capability("gap.probe")
        assert [e.name for e in found] == ["cap_worker"]  # sanity: registered at start

        await runtime.send("cap_worker", {"cmd": "boom"})
        # Wait for the supervisor to observe the crash and complete the restart —
        # checking capabilities before the restart races and passes spuriously.
        assert await _wait_for(lambda: root._restart_counts.get("cap_worker", 0) >= 1)
        assert await _wait_for(lambda: worker.status == ProcessStatus.RUNNING)

        found = runtime._registry.find_by_capability("gap.probe")
        assert [e.name for e in found] == ["cap_worker"], (
            "capability registration was lost across crash-restart"
        )
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# B2 / #30 — a failed restart must be surfaced, never silently dropped
# ---------------------------------------------------------------------------


async def test_failed_restart_is_surfaced(caplog):
    worker = CrashOnCommand("fragile")
    root = Supervisor("root", children=[worker], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        # Sabotage re-registration so the restart itself fails.
        registry = runtime._registry
        original_register = registry.register

        def bad_register(name: str, *args: Any, **kwargs: Any) -> None:
            if name == "fragile":
                raise RuntimeError("registry unavailable")
            original_register(name, *args, **kwargs)

        registry.register = bad_register  # type: ignore[method-assign]

        with caplog.at_level(logging.WARNING):
            await runtime.send("fragile", {"cmd": "boom"})
            await asyncio.sleep(0.5)

        registry.register = original_register  # type: ignore[method-assign]

        # The child is dead — that must be visible somewhere: at minimum an
        # ERROR/WARNING log naming the child from the supervision layer.
        surfaced = any(
            "fragile" in record.getMessage() and record.levelno >= logging.WARNING
            for record in caplog.records
        )
        assert surfaced, "restart failure was swallowed silently — child dead, no log"
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# H5b/H6/H7 (PR3) — restart state contract, watchdog, on_stop containment
# ---------------------------------------------------------------------------


class CheckpointingCrasher(AgentProcess):
    """Checkpoints business state, then crashes on command."""

    async def handle(self, message: Message) -> Message | None:
        if message.payload.get("cmd") == "save":
            self.state["saved"] = message.payload["value"]
            await self.checkpoint()
            return self.reply({"ok": True})
        if message.payload.get("cmd") == "boom":
            raise RuntimeError("boom")
        return self.reply({"saved": self.state.get("saved")})


async def test_checkpointed_state_survives_reset_and_restart():
    """H5b keeps the documented contract intact: checkpointed state is restored
    after the incarnation reset."""
    agent = CheckpointingCrasher("saver")
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.ask("saver", {"cmd": "save", "value": 42}, timeout=2.0)
        await runtime.send("saver", {"cmd": "boom"})
        assert await _wait_for(lambda: root._restart_counts.get("saver", 0) >= 1)
        assert await _wait_for(lambda: agent.status == ProcessStatus.RUNNING)
        reply = await runtime.ask("saver", {"cmd": "check"}, timeout=2.0)
        assert reply.payload["saved"] == 42
    finally:
        await runtime.stop()


async def test_checkpointed_suspend_marker_survives_reset():
    """Constraint 4 (plan §2): the H5b reset must not defeat durable suspension —
    the checkpointed marker is restored and the agent comes up SUSPENDED (S7)."""
    from civitas.plugins.state import InMemoryStateStore

    store = InMemoryStateStore()
    await store.set(
        "restorer",
        {"_civitas.suspended": {"reason": "hitl", "since": 1.0, "approver": None}},
    )
    agent = CrashOnCommand("restorer")
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root, state_store=store)
    await runtime.start()
    try:
        assert await _wait_for(lambda: agent.status == ProcessStatus.SUSPENDED)
    finally:
        await runtime.stop()


def test_ctor_spec_captured_for_fresh_instance_restart():
    """__new__ records (cls, args, kwargs) — the child spec the v0.9
    fresh-instance restart (design D1a) will consume."""
    from civitas.supervisor import DynamicSupervisor

    dyn = DynamicSupervisor("workers", max_children=7, restart="never")
    cls, args, kwargs = dyn._civitas_spec
    assert cls is DynamicSupervisor
    assert args == ("workers",)
    assert kwargs == {"max_children": 7, "restart": "never"}


class HangingAgent(AgentProcess):
    """Hangs forever on command; SKIPs timeouts when configured to."""

    skip_on_timeout = False

    async def handle(self, message: Message) -> Message | None:
        if message.payload.get("cmd") == "hang":
            await asyncio.sleep(3600)
        return self.reply({"ok": True})

    async def on_error(self, error: Exception, message: Message):
        from civitas.errors import ErrorAction

        if self.skip_on_timeout and isinstance(error, TimeoutError):
            return ErrorAction.SKIP
        return ErrorAction.ESCALATE


async def test_handle_timeout_turns_hang_into_visible_crash():
    """H6: a hung async handle() becomes an ordinary crash the supervisor sees
    and restarts — hung local agents stop being invisible (A7)."""
    agent = HangingAgent("hanger", handle_timeout=0.05)
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("hanger", {"cmd": "hang"})
        assert await _wait_for(lambda: root._restart_counts.get("hanger", 0) >= 1)
        assert await _wait_for(lambda: agent.status == ProcessStatus.RUNNING)
        reply = await runtime.ask("hanger", {"cmd": "work"}, timeout=2.0)
        assert reply.payload["ok"] is True  # recovered and serving
    finally:
        await runtime.stop()


async def test_handle_timeout_respects_on_error_skip():
    """H6: TimeoutError flows through the normal on_error path — an agent may
    choose to SKIP instead of crashing."""
    agent = HangingAgent("tolerant", handle_timeout=0.05)
    agent.skip_on_timeout = True
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("tolerant", {"cmd": "hang"})
        reply = await runtime.ask("tolerant", {"cmd": "work"}, timeout=2.0)
        assert reply.payload["ok"] is True
        assert root._restart_counts.get("tolerant", 0) == 0  # never crashed
    finally:
        await runtime.stop()


async def test_handle_timeout_disabled_by_default():
    """H6: default None — no watchdog, zero behavior change on upgrade."""
    agent = HangingAgent("unbounded")
    assert agent._handle_timeout is None
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("unbounded", {"cmd": "hang"})
        await asyncio.sleep(0.15)  # well past the other tests' timeout
        assert agent.status == ProcessStatus.RUNNING  # still "handling", no crash
        assert root._restart_counts.get("unbounded", 0) == 0
    finally:
        await runtime.stop()


def test_yaml_handle_timeout_passthrough():
    """H6: topology YAML `agent: {handle_timeout: N}` reaches the constructor."""
    config = {
        "supervision": {
            "name": "root",
            "children": [
                {"agent": {"name": "bounded", "type": "W", "handle_timeout": 12.5}},
                {"agent": {"name": "unbounded", "type": "W"}},
            ],
        }
    }
    rt = Runtime.from_config_dict(config, agent_classes={"W": CrashOnCommand})
    agents = {a.name: a for a in rt._root_supervisor.all_agents()}
    assert agents["bounded"]._handle_timeout == 12.5
    assert agents["unbounded"]._handle_timeout is None


class FaultyStopAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"ok": True})

    async def on_stop(self) -> None:
        raise RuntimeError("cleanup exploded")


async def test_on_stop_exception_contained_during_shutdown():
    """H7 (#27): a raising on_stop() during graceful shutdown is contained —
    the agent reaches STOPPED, no crash event lands on the supervisor, and the
    shutdown sequence completes."""
    faulty = FaultyStopAgent("faulty")
    healthy = CrashOnCommand("healthy")
    root = Supervisor("root", children=[faulty, healthy], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()

    await runtime.stop()  # must not raise despite faulty.on_stop()

    assert faulty.status == ProcessStatus.STOPPED
    assert healthy.status == ProcessStatus.STOPPED  # shutdown sequence completed
    assert root._crash_queue.qsize() == 0  # graceful stop produced no crash event


# ---------------------------------------------------------------------------
# H8/H9/H10 (PR4) — retry-in-place, _runtime sink, registry hygiene
# ---------------------------------------------------------------------------


class FlakyOnce(AgentProcess):
    """Fails the first attempt of 'flaky' messages, then succeeds; records order."""

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        self.observed: list[tuple[str, int]] = []

    async def handle(self, message: Message) -> Message | None:
        self.observed.append((message.payload.get("tag", ""), message.attempt))
        if message.payload.get("tag") == "flaky" and message.attempt == 0:
            raise RuntimeError("transient")
        return None

    async def on_error(self, error: Exception, message: Message):
        from civitas.errors import ErrorAction

        return ErrorAction.RETRY


async def test_retry_in_place_preserves_fifo():
    """H8 (#32): a retried message completes before anything queued behind it —
    per-sender FIFO holds even across transient failures."""
    agent = FlakyOnce("fifo")
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("fifo", {"tag": "flaky"})
        await runtime.send("fifo", {"tag": "behind"})
        assert await _wait_for(lambda: len(agent.observed) >= 3)
        assert agent.observed == [("flaky", 0), ("flaky", 1), ("behind", 0)], (
            f"FIFO broken across retry: {agent.observed}"
        )
    finally:
        await runtime.stop()


async def test_retry_gets_fresh_handle_timeout_per_attempt():
    """H8 × H6 (plan constraint 5): each attempt is wrapped separately — a
    retried handler gets the full budget, and a hang-then-recover pattern works."""

    class HangsOnceThenFast(AgentProcess):
        attempts = 0

        async def handle(self, message: Message) -> Message | None:
            type(self).attempts += 1
            if message.attempt == 0:
                await asyncio.sleep(3600)  # times out
            return self.reply({"recovered": True})

        async def on_error(self, error: Exception, message: Message):
            from civitas.errors import ErrorAction

            if isinstance(error, TimeoutError):
                return ErrorAction.RETRY
            return ErrorAction.ESCALATE

    HangsOnceThenFast.attempts = 0
    agent = HangsOnceThenFast("hangs_once", handle_timeout=0.05)
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        reply = await runtime.ask("hangs_once", {"work": 1}, timeout=3.0)
        assert reply.payload["recovered"] is True
        assert HangsOnceThenFast.attempts == 2
        assert root._restart_counts.get("hangs_once", 0) == 0  # recovered, no crash
    finally:
        await runtime.stop()


async def test_retry_aborts_when_agent_leaves_running():
    """H8 (plan constraint 5): a STOP arriving mid-retry is not delayed by
    max_retries × handler-time — the message is dropped (at-most-once)."""

    class StopMidRetry(AgentProcess):
        attempts = 0

        async def handle(self, message: Message) -> Message | None:
            type(self).attempts += 1
            raise RuntimeError("always")

        async def on_error(self, error: Exception, message: Message):
            from civitas.errors import ErrorAction

            self._status = ProcessStatus.STOPPING  # simulate STOP landing mid-retry
            return ErrorAction.RETRY

    StopMidRetry.attempts = 0
    agent = StopMidRetry("stopper", max_retries=5)
    await agent._start()
    await agent._mailbox.put(Message(type="work"))
    assert await _wait_for(lambda: agent.status == ProcessStatus.STOPPED)
    assert StopMidRetry.attempts == 1  # no further attempts after status change


class RepliesToSender(AgentProcess):
    """Does the natural-but-dangerous send(message.sender) follow-up."""

    async def handle(self, message: Message) -> Message | None:
        await self.send(message.sender, {"heads_up": True})
        return self.reply({"ok": True})


async def test_send_to_runtime_sender_is_dropped_not_crashed(caplog):
    """H9 (#33): send(message.sender) on a Runtime-initiated message lands in
    the '_runtime' sink — WARNING-logged, dropped, agent unharmed."""
    import logging as _logging

    agent = RepliesToSender("chatty")
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        with caplog.at_level(_logging.WARNING, logger="civitas.runtime"):
            reply = await runtime.ask("chatty", {"q": 1}, timeout=2.0)
        assert reply.payload["ok"] is True
        assert agent.status == ProcessStatus.RUNNING  # no crash
        assert any("_runtime" in r.getMessage() for r in caplog.records)
        assert root._restart_counts.get("chatty", 0) == 0
    finally:
        await runtime.stop()


async def test_ask_runtime_fails_fast_with_error_reply():
    """H9 (#33): ask('_runtime') gets an immediate error reply, not a timeout."""
    agent = CrashOnCommand("bystander")
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        start = asyncio.get_running_loop().time()
        reply = await runtime.ask("_runtime", {"oops": True}, timeout=10.0)
        elapsed = asyncio.get_running_loop().time() - start
        assert reply.payload["status"] == "error"
        assert "_runtime" in reply.payload["error"]
        assert elapsed < 2.0, "error reply should be immediate, not a timeout"
    finally:
        await runtime.stop()


def test_glob_patterns_exclude_system_names():
    """C6 slice of H13: broadcast('*') must not hit internal endpoints; explicit
    underscore patterns still match."""
    from civitas.registry import LocalRegistry

    registry = LocalRegistry()
    registry.register("worker_a")
    registry.register("_runtime")
    registry.register("_agency.worker.restart")

    assert [e.name for e in registry.lookup_all("*")] == ["worker_a"]
    assert [e.name for e in registry.lookup_all("_agency.*")] == ["_agency.worker.restart"]
    assert [e.name for e in registry.lookup_all("_*")] == ["_runtime", "_agency.worker.restart"]


def test_local_registry_register_b64_removed():
    """H10 (#34): the dead, key-dropping routing-table polluter is gone — verify
    keys live in KeyRtry only."""
    from civitas.registry import LocalRegistry
    from civitas.security.registry import KeyRegistry

    assert not hasattr(LocalRegistry, "register_b64")
    assert hasattr(KeyRegistry, "register_b64")  # the real home, untouched


# ---------------------------------------------------------------------------
# H2 (#30) — crash-queue serialization, staleness, and shutdown properties
# ---------------------------------------------------------------------------


class CountingCrasher(AgentProcess):
    """Counts on_start() invocations per name in a class-level dict."""

    starts: dict[str, int] = {}

    async def on_start(self) -> None:
        type(self).starts[self.name] = type(self).starts.get(self.name, 0) + 1

    async def handle(self, message: Message) -> Message | None:
        if message.payload.get("cmd") == "boom":
            raise RuntimeError("boom")
        return self.reply({"ok": True})


async def test_simultaneous_crash_events_one_restart_cycle():
    """Two crash events for the same ONE_FOR_ALL burst → exactly one restart
    cycle: the second event is stale (its incarnation task was already replaced)
    and must be skipped — the OTP EXIT-pid-matching analog."""
    CountingCrasher.starts = {}
    a = CountingCrasher("cc_a")
    b = CountingCrasher("cc_b")
    root = Supervisor(
        "root", children=[a, b], strategy="ONE_FOR_ALL", max_restarts=5, backoff_base=0.01
    )
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        assert CountingCrasher.starts == {"cc_a": 1, "cc_b": 1}
        # Enqueue both events in the same tick, before the drain task wakes —
        # deterministically modelling "both children crashed simultaneously".
        exc = RuntimeError("burst")
        root._crash_queue.put_nowait(("cc_a", exc, root._child_tasks["cc_a"]))
        root._crash_queue.put_nowait(("cc_b", exc, root._child_tasks["cc_b"]))

        assert await _wait_for(lambda: CountingCrasher.starts.get("cc_a", 0) >= 2)
        await asyncio.sleep(0.1)  # allow a (buggy) second cycle to happen
        # Exactly one ONE_FOR_ALL cycle: initial start + one restart each.
        assert CountingCrasher.starts == {"cc_a": 2, "cc_b": 2}
        assert a.status == ProcessStatus.RUNNING
        assert b.status == ProcessStatus.RUNNING
    finally:
        await runtime.stop()


async def test_two_independent_crashes_both_recover():
    """Serialization must not lose events: two ONE_FOR_ONE crashes queued
    back-to-back are both processed — both children recover."""
    a = CrashOnCommand("ind_a")
    b = CrashOnCommand("ind_b")
    root = Supervisor("root", children=[a, b], max_restarts=5, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("ind_a", {"cmd": "boom"})
        await runtime.send("ind_b", {"cmd": "boom"})
        assert await _wait_for(
            lambda: (
                root._restart_counts.get("ind_a", 0) >= 1
                and root._restart_counts.get("ind_b", 0) >= 1
            )
        )
        assert await _wait_for(
            lambda: a.status == ProcessStatus.RUNNING and b.status == ProcessStatus.RUNNING
        )
    finally:
        await runtime.stop()


async def test_stop_during_crash_drain_no_zombie():
    """stop() during a pending restart (backoff sleep) must not resurrect the
    child afterwards, and must tear the drain task down cleanly."""
    worker = CrashOnCommand("zombie")
    root = Supervisor("root", children=[worker], max_restarts=5, backoff_base=0.5)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("zombie", {"cmd": "boom"})
        # Crash observed (count bumped before the 0.5 s backoff sleep) — the
        # drain task is now mid-restart.
        assert await _wait_for(lambda: root._restart_counts.get("zombie", 0) >= 1)
    finally:
        await runtime.stop()

    assert worker.status != ProcessStatus.RUNNING, "child resurrected after stop()"
    assert root._crash_drain_task is None


# ---------------------------------------------------------------------------
# A6 / #31 — heartbeats must ride the priority channel
# ---------------------------------------------------------------------------


async def test_heartbeat_uses_priority_channel():
    sup = Supervisor("sup")
    sup.add_remote_child("remote", heartbeat_interval=0.01, heartbeat_timeout=0.05)

    seen: list[Message] = []

    class BusStub:
        async def request(self, message: Message, timeout: float) -> Message:
            seen.append(message)
            return Message(type="_agency.heartbeat_ack", sender="remote", recipient="sup")

    sup._bus = BusStub()  # type: ignore[assignment]
    sup._running = True
    await sup._start_heartbeat_monitor()
    try:
        assert await _wait_for(lambda: len(seen) >= 2, timeout=1.0)
    finally:
        sup._running = False
        await sup._stop_heartbeat_monitor()

    # System liveness probes must bypass the business mailbox: a SUSPENDED
    # agent drains only the priority queue, and a loaded agent must not have
    # its liveness conflated with queue depth.
    assert all(m.priority == 1 for m in seen), (
        f"heartbeats sent at priority {seen[0].priority} — they queue behind "
        "business messages and are buffered by suspended agents"
    )


async def test_suspended_agent_acks_priority_heartbeat():
    """A SUSPENDED agent is alive and must say so (H4, #31): its loop drains
    only the priority queue, so a priority-1 heartbeat reaches it and is acked
    while business messages stay buffered — suspension is a governance state,
    not a liveness failure."""
    worker = CrashOnCommand("paused")
    root = Supervisor("root", children=[worker], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.suspend("paused", reason="hitl gate")
        assert await _wait_for(lambda: worker.status == ProcessStatus.SUSPENDED)

        from civitas.messages import _uuid7

        heartbeat = Message(
            type="_agency.heartbeat",
            sender="root",
            recipient="paused",
            correlation_id=_uuid7(),
            priority=1,
        )
        ack = await runtime._bus.request(heartbeat, timeout=1.0)
        assert ack.type == "_agency.heartbeat_ack"
        assert worker.status == ProcessStatus.SUSPENDED  # still paused — ack ≠ resume
    finally:
        await runtime.stop()
