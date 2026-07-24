"""Regression tests tracking actor-model gaps in the supervision core.

Fixed gaps keep their tests here as plain regressions (v0.8.0 PR1 flipped
#28/#29/#30). Remaining gaps stay ``xfail(strict=True)``: the suite is green
while the bug exists, and a fix flips the test to XPASS — failing the run and
forcing the marker's removal, so the tracking can never go stale.

Findings catalog: docs/design/supervision-hardening.md
As of v0.9.0 E2 every tracker is a plain regression — ZERO xfails remain:
the 2026-07 architecture review is fully closed in code. Waits observe the
CURRENT incarnation via runtime.get_agent (Q1: object refs go stale across
restarts by design — route by name).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from civitas.messages import Message
from civitas.process import AgentProcess, ProcessStatus
from civitas.runtime import Runtime
from civitas.supervisor import HeartbeatTimeout, Supervisor

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
        # Q1 (v0.9.0): observe the CURRENT incarnation — the old ref is stale.
        assert await _wait_for(lambda: runtime.get_agent("dirty").status == ProcessStatus.RUNNING)
        assert runtime.get_agent("dirty") is not agent  # fresh incarnation
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
        # Q1: current incarnation, not the stale pre-crash ref
        assert await _wait_for(
            lambda: runtime.get_agent("dirty_state").status == ProcessStatus.RUNNING
        )
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
        assert await _wait_for(  # Q1: current incarnation
            lambda: runtime.get_agent("cap_worker").status == ProcessStatus.RUNNING
        )

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
        assert await _wait_for(  # Q1: current incarnation
            lambda: runtime.get_agent("saver").status == ProcessStatus.RUNNING
        )
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
        assert await _wait_for(  # Q1: current incarnation
            lambda: runtime.get_agent("hanger").status == ProcessStatus.RUNNING
        )
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
    assert not root._pending_crash_events  # graceful stop produced no crash event


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
        root._enqueue_crash_event("cc_a", exc, root._child_tasks["cc_a"])
        root._enqueue_crash_event("cc_b", exc, root._child_tasks["cc_b"])

        assert await _wait_for(lambda: CountingCrasher.starts.get("cc_a", 0) >= 2)
        await asyncio.sleep(0.1)  # allow a (buggy) second cycle to happen
        # Exactly one ONE_FOR_ALL cycle: initial start + one restart each.
        assert CountingCrasher.starts == {"cc_a": 2, "cc_b": 2}
        # Q1: fresh incarnations — check the current objects
        assert runtime.get_agent("cc_a").status == ProcessStatus.RUNNING
        assert runtime.get_agent("cc_b").status == ProcessStatus.RUNNING
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
        assert await _wait_for(  # Q1: current incarnations
            lambda: (
                runtime.get_agent("ind_a").status == ProcessStatus.RUNNING
                and runtime.get_agent("ind_b").status == ProcessStatus.RUNNING
            )
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
    # D-E4-8: the supervisor's OWN loop (crash-processing now rides it) is
    # what gets torn down cleanly, replacing the old drain-task check.
    assert root._task is not None and root._task.done()


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


# ---------------------------------------------------------------------------
# D1a (v0.9.0 E2) — fresh-incarnation restart edge inventory
# ---------------------------------------------------------------------------


async def test_fresh_incarnation_is_fully_rewired():
    """The new incarnation carries every injected dependency (llm/tools/store/
    credentials/metrics) — wire-fully-before-start, design §4 constraint 1."""
    from civitas.plugins.tools import ToolRegistry

    llm, tools = object(), ToolRegistry()
    agent = CrashOnCommand("wired")
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root, model_provider=llm, tool_registry=tools)
    runtime._agent_credentials = {"wired": {"anthropic": "sk-test"}}
    await runtime.start()
    try:
        assert agent.llm is llm and agent._credentials == {"anthropic": "sk-test"}
        await runtime.send("wired", {"cmd": "boom"})
        assert await _wait_for(
            lambda: (
                runtime.get_agent("wired") is not agent
                and runtime.get_agent("wired").status == ProcessStatus.RUNNING
            )
        )
        fresh = runtime.get_agent("wired")
        assert fresh.llm is llm
        assert fresh.tools is tools
        assert fresh.store is not None
        assert fresh._credentials == {"anthropic": "sk-test"}
        assert fresh._metrics is agent._metrics and fresh._audit_sink is agent._audit_sink
    finally:
        await runtime.stop()


async def test_mailbox_carries_over_in_order():
    """Messages queued behind the poison one are processed by the FRESH
    incarnation, in order (design §4 constraint 2)."""
    CountingCrasher.starts = {}

    class OrderRecorder(AgentProcess):
        seen: list[str] = []  # class-level: survives incarnations

        async def handle(self, message: Message) -> Message | None:
            if message.payload.get("cmd") == "boom":
                raise RuntimeError("boom")
            type(self).seen.append(message.payload.get("tag", ""))
            return None

    OrderRecorder.seen = []
    agent = OrderRecorder("keeper")
    root = Supervisor("root", children=[agent], max_restarts=3, backoff_base=0.05)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("keeper", {"cmd": "boom"})
        for tag in ("a", "b", "c"):  # these land while the crash/backoff runs
            await runtime.send("keeper", {"tag": tag})
        assert await _wait_for(lambda: len(OrderRecorder.seen) >= 3)
        assert OrderRecorder.seen == ["a", "b", "c"]
        assert runtime.get_agent("keeper") is not agent  # processed by the fresh one
    finally:
        await runtime.stop()


async def test_suspended_child_restarts_into_suspended_fresh_instance():
    """S7 × D1a: the checkpointed marker restores SUSPENDED on a NEW object."""
    worker = CrashOnCommand("paused_fresh")
    root = Supervisor("root", children=[worker], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.suspend("paused_fresh", reason="gate")
        assert await _wait_for(lambda: worker.status == ProcessStatus.SUSPENDED)
        # White-box: drive the restart machinery directly (a suspended agent
        # processes no business messages, so we can't crash it organically).
        await root._restart_agent_child(worker)
        fresh = runtime.get_agent("paused_fresh")
        assert fresh is not worker
        assert await _wait_for(lambda: fresh.status == ProcessStatus.SUSPENDED)
    finally:
        await runtime.stop()


async def test_wire_failure_is_loud_restart_failure(caplog):
    """A wiring failure escalates through the H2 loud path — never a
    half-wired child with a live task (design §4 constraint 1)."""
    import logging as _logging

    worker = CrashOnCommand("unwirable")
    root = Supervisor("root", children=[worker], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:

        def bad_wire(agent):
            raise RuntimeError("injection infrastructure down")

        root._wire_child = bad_wire
        with caplog.at_level(_logging.WARNING):
            await runtime.send("unwirable", {"cmd": "boom"})
            await asyncio.sleep(0.3)
        assert any(
            "unwirable" in r.getMessage() and r.levelno >= _logging.WARNING for r in caplog.records
        )
        assert runtime.get_agent("unwirable").status != ProcessStatus.RUNNING
    finally:
        await runtime.stop()


async def test_restart_of_restart_works():
    """The fresh incarnation re-captures its own spec — crash twice, recover twice."""
    agent = CrashOnCommand("phoenix")
    root = Supervisor("root", children=[agent], max_restarts=5, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        for expected in (1, 2):
            await runtime.send("phoenix", {"cmd": "boom"})
            assert await _wait_for(lambda n=expected: root._restart_counts.get("phoenix", 0) >= n)
            assert await _wait_for(
                lambda: runtime.get_agent("phoenix").status == ProcessStatus.RUNNING
            )
        second = runtime.get_agent("phoenix")
        assert second is not agent
        cls, args, kwargs = second._civitas_spec  # re-captured on the incarnation
        assert cls is CrashOnCommand and args == ("phoenix",)
        reply = await runtime.ask("phoenix", {"cmd": "work"}, timeout=2.0)
        assert reply.payload["ok"] is True
    finally:
        await runtime.stop()


class SpawnedJob(AgentProcess):
    """Module-level: spawn() resolves classes by dotted import path."""

    instances = 0

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        type(self).instances += 1

    async def handle(self, message: Message) -> Message | None:
        if message.payload.get("cmd") == "boom":
            raise RuntimeError("boom")
        return self.reply({"job": self.config.get("job_id")})


async def test_dynamic_child_fresh_restart_preserves_config():
    """DynSup restart path: fresh incarnation, spawn-time config carried over."""
    from civitas import DynamicSupervisor

    Job = SpawnedJob
    Job.instances = 0
    dyn = DynamicSupervisor("pool", restart="permanent", max_restarts=3)
    runtime = Runtime(supervisor=Supervisor("root", children=[dyn]))
    await runtime.start()
    try:
        await runtime.spawn("pool", Job, "job-1", config={"job_id": 42})
        assert Job.instances == 1
        await runtime.send("job-1", {"cmd": "boom"})
        assert await _wait_for(lambda: Job.instances >= 2)  # fresh incarnation built
        reply = await runtime.ask("job-1", {"cmd": "check"}, timeout=2.0)
        assert reply.payload["job"] == 42  # spawn-time config carried over
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# D5 (v0.9.0 E3) — per-process liveness: A6's false-positive finally dies
# ---------------------------------------------------------------------------


def _channel_registry(mapping: dict[str, str]):
    from civitas.registry import LocalRegistry

    registry = LocalRegistry()
    for agent_name, channel in mapping.items():
        registry.register_remote(agent_name, health_channel=channel)
    return registry


async def test_busy_remote_agent_is_not_restarted():
    """THE A6 VICTORY TEST. A remote agent stuck in a long handle() cannot ack
    per-agent pings — but the PROCESS answers the health probe with a snapshot
    showing it RUNNING with a live task. No crash, no restart. Under the
    pre-v0.9 per-agent scheme this exact configuration force-restarted a
    healthy agent after interval x threshold."""
    sup = Supervisor("sup")
    sup.add_remote_child(
        "busy", heartbeat_interval=0.01, heartbeat_timeout=0.05, missed_heartbeats_threshold=3
    )
    sup._registry = _channel_registry({"busy": "_agency.worker.w1.health"})
    sup._running = True

    probes: list[Message] = []

    class BusStub:
        async def request(self, message: Message, timeout: float) -> Message:
            probes.append(message)
            if message.recipient == "busy":
                raise TimeoutError  # the agent itself would NEVER answer — it's busy
            return Message(
                type="_agency.health_ack",
                sender=message.recipient,
                recipient=sup.name,
                payload={
                    "worker_id": "w1",
                    "agents": {
                        "busy": {"status": "RUNNING", "task_alive": True, "mailbox_depth": 7}
                    },
                },
            )

    sup._bus = BusStub()  # type: ignore[assignment]
    await sup._start_heartbeat_monitor()
    await asyncio.sleep(0.15)  # >> interval x threshold — old scheme would have fired
    sup._running = False
    await sup._stop_heartbeat_monitor()

    assert all(p.recipient == "_agency.worker.w1.health" for p in probes)  # process, not agent
    assert not sup._pending_crash_events, "healthy-but-busy agent was declared crashed"


async def test_dead_remote_task_detected_in_one_probe():
    """Fast remote crash detection: process healthy, THIS child's task dead —
    crash enqueued from a single ack, no starvation cycle."""
    sup = Supervisor("sup")
    sup.add_remote_child("victim", heartbeat_interval=0.01, heartbeat_timeout=0.05)
    sup.add_remote_child("fine", heartbeat_interval=0.01, heartbeat_timeout=0.05)
    channel = "_agency.worker.w1.health"
    sup._registry = _channel_registry({"victim": channel, "fine": channel})

    class BusStub:
        async def request(self, message: Message, timeout: float) -> Message:
            return Message(
                type="_agency.health_ack",
                sender=channel,
                recipient=sup.name,
                payload={
                    "worker_id": "w1",
                    "agents": {
                        "victim": {"status": "CRASHED", "task_alive": False, "mailbox_depth": 0},
                        "fine": {"status": "RUNNING", "task_alive": True, "mailbox_depth": 0},
                    },
                },
            )

    sup._bus = BusStub()  # type: ignore[assignment]
    await sup._probe_health_channel(channel, ["victim", "fine"])

    assert len(sup._pending_crash_events) == 1
    name, exc, task = next(iter(sup._pending_crash_events.values()))
    assert name == "victim" and isinstance(exc, HeartbeatTimeout)


async def test_unreachable_process_crashes_all_its_children():
    sup = Supervisor("sup")
    channel = "_agency.worker.w1.health"
    for n in ("a", "b"):
        sup.add_remote_child(n, heartbeat_timeout=0.01, missed_heartbeats_threshold=2)
    sup._registry = _channel_registry({"a": channel, "b": channel})

    class DeadBus:
        async def request(self, message: Message, timeout: float) -> Message:
            raise TimeoutError

    sup._bus = DeadBus()  # type: ignore[assignment]
    await sup._probe_health_channel(channel, ["a", "b"])  # miss 1
    assert not sup._pending_crash_events
    await sup._probe_health_channel(channel, ["a", "b"])  # miss 2 = threshold
    crashed = {v[0] for v in sup._pending_crash_events.values()}
    assert crashed == {"a", "b"}


async def test_legacy_worker_falls_back_to_per_agent_pings():
    """Q2 skew: no announced channel -> the pre-v0.9 per-agent path, verbatim."""
    sup = Supervisor("sup")
    sup.add_remote_child("old_style", heartbeat_interval=0.01, heartbeat_timeout=0.05)
    sup._registry = _channel_registry({})  # registered nowhere / no channel
    sup._running = True

    seen: list[Message] = []

    class BusStub:
        async def request(self, message: Message, timeout: float) -> Message:
            seen.append(message)
            return Message(type="_agency.heartbeat_ack", sender="old_style", recipient=sup.name)

    sup._bus = BusStub()  # type: ignore[assignment]
    await sup._start_heartbeat_monitor()
    await asyncio.sleep(0.05)
    sup._running = False
    await sup._stop_heartbeat_monitor()

    assert seen and all(
        m.recipient == "old_style" and m.type == "_agency.heartbeat" and m.priority == 1
        for m in seen
    )


# ---------------------------------------------------------------------------
# D6 / v0.9.0 E4 Phase B — Supervisor actorization, Halt-Check B named proofs
# (design supervision-endgame.md §6.1 D-E4-7)
# ---------------------------------------------------------------------------


async def test_bare_supervisor_crash_delivery_without_bus():
    """Halt-Check B proof #1 (D-E4-7): a bare Supervisor (no bus, no Runtime)
    still delivers a real child crash end-to-end through its OWN mailbox —
    _on_child_done -> side-table -> put_nowait -> own message loop -> handle()
    -> _process_crash_event -> _handle_crash -> restart. Self-delivery is
    local Mailbox traffic, not transport traffic (confirmed safe by Phase A's
    standalone-loop finding) — this is the direct evidence, and it grounds
    the D-E4-7 test-authoring heuristic: bare-Supervisor tests that only
    assert on handling logic can keep calling ``_handle_crash`` directly,
    because THIS test proves the delivery path around it works without a bus.
    """
    worker = CrashOnCommand("bare_worker")
    sup = Supervisor("sup", children=[worker], max_restarts=3, backoff_base=0.01)
    assert sup._bus is None  # bare — no Runtime, no bus wiring
    await sup.start()
    try:
        await worker._mailbox.put(
            Message(
                type="test.crash", sender="test", recipient="bare_worker", payload={"cmd": "boom"}
            )
        )
        assert await _wait_for(lambda: sup._restart_counts.get("bare_worker", 0) >= 1)
        assert await _wait_for(lambda: sup._children_by_name["bare_worker"] is not worker)
        fresh = sup._children_by_name["bare_worker"]
        assert await _wait_for(lambda: fresh.status == ProcessStatus.RUNNING)
    finally:
        await sup.stop()


async def test_no_resurrection_after_stop_during_backoff():
    """Halt-Check B proof #2 (D-E4-8): stop() during an in-flight restart's
    backoff sleep must not let that restart complete afterwards. Only
    cancelling the Supervisor's own loop (self._stop()'s timeout-then-cancel
    fallback) can abort a sleep already in progress — a flag check cannot,
    which is why stop() reorders to stop its own loop FIRST (D-E4-8), not
    last as Phase A had it. ``_shutdown_timeout`` is shortened so the
    cancel-on-timeout fallback fires quickly instead of waiting out the
    (deliberately much longer) backoff sleep.
    """
    worker = CrashOnCommand("zombie")
    sup = Supervisor("sup", children=[worker], max_restarts=5, backoff_base=5.0)
    sup._shutdown_timeout = 0.05
    assert sup._bus is None
    await sup.start()
    try:
        await worker._mailbox.put(
            Message(type="test.crash", sender="test", recipient="zombie", payload={"cmd": "boom"})
        )
        # Restart count bumps before the 5s backoff sleep — mid-restart now.
        assert await _wait_for(lambda: sup._restart_counts.get("zombie", 0) >= 1)
    finally:
        await sup.stop()

    assert sup._children_by_name["zombie"] is worker, "child resurrected after stop()"
    assert sup._task is not None and sup._task.done()
