"""Regression tests tracking known actor-model gaps in the supervision core.

Each test asserts the *intended* (OTP-faithful) behavior and is marked
``xfail(strict=True)`` against the tracked GitHub issue. The suite therefore
stays green while the bugs exist, and a fix flips the test to XPASS — failing
the run and forcing the marker's removal, so the tracking can never go stale.

Findings catalog: docs/design/supervision-hardening.md
Issues: #28 (A2), #29 (A3), #30 (B2), #31 (A6), plus A1 (restart semantics,
design-level — no single issue; see the design doc).
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
    reason="A1: restart reuses the same AgentProcess instance — instance "
    "variables survive the crash (docs/design/supervision-hardening.md)",
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


@pytest.mark.xfail(
    reason="A1: un-checkpointed self.state survives restart — _restore_state() "
    "only overwrites when a checkpoint exists (docs/design/supervision-hardening.md)",
    strict=True,
)
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


@pytest.mark.xfail(
    reason="#28: _restart_child() returns early for Supervisor children — "
    "escalated subtree is never restarted under ONE_FOR_ONE",
    strict=True,
)
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
        # First wait until child_sup has observed the crash (and escalated) —
        # only then is "worker is RUNNING again" meaningful.
        assert await _wait_for(lambda: child_sup._restart_counts.get("worker", 0) >= 1)
        assert await _wait_for(lambda: worker.status == ProcessStatus.CRASHED, timeout=1.0)
        # OTP semantics: root restarts the escalated child_sup, which restarts
        # its children — the worker must come back RUNNING.
        assert await _wait_for(lambda: worker.status == ProcessStatus.RUNNING), (
            f"escalated subtree was never restarted — worker stayed {worker.status.value}"
        )
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# A3 / #29 — capabilities must survive a crash-restart
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="#29: _restart_child() re-registers bare — capabilities and "
    "capability_metadata are dropped after the first crash-restart",
    strict=True,
)
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


@pytest.mark.xfail(
    reason="#30: crash-handler task exceptions are never retrieved — a failed "
    "restart leaves the child dead with no log line",
    strict=True,
)
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
# A6 / #31 — heartbeats must ride the priority channel
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="#31: heartbeats are sent at priority 0 — buffered behind business "
    "messages and by SUSPENDED agents, causing false-positive crash detection",
    strict=True,
)
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
