"""Durable suspension — interleaving coverage for agent.suspend()/resume().

Covers the FINAL DESIGN decisions S1–S10 and the "Additional holes" #1–#10 from
docs/design/durable-suspension.md: the priority-only mailbox wait (footgun #10),
the non-blocking boundary transition (S2), priority-only drain while suspended
(S3), the durable marker (S4), write-ahead ordering (S5), approver-gated resume
(S6), supervisor/lifecycle handling (S7), marker lifecycle (S8), and the
restore-into-SUSPENDED path.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from civitas import AgentProcess, DynamicSupervisor, Runtime, Supervisor
from civitas.errors import ErrorAction
from civitas.messages import Message
from civitas.plugins.state import InMemoryStateStore
from civitas.process import Mailbox, ProcessStatus, SuspendCategory
from civitas.supervisor import _ChildRec
from tests.conftest import EchoAgent, wait_for, wait_for_status

MARKER = AgentProcess._SUSPEND_STATE_KEY


# ---------------------------------------------------------------------------
# Test agents and helpers
# ---------------------------------------------------------------------------


class RecorderAgent(AgentProcess):
    """Records handled message types and suspend/resume hook invocations."""

    def __init__(self, name: str = "recorder") -> None:
        super().__init__(name)
        self.handled: list[str] = []
        self.suspended_reasons: list[str] = []
        self.resume_approvers: list[str] = []

    async def handle(self, message: Message) -> Message | None:
        self.handled.append(message.type)
        return None

    async def on_suspend(self, reason: str) -> None:
        self.suspended_reasons.append(reason)

    async def on_resume(self, approver: str) -> None:
        self.resume_approvers.append(approver)


class SelfSuspendAgent(RecorderAgent):
    """Suspends itself from inside handle() when it sees a trigger message."""

    async def handle(self, message: Message) -> Message | None:
        self.handled.append(message.type)
        if message.type == "please_suspend":
            await self.suspend("self-requested")
        return None


class SelfSuspendForApprovalAgent(RecorderAgent):
    """v0.9.4: suspends itself via suspend_for_approval() -- the direct
    convenience-wrapper API, not the _agency.suspend wire message -- when it
    sees a trigger message. Self-suspension is what makes a DIRECT method
    call (rather than a message) actually observable: calling suspend() from
    OUTSIDE an idling agent never wakes its message loop to check the
    boundary flag, but calling it from INSIDE handle() naturally does, at
    the very next loop iteration after this dispatch completes.
    """

    async def handle(self, message: Message) -> Message | None:
        self.handled.append(message.type)
        if message.type == "please_suspend_for_approval":
            await self.suspend_for_approval("awaiting spend approval")
        return None


class CrashOnMessageAgent(AgentProcess):
    """Escalates on the first message so a supervisor sees a crash."""

    async def handle(self, message: Message) -> Message | None:
        raise RuntimeError("intentional crash")

    async def on_error(self, error: Exception, message: Message) -> ErrorAction:
        return ErrorAction.ESCALATE


async def _suspend_via_message(
    agent: AgentProcess, reason: str = "", category: SuspendCategory | None = None
) -> None:
    payload: dict[str, Any] = {"reason": reason}
    if category is not None:
        payload["category"] = category.value
    await agent._mailbox.put(Message(type="_agency.suspend", payload=payload, priority=1))
    await wait_for_status(agent, ProcessStatus.SUSPENDED)


async def _resume_via_message(agent: AgentProcess, approver: str = "approver-1") -> None:
    await agent._mailbox.put(
        Message(type="_agency.resume", payload={"approver": approver}, priority=1)
    )
    await wait_for_status(agent, ProcessStatus.RUNNING)


def _suspended_store(**extra: Any) -> AsyncMock:
    """A mock StateStore whose checkpoint carries a suspend marker."""
    store = AsyncMock()
    store.get.return_value = {MARKER: {"reason": "r", "since": 1.0, "approver": None}, **extra}
    return store


# ---------------------------------------------------------------------------
# Mailbox.get_priority — priority-only wait (S3, footgun #10)
# ---------------------------------------------------------------------------


async def test_get_priority_returns_priority_message():
    """get_priority() returns a queued priority message immediately."""
    mb = Mailbox()
    await mb.put(Message(type="ctrl", priority=1))
    assert (await mb.get_priority()).type == "ctrl"


async def test_get_priority_ignores_normal_and_leaves_it_buffered():
    """A normal message is neither returned nor consumed by get_priority (S3)."""
    mb = Mailbox()
    await mb.put(Message(type="biz", priority=0))
    task = asyncio.create_task(mb.get_priority())
    await asyncio.sleep(0.05)
    assert not task.done()  # business message is ignored, get_priority keeps waiting

    await mb.put(Message(type="ctrl", priority=1))
    result = await asyncio.wait_for(task, timeout=1.0)
    assert result.type == "ctrl"
    assert (await mb.get()).type == "biz"  # business message preserved for resume


async def test_get_priority_normal_put_does_not_wake_return():
    """A normal put during the wait is a bounded spurious wakeup, not a return (footgun #10)."""
    mb = Mailbox()
    task = asyncio.create_task(mb.get_priority())
    await asyncio.sleep(0.02)

    await mb.put(Message(type="biz", priority=0))  # sets the shared _notify event
    await asyncio.sleep(0.05)
    assert not task.done()  # re-checked priority queue, re-waited — no lost/false wakeup

    await mb.put(Message(type="ctrl", priority=1))
    result = await asyncio.wait_for(task, timeout=1.0)
    assert result.type == "ctrl"


async def test_get_priority_discards_expired(caplog: Any) -> None:
    """An expired priority message is discarded, not returned (F01-3 interaction)."""
    mb = Mailbox()
    await mb.put(Message(type="stale", priority=1, timestamp=0.0, ttl=1.0))
    await mb.put(Message(type="fresh", priority=1))
    with caplog.at_level("WARNING"):
        result = await mb.get_priority()
    assert result.type == "fresh"
    assert any("discarding expired message" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Non-blocking self-suspension at the loop boundary (S2)
# ---------------------------------------------------------------------------


async def test_self_suspend_is_non_blocking_and_transitions_at_boundary():
    """self.suspend() from handle() returns immediately; the loop transitions after (S2)."""
    agent = SelfSuspendAgent("self")
    await agent._start()
    try:
        await agent._mailbox.put(Message(type="please_suspend"))
        await wait_for_status(agent, ProcessStatus.SUSPENDED)
        assert agent.handled == ["please_suspend"]  # handle() completed, did not block
        assert agent.suspended_reasons == ["self-requested"]
    finally:
        await agent._stop()


async def test_suspend_writes_marker_into_state_via_checkpoint():
    """Suspend records the marker inside self.state in one atomic checkpoint (S4)."""
    agent = RecorderAgent("markered")
    store = AsyncMock()
    store.get.return_value = None
    agent.store = store
    await agent._start()
    try:
        await _suspend_via_message(agent, reason="awaiting approval")
        marker = agent.state[MARKER]
        assert marker["reason"] == "awaiting approval"
        assert marker["approver"] is None
        assert "since" in marker
        # the marker is persisted through the normal checkpoint() path
        assert any(call.args[1] is agent.state for call in store.set.await_args_list)
    finally:
        await agent._stop()


async def test_external_suspend_via_priority_message_transitions():
    """An _agency.suspend priority message drives the transition (S2/S10)."""
    agent = RecorderAgent("ext")
    await agent._start()
    try:
        await _suspend_via_message(agent, reason="policy")
        assert agent.status == ProcessStatus.SUSPENDED
        assert agent.suspended_reasons == ["policy"]
    finally:
        await agent._stop()


# ---------------------------------------------------------------------------
# Priority-only drain + business buffering while suspended (S3, hole #8)
# ---------------------------------------------------------------------------


async def test_business_messages_buffer_and_deliver_fifo_on_resume():
    """Business traffic buffers during suspension and is delivered in FIFO on resume (S3)."""
    agent = RecorderAgent("buffered")
    await agent._start()
    try:
        await _suspend_via_message(agent)
        for t in ("b1", "b2", "b3"):
            await agent._mailbox.put(Message(type=t, priority=0))
        await asyncio.sleep(0.05)
        assert agent.handled == []  # nothing dispatched while suspended

        await _resume_via_message(agent, approver="alice")
        await wait_for(lambda: agent.handled == ["b1", "b2", "b3"])
    finally:
        await agent._stop()


async def test_buffered_business_ttl_expires_on_resume(caplog: Any) -> None:
    """A buffered business message can ttl-expire and be dropped on resume (hole #6)."""
    agent = RecorderAgent("ttl")
    await agent._start()
    try:
        await _suspend_via_message(agent)
        await agent._mailbox.put(Message(type="stale", priority=0, timestamp=0.0, ttl=1.0))
        await agent._mailbox.put(Message(type="fresh", priority=0))
        with caplog.at_level("WARNING"):
            await _resume_via_message(agent)
            await wait_for(lambda: "fresh" in agent.handled)
        assert "stale" not in agent.handled
    finally:
        await agent._stop()


# ---------------------------------------------------------------------------
# Resume authorization (S6, hole #4)
# ---------------------------------------------------------------------------


async def test_resume_requires_non_empty_approver():
    """resume() with an empty approver raises ValueError (S6)."""
    agent = RecorderAgent("auth")
    await agent._start()
    try:
        await _suspend_via_message(agent)
        with pytest.raises(ValueError, match="approver"):
            await agent.resume("")
        assert agent.status == ProcessStatus.SUSPENDED  # rejected, still paused
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


async def test_resume_clears_marker_and_fires_hook_with_approver():
    """resume() clears the marker, returns to RUNNING, and reports the approver (S6)."""
    agent = RecorderAgent("resumer")
    await agent._start()
    try:
        await _suspend_via_message(agent)
        await _resume_via_message(agent, approver="carol")
        assert agent.status == ProcessStatus.RUNNING
        assert MARKER not in agent.state
        assert agent.resume_approvers == ["carol"]
    finally:
        await agent._stop()


async def test_empty_approver_resume_message_is_ignored_not_crash(caplog: Any) -> None:
    """An _agency.resume with an empty approver is logged and ignored, never crashes the loop."""
    agent = RecorderAgent("safe")
    await agent._start()
    try:
        await _suspend_via_message(agent)
        with caplog.at_level("WARNING"):
            await agent._mailbox.put(
                Message(type="_agency.resume", payload={"approver": ""}, priority=1)
            )
            await asyncio.sleep(0.05)
        assert agent.status == ProcessStatus.SUSPENDED  # still paused, loop alive
        assert any("empty approver" in r.message for r in caplog.records)
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


# ---------------------------------------------------------------------------
# Idempotency (S10, hole #7)
# ---------------------------------------------------------------------------


async def test_double_suspend_keeps_since_updates_reason():
    """Suspending a suspended agent keeps the original since and updates the reason (S10)."""
    agent = RecorderAgent("idem")
    await agent._start()
    try:
        await _suspend_via_message(agent, reason="first")
        original_since = agent.state[MARKER]["since"]
        await asyncio.sleep(0.01)

        await agent._mailbox.put(
            Message(type="_agency.suspend", payload={"reason": "second"}, priority=1)
        )
        await wait_for(lambda: agent.state[MARKER]["reason"] == "second")
        assert agent.state[MARKER]["since"] == original_since
        assert agent.suspended_reasons == ["first"]  # on_suspend fired once only
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


async def test_resume_of_not_suspended_is_noop_but_requires_approver():
    """Resuming a running agent is a no-op, yet still validates the approver (S10)."""
    agent = RecorderAgent("noop")
    await agent._start()
    try:
        with pytest.raises(ValueError, match="approver"):
            await agent.resume("")
        await agent.resume("alice")  # valid approver, agent not suspended
        assert agent.status == ProcessStatus.RUNNING
        assert agent.resume_approvers == []  # on_resume not fired — nothing to resume
    finally:
        await agent._stop()


# ---------------------------------------------------------------------------
# Write-ahead ordering (S5, hole #3)
# ---------------------------------------------------------------------------


async def test_suspend_persist_failure_stays_suspended(caplog: Any) -> None:
    """A failed marker persist leaves the agent paused, never RUNNING (S5)."""
    agent = RecorderAgent("degraded")
    store = AsyncMock()
    store.get.return_value = None
    store.set.side_effect = RuntimeError("disk full")
    agent.store = store
    await agent._start()
    try:
        with caplog.at_level("WARNING"):
            await _suspend_via_message(agent)
        assert agent.status == ProcessStatus.SUSPENDED  # paused in-memory first (S5)
        assert MARKER in agent.state  # marker set even though persistence failed
        assert any("durability degraded" in r.message for r in caplog.records)
    finally:
        store.set.side_effect = None
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


# ---------------------------------------------------------------------------
# on_suspend/on_resume semantics (hole #9)
# ---------------------------------------------------------------------------


async def test_on_suspend_raising_leaves_agent_suspended(caplog: Any) -> None:
    """If on_suspend() raises mid-transition, the agent stays SUSPENDED (hole #9)."""

    class SuspendRaises(RecorderAgent):
        async def on_suspend(self, reason: str) -> None:
            raise RuntimeError("hook boom")

    agent = SuspendRaises("hooky")
    await agent._start()
    try:
        with caplog.at_level("ERROR"):
            await _suspend_via_message(agent)
        assert agent.status == ProcessStatus.SUSPENDED
        assert MARKER in agent.state
        assert any("on_suspend() raised" in r.message for r in caplog.records)
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


# ---------------------------------------------------------------------------
# Restore-into-SUSPENDED (S4/S7, hole #1)
# ---------------------------------------------------------------------------


async def test_restore_into_suspended_no_hook_no_hang():
    """A restored marker brings the agent up SUSPENDED without firing on_suspend (S7)."""
    agent = RecorderAgent("restored")
    agent.store = _suspended_store(counter=5)
    # _start() must not hang even though the loop enters SUSPENDED (hole #1)
    await asyncio.wait_for(agent._start(), timeout=2.0)
    try:
        assert agent.status == ProcessStatus.SUSPENDED
        assert agent.state["counter"] == 5  # user state restored alongside the marker
        assert agent.suspended_reasons == []  # restore is not a fresh suspend (S7)
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


async def test_restore_running_when_no_marker():
    """Without a marker the agent restores straight into RUNNING."""
    agent = RecorderAgent("normal")
    store = AsyncMock()
    store.get.return_value = {"counter": 1}
    agent.store = store
    await agent._start()
    try:
        assert agent.status == ProcessStatus.RUNNING
    finally:
        await agent._stop()


async def test_restored_suspended_agent_resumes_and_drains():
    """A restored-suspended agent resumes correctly and drains its buffered work."""
    agent = RecorderAgent("restored2")
    agent.store = _suspended_store()
    await agent._start()
    try:
        await agent._mailbox.put(Message(type="queued", priority=0))
        await asyncio.sleep(0.05)
        assert agent.handled == []  # still suspended after restore
        await _resume_via_message(agent, approver="alice")
        await wait_for(lambda: "queued" in agent.handled)
    finally:
        await agent._stop()


# ---------------------------------------------------------------------------
# SuspendCategory (v0.9.4, dashboard-v2.md §6/§18) — HITL-wait vs governance
# ---------------------------------------------------------------------------


async def test_suspend_default_category_is_other():
    """Backward compatibility: an existing caller passing only reason= lands
    in OTHER, today's only category in effect -- unaffected by this addition."""
    agent = RecorderAgent("default-cat")
    await agent._start()
    try:
        await _suspend_via_message(agent, reason="ops pause")
        assert agent.suspend_category == SuspendCategory.OTHER.value
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


async def test_suspend_for_approval_sets_hitl_category():
    """The convenience wrapper -- "a subset of suspend/resume" -- categorizes
    correctly without the caller needing to know about SuspendCategory at all.

    Exercised via self-suspension (SelfSuspendForApprovalAgent), the correct
    pattern for a DIRECT method call, not the _agency.suspend wire message --
    calling suspend_for_approval() from OUTSIDE an idling agent would never
    wake its loop to check the boundary flag at all (found while writing this
    test: my first attempt called it directly from outside and the agent
    never actually transitioned, confirmed by a real TimeoutError, not
    assumed).
    """
    agent = SelfSuspendForApprovalAgent("hitl")
    await agent._start()
    try:
        await agent._mailbox.put(Message(type="please_suspend_for_approval"))
        await wait_for_status(agent, ProcessStatus.SUSPENDED)
        assert agent.suspend_category == SuspendCategory.HITL_APPROVAL.value
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


async def test_suspend_category_survives_a_real_restore():
    """The actual point of persisting category in the durable marker, not just
    the in-memory _suspend_category attribute: an approval pending across a
    crash/redeploy must still show as HITL_APPROVAL after restore, not
    silently reset to OTHER/grey. A FRESH agent instance restoring from a
    real marker dict -- not the same object that called suspend() -- proving
    this is genuinely restore-safe, not just "works because it's the same
    Python object".
    """
    agent = RecorderAgent("restored-hitl")
    store = AsyncMock()
    store.get.return_value = {
        MARKER: {
            "reason": "r",
            "since": 1.0,
            "approver": None,
            "category": SuspendCategory.HITL_APPROVAL.value,
        }
    }
    agent.store = store
    await asyncio.wait_for(agent._start(), timeout=2.0)
    try:
        assert agent.status == ProcessStatus.SUSPENDED
        assert agent.suspend_category == SuspendCategory.HITL_APPROVAL.value
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


async def test_suspend_category_defaults_to_other_for_a_marker_that_predates_it():
    """A marker persisted before v0.9.4 (no 'category' key at all) must not
    crash -- defaults to OTHER, matching every other backward-compatibility
    default in this feature."""
    agent = RecorderAgent("pre-v094-marker")
    agent.store = _suspended_store()  # no "category" key, matching the OLD marker shape
    await asyncio.wait_for(agent._start(), timeout=2.0)
    try:
        assert agent.status == ProcessStatus.SUSPENDED
        assert agent.suspend_category == SuspendCategory.OTHER.value
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


async def test_suspend_category_not_suspended_at_all_is_other():
    """A never-suspended agent's suspend_category is OTHER -- a plain,
    always-safe default, not a crash or a None the caller has to guard."""
    agent = RecorderAgent("never-suspended")
    await agent._start()
    try:
        assert agent.status == ProcessStatus.RUNNING
        assert agent.suspend_category == SuspendCategory.OTHER.value
    finally:
        await agent._stop()


async def test_agency_suspend_message_carries_category_over_the_wire():
    """The cross-process/by-name entry point (Runtime.suspend(), which sends
    this exact message shape) needs category to actually reach the target
    agent -- not just the direct same-process suspend()/suspend_for_approval()
    calls."""
    agent = RecorderAgent("wire-category")
    await agent._start()
    try:
        await agent._mailbox.put(
            Message(
                type="_agency.suspend",
                payload={"reason": "policy hold", "category": SuspendCategory.HITL_APPROVAL.value},
                priority=1,
            )
        )
        await wait_for_status(agent, ProcessStatus.SUSPENDED)
        assert agent.suspend_category == SuspendCategory.HITL_APPROVAL.value
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


async def test_agency_suspend_message_without_category_field_is_other():
    """An older sender (or a real Presidium not yet aware of categories) that
    never sends a 'category' field at all keeps working unchanged."""
    agent = RecorderAgent("wire-no-category")
    await agent._start()
    try:
        await agent._mailbox.put(
            Message(type="_agency.suspend", payload={"reason": "ops pause"}, priority=1)
        )
        await wait_for_status(agent, ProcessStatus.SUSPENDED)
        assert agent.suspend_category == SuspendCategory.OTHER.value
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


async def test_re_suspend_of_already_suspended_agent_updates_category():
    """The idempotent re-suspend path (S10, _update_suspend_reason) also
    updates category when the second suspend request carries one."""
    agent = RecorderAgent("re-suspend-cat")
    await agent._start()
    try:
        await _suspend_via_message(agent, reason="first")
        assert agent.suspend_category == SuspendCategory.OTHER.value

        await agent._mailbox.put(
            Message(
                type="_agency.suspend",
                payload={
                    "reason": "now awaiting approval",
                    "category": SuspendCategory.HITL_APPROVAL.value,
                },
                priority=1,
            )
        )
        await wait_for(lambda: agent.suspend_category == SuspendCategory.HITL_APPROVAL.value)
    finally:
        await _resume_via_message(agent, approver="alice")
        await agent._stop()


# ---------------------------------------------------------------------------
# Observability + audit (S9)
# ---------------------------------------------------------------------------


async def test_suspend_and_resume_emit_audit_events():
    """Suspend and resume each emit an AuditEvent; resume records the approver (S9)."""
    agent = RecorderAgent("audited")
    sink = AsyncMock()
    agent._audit_sink = sink
    await agent._start()
    try:
        await _suspend_via_message(agent, reason="policy")
        await _resume_via_message(agent, approver="dave")
        events = [call.args[0]["event"] for call in sink.emit.await_args_list]
        assert "agent.suspend" in events
        assert "agent.resume" in events
        resume_event = next(
            call.args[0]
            for call in sink.emit.await_args_list
            if call.args[0]["event"] == "agent.resume"
        )
        assert resume_event["details"]["approver"] == "dave"
    finally:
        await agent._stop()


async def test_suspend_resume_emit_lifecycle_spans():
    """Suspend/resume emit civitas.agent.suspend / civitas.agent.resume spans (S9)."""
    agent = RecorderAgent("spanned")
    tracer = MagicMock()
    tracer.start_span.return_value = MagicMock()
    agent._tracer = tracer
    await agent._start()
    try:
        await _suspend_via_message(agent)
        await _resume_via_message(agent, approver="erin")
        span_names = [call.args[0] for call in tracer.start_span.call_args_list]
        assert "civitas.agent.suspend" in span_names
        assert "civitas.agent.resume" in span_names
    finally:
        await agent._stop()


# ---------------------------------------------------------------------------
# Supervisor / lifecycle (S7)
# ---------------------------------------------------------------------------


async def test_supervisor_stop_stops_suspended_child():
    """_stop() actually stops a SUSPENDED child rather than leaking it (S7)."""
    victim = RecorderAgent("victim")
    sup = Supervisor("root", children=[victim])
    await sup.start()
    await _suspend_via_message(victim)
    await sup.stop()
    assert victim.status == ProcessStatus.STOPPED
    assert victim._task is not None and victim._task.done()


async def test_one_for_all_restart_stops_suspended_no_double_loop():
    """ONE_FOR_ALL stops-then-restarts a suspended sibling with exactly one live loop (S7)."""
    crasher = CrashOnMessageAgent("crasher")
    victim = RecorderAgent("victim")
    # H5b: only checkpointed state survives a restart — the suspend marker needs
    # a store to persist across the ONE_FOR_ALL cycle (Runtime always injects
    # one; this bare-Supervisor setup must do it explicitly).
    victim.store = InMemoryStateStore()
    sup = Supervisor(
        "root",
        children=[crasher, victim],
        strategy="ONE_FOR_ALL",
        max_restarts=5,
        backoff_base=0.0,
    )
    await sup.start()
    try:
        await _suspend_via_message(victim)
        old_task = victim._task

        await crasher._mailbox.put(Message(type="go"))
        # ONE_FOR_ALL stops every sibling then restarts as a FRESH incarnation
        # (D1a); the checkpointed marker restores it into SUSPENDED. Observe the
        # CURRENT object via the supervisor — the old ref is stale (Q1).
        await wait_for(
            lambda: (
                sup._children_by_name["victim"] is not victim
                and sup._children_by_name["victim"].status == ProcessStatus.SUSPENDED
            ),
            timeout=3.0,
        )
        current = sup._children_by_name["victim"]
        assert old_task is not None and old_task.done()  # old loop stopped, no double-loop
        assert current._task is not None and not current._task.done()  # exactly one live loop
    finally:
        await sup.stop()


async def test_rest_for_one_restart_handles_suspended_downstream():
    """REST_FOR_ONE restarts a suspended downstream child cleanly (S7)."""
    crasher = CrashOnMessageAgent("crasher")
    victim = RecorderAgent("downstream")
    victim.store = InMemoryStateStore()  # H5b: marker must be checkpointed to survive
    sup = Supervisor(
        "root",
        children=[crasher, victim],
        strategy="REST_FOR_ONE",
        max_restarts=5,
        backoff_base=0.0,
    )
    await sup.start()
    try:
        await _suspend_via_message(victim)
        old_task = victim._task

        await crasher._mailbox.put(Message(type="go"))
        # D1a fresh incarnation + Q1: observe the current object, not the stale ref.
        await wait_for(
            lambda: (
                sup._children_by_name["downstream"] is not victim
                and sup._children_by_name["downstream"].status == ProcessStatus.SUSPENDED
            ),
            timeout=3.0,
        )
        assert old_task is not None and old_task.done()
    finally:
        await sup.stop()


# ---------------------------------------------------------------------------
# Marker lifecycle — clear on permanent removal (S8, hole #2)
# ---------------------------------------------------------------------------


async def test_clear_suspend_marker_removes_and_persists():
    """_clear_suspend_marker() drops the marker and persists the cleared state (S8)."""
    agent = RecorderAgent("cleared")
    store = AsyncMock()
    store.get.return_value = None
    agent.store = store
    agent.state[MARKER] = {"reason": "r", "since": 1.0, "approver": None}

    await agent._clear_suspend_marker()
    assert MARKER not in agent.state
    store.set.assert_awaited()


async def test_clear_suspend_marker_noop_without_marker():
    """Clearing when no marker is present writes nothing (S8)."""
    agent = RecorderAgent("nomarker")
    store = AsyncMock()
    agent.store = store
    await agent._clear_suspend_marker()
    store.set.assert_not_awaited()


async def test_clear_suspend_marker_persist_failure_warns_but_clears_in_memory(
    caplog: Any,
) -> None:
    """A failed persist during marker removal is degraded, not fatal (v0.9.1
    top-up): the in-memory marker is still gone even though the store write
    that would have made it durable failed."""
    agent = RecorderAgent("cleared-degraded")
    store = AsyncMock()
    store.get.return_value = None
    store.set.side_effect = RuntimeError("disk full")
    agent.store = store
    agent.state[MARKER] = {"reason": "r", "since": 1.0, "approver": None}

    with caplog.at_level("WARNING"):
        await agent._clear_suspend_marker()

    assert MARKER not in agent.state
    assert any("failed to clear suspend marker" in r.message for r in caplog.records)


async def test_update_suspend_reason_persist_failure_warns(caplog: Any) -> None:
    """_update_suspend_reason()'s checkpoint failure is degraded, not fatal
    (v0.9.1 top-up) — the reason still updates in memory."""
    agent = RecorderAgent("reason-degraded")
    store = AsyncMock()
    store.get.return_value = None
    store.set.side_effect = RuntimeError("disk full")
    agent.store = store
    agent.state[MARKER] = {"reason": "old", "since": 1.0, "approver": None}

    with caplog.at_level("WARNING"):
        await agent._update_suspend_reason("new")

    assert agent.state[MARKER]["reason"] == "new"
    assert any("failed to persist updated suspend reason" in r.message for r in caplog.records)


async def test_resume_persist_failure_warns_but_still_resumes(caplog: Any) -> None:
    """resume()'s checkpoint failure (clearing the marker) is degraded, not
    fatal (v0.9.1 top-up) — the agent still transitions to RUNNING and fires
    on_resume; only durability is what's lost."""
    agent = RecorderAgent("resume-degraded")
    agent._status = ProcessStatus.SUSPENDED
    agent.state[MARKER] = {"reason": "r", "since": 1.0, "approver": None}
    store = AsyncMock()
    store.get.return_value = None
    store.set.side_effect = RuntimeError("disk full")
    agent.store = store

    with caplog.at_level("WARNING"):
        await agent.resume("alice")

    assert agent.status == ProcessStatus.RUNNING
    assert agent.resume_approvers == ["alice"]
    assert any("failed to clear suspend marker on resume" in r.message for r in caplog.records)


async def test_dynamic_supervisor_clear_child_marker():
    """DynamicSupervisor clears a child's marker on permanent removal (S8)."""
    dyn = DynamicSupervisor("workers")
    agent = RecorderAgent("w1")
    store = AsyncMock()
    store.get.return_value = None
    agent.store = store
    agent.state[MARKER] = {"reason": "r", "since": 1.0, "approver": None}
    task = asyncio.create_task(asyncio.sleep(0))
    dyn._dynamic_children["w1"] = _ChildRec(agent=agent, task=task)

    await dyn._clear_child_marker("w1")
    assert MARKER not in agent.state
    await task


async def test_despawn_clears_persisted_marker():
    """Despawn (permanent removal) clears the persisted marker — zombie prevention (S8)."""
    dyn = DynamicSupervisor("workers")
    rt = Runtime(
        supervisor=Supervisor("root", children=[dyn]),
        state_store=InMemoryStateStore(),
    )
    await rt.start()
    try:
        await rt.spawn("workers", EchoAgent, name="echo-1")
        agent = dyn._dynamic_children["echo-1"].agent
        await rt.suspend("echo-1", reason="hitl")
        await wait_for_status(agent, ProcessStatus.SUSPENDED)
        assert MARKER in agent.state

        await rt.despawn("workers", "echo-1")
        saved = await rt._state_store.get("echo-1")
        assert saved is None or MARKER not in saved
    finally:
        await rt.stop()


async def test_graceful_shutdown_keeps_marker():
    """Graceful shutdown keeps the marker — that is the cross-restart point (S8)."""
    agent = RecorderAgent("keeper")
    store = InMemoryStateStore()
    agent.store = store
    await agent._start()
    await _suspend_via_message(agent)
    await agent._stop()
    saved = await store.get("keeper")
    assert saved is not None and MARKER in saved


# ---------------------------------------------------------------------------
# Runtime external entry points (S10)
# ---------------------------------------------------------------------------


async def test_runtime_suspend_routes_priority_control_message():
    """runtime.suspend() routes a priority _agency.suspend message (S10)."""
    rt = Runtime()
    rt._bus = MagicMock()
    rt._bus.route = AsyncMock()
    rt._tracer = MagicMock()
    rt._tracer.new_trace_id.return_value = "trace"

    await rt.suspend("agent-x", reason="awaiting approval")
    msg = rt._bus.route.call_args[0][0]
    assert msg.type == "_agency.suspend"
    assert msg.priority == 1
    assert msg.recipient == "agent-x"
    assert msg.payload["reason"] == "awaiting approval"


async def test_runtime_resume_requires_approver_before_routing():
    """runtime.resume() validates the approver before any message is routed (S6)."""
    rt = Runtime()
    rt._bus = MagicMock()
    rt._bus.route = AsyncMock()
    rt._tracer = MagicMock()

    with pytest.raises(ValueError, match="approver"):
        await rt.resume("agent-x", "")
    rt._bus.route.assert_not_called()


async def test_runtime_resume_routes_priority_control_message():
    """runtime.resume() routes a priority _agency.resume message carrying the approver (S10)."""
    rt = Runtime()
    rt._bus = MagicMock()
    rt._bus.route = AsyncMock()
    rt._tracer = MagicMock()
    rt._tracer.new_trace_id.return_value = "trace"

    await rt.resume("agent-x", approver="alice")
    msg = rt._bus.route.call_args[0][0]
    assert msg.type == "_agency.resume"
    assert msg.priority == 1
    assert msg.payload["approver"] == "alice"


async def test_runtime_suspend_before_start_raises():
    """runtime.suspend() before start() raises RuntimeError."""
    rt = Runtime()
    with pytest.raises(RuntimeError, match="not started"):
        await rt.suspend("agent-x")


async def test_runtime_resume_before_start_raises():
    """runtime.resume() before start() raises RuntimeError once the approver is valid."""
    rt = Runtime()
    with pytest.raises(RuntimeError, match="not started"):
        await rt.resume("agent-x", approver="alice")


async def test_runtime_suspend_resume_end_to_end():
    """Full path: suspend buffers traffic, resume drains it — proves bus accepts the types."""
    agent = RecorderAgent("worker")
    rt = Runtime(
        supervisor=Supervisor("root", children=[agent]),
        state_store=InMemoryStateStore(),
    )
    await rt.start()
    try:
        await rt.suspend("worker", reason="hitl")
        await wait_for_status(agent, ProcessStatus.SUSPENDED)

        await rt.send("worker", {"n": 1}, message_type="biz")
        await asyncio.sleep(0.05)
        assert agent.handled == []  # buffered while suspended

        await rt.resume("worker", approver="alice")
        await wait_for_status(agent, ProcessStatus.RUNNING)
        await wait_for(lambda: "biz" in agent.handled)
    finally:
        await rt.stop()
