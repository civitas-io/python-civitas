"""Unit tests for Supervisor — backoff, sliding window, strategy dispatch, heartbeat config."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from civitas.messages import Message
from civitas.process import AgentProcess, ProcessStatus
from civitas.supervisor import (
    HeartbeatTimeout,
    Supervisor,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class NullAgent(AgentProcess):
    async def handle(self, message):
        return None


def make_supervisor(**kwargs) -> Supervisor:
    defaults = dict(name="root", max_restarts=3, backoff="CONSTANT", backoff_base=1.0)
    defaults.update(kwargs)
    return Supervisor(**defaults)


# ---------------------------------------------------------------------------
# _compute_backoff
# ---------------------------------------------------------------------------


class TestComputeBackoff:
    def test_constant_always_returns_base(self):
        sup = make_supervisor(backoff="CONSTANT", backoff_base=0.5)
        assert sup._compute_backoff(1) == 0.5
        assert sup._compute_backoff(5) == 0.5

    def test_linear_scales_with_count(self):
        sup = make_supervisor(backoff="LINEAR", backoff_base=2.0)
        assert sup._compute_backoff(1) == 2.0
        assert sup._compute_backoff(3) == 6.0
        assert sup._compute_backoff(5) == 10.0

    def test_exponential_doubles_each_restart(self):
        sup = make_supervisor(backoff="EXPONENTIAL", backoff_base=1.0)
        # base * 2^(n-1), ignoring jitter
        with patch("civitas.supervision.engine.random.random", return_value=0.0):
            assert sup._compute_backoff(1) == 1.0  # 1 * 2^0
            assert sup._compute_backoff(2) == 2.0  # 1 * 2^1
            assert sup._compute_backoff(3) == 4.0  # 1 * 2^2

    def test_exponential_applies_jitter(self):
        sup = make_supervisor(backoff="EXPONENTIAL", backoff_base=1.0)
        with patch("civitas.supervision.engine.random.random", return_value=1.0):
            # delay = base * 2^0 = 1.0, jitter = 1.0 * 1.0 * 0.25 = 0.25
            assert sup._compute_backoff(1) == pytest.approx(1.25)

    def test_backoff_max_caps_result(self):
        sup = make_supervisor(backoff="LINEAR", backoff_base=10.0, backoff_max=15.0)
        assert sup._compute_backoff(10) == 15.0  # 100.0 capped at 15.0


# ---------------------------------------------------------------------------
# Sliding window (_restart_timestamps)
# ---------------------------------------------------------------------------


class TestRestartWindow:
    def test_timestamps_stored_as_deque(self):
        sup = make_supervisor()
        assert isinstance(sup._restart_timestamps, deque)

    def test_timestamps_pruned_outside_window(self):
        sup = make_supervisor(restart_window=10.0, max_restarts=100)
        now = time.time()
        # Inject two old timestamps (outside window) and one recent
        sup._restart_timestamps.append(now - 20.0)
        sup._restart_timestamps.append(now - 15.0)
        sup._restart_timestamps.append(now - 5.0)

        # Simulate _handle_crash pruning logic
        cutoff = now - sup.restart_window
        sup._restart_timestamps.append(now)
        while sup._restart_timestamps and sup._restart_timestamps[0] <= cutoff:
            sup._restart_timestamps.popleft()

        assert len(sup._restart_timestamps) == 2  # only the recent one + new
        assert all(t > cutoff for t in sup._restart_timestamps)

    def test_max_restarts_check_uses_window_length(self):
        sup = make_supervisor(restart_window=60.0, max_restarts=2)
        now = time.time()
        # 2 restarts already in window
        sup._restart_timestamps.extend([now - 5.0, now - 3.0])
        # Third crash exceeds limit
        assert len(sup._restart_timestamps) >= sup.max_restarts


# ---------------------------------------------------------------------------
# _find_child (O(1) dict lookup)
# ---------------------------------------------------------------------------


class TestFindChild:
    def test_find_returns_correct_agent(self):
        a = NullAgent("alpha")
        b = NullAgent("beta")
        sup = Supervisor("root", children=[a, b])
        assert sup._find_child("alpha") is a
        assert sup._find_child("beta") is b

    def test_find_returns_none_for_unknown(self):
        sup = Supervisor("root", children=[NullAgent("x")])
        assert sup._find_child("missing") is None

    def test_find_child_via_dict_not_linear_scan(self):
        # _children_by_name is a dict — verify it exists and has correct keys
        a = NullAgent("a")
        sup = Supervisor("root", children=[a])
        assert "a" in sup._children_by_name
        assert sup._children_by_name["a"] is a

    def test_find_child_supervisor(self):
        child_sup = Supervisor("child")
        sup = Supervisor("root", children=[child_sup])
        assert sup._find_child("child") is child_sup


# ---------------------------------------------------------------------------
# _escalate — permanently failed agent stays CRASHED
# ---------------------------------------------------------------------------


class TestEscalate:
    @pytest.mark.asyncio
    async def test_escalate_top_level_leaves_agent_crashed(self):
        agent = NullAgent("worker")
        agent._status = ProcessStatus.CRASHED
        sup = Supervisor("root", children=[agent], max_restarts=1)
        # No parent — top-level escalation
        await sup._escalate("worker", ValueError("boom"))
        # Agent stays CRASHED — not mutated to STOPPED
        assert agent.status == ProcessStatus.CRASHED

    @pytest.mark.asyncio
    async def test_escalate_with_parent_enqueues_crash_event(self):
        """Escalation hands off to the parent via a crash event (H2, D-E4-1) —
        never an inline call, which would let the parent's restart tear down
        the very message dispatch performing the escalation. No bus wired
        (bare-Supervisor test): the trigger lands directly on the parent's
        mailbox (D-E4-7 fallback)."""
        child_sup = Supervisor("child", max_restarts=1)
        parent_sup = Supervisor("root", children=[child_sup], max_restarts=5)
        child_sup._parent = parent_sup

        exc = ValueError("cascade")
        await child_sup._escalate("child", exc)

        assert len(parent_sup._pending_crash_events) == 1
        name, queued_exc, task = next(iter(parent_sup._pending_crash_events.values()))
        assert name == "child"  # the escalating supervisor itself
        assert queued_exc is exc
        assert task is None  # supervisors have no incarnation task
        assert parent_sup._mailbox.depth() == 1  # the trigger message itself


# ---------------------------------------------------------------------------
# add_remote_child — per-child heartbeat config (F03-3)
# ---------------------------------------------------------------------------


class TestRemoteChildConfig:
    def test_each_child_gets_independent_config(self):
        sup = make_supervisor()
        sup.add_remote_child(
            "fast", heartbeat_interval=1.0, heartbeat_timeout=0.5, missed_heartbeats_threshold=2
        )
        sup.add_remote_child(
            "slow", heartbeat_interval=10.0, heartbeat_timeout=5.0, missed_heartbeats_threshold=5
        )

        fast_cfg = sup._remote_child_config["fast"]
        slow_cfg = sup._remote_child_config["slow"]

        assert fast_cfg["interval"] == 1.0
        assert fast_cfg["timeout"] == 0.5
        assert fast_cfg["threshold"] == 2

        assert slow_cfg["interval"] == 10.0
        assert slow_cfg["timeout"] == 5.0
        assert slow_cfg["threshold"] == 5

    def test_second_add_does_not_overwrite_first(self):
        sup = make_supervisor()
        sup.add_remote_child("a", heartbeat_timeout=0.5)
        sup.add_remote_child("b", heartbeat_timeout=9.9)
        # First child's config unchanged
        assert sup._remote_child_config["a"]["timeout"] == 0.5

    def test_add_remote_child_registers_in_set(self):
        sup = make_supervisor()
        sup.add_remote_child("remote_agent")
        assert "remote_agent" in sup._remote_children


# ---------------------------------------------------------------------------
# HeartbeatTimeout
# ---------------------------------------------------------------------------


class TestHeartbeatTimeout:
    def test_attributes(self):
        exc = HeartbeatTimeout("my_agent", missed=4)
        assert exc.agent_name == "my_agent"
        assert exc.missed == 4
        assert "my_agent" in str(exc)
        assert "4" in str(exc)


# ---------------------------------------------------------------------------
# Restart strategy dispatch
# ---------------------------------------------------------------------------


class TestStrategyDispatch:
    @pytest.mark.asyncio
    async def test_one_for_one_calls_restart_child(self):
        sup = Supervisor(
            "root", strategy="ONE_FOR_ONE", max_restarts=5, backoff="CONSTANT", backoff_base=0.0
        )
        sup._restart_counts["a"] = 0

        called = []
        sup._restart_child = AsyncMock(side_effect=lambda n: called.append(n))  # type: ignore[method-assign]
        sup._escalate = AsyncMock()  # type: ignore[method-assign]

        await sup._handle_crash("a", ValueError("x"))

        assert called == ["a"]
        sup._escalate.assert_not_called()

    @pytest.mark.asyncio
    async def test_one_for_all_calls_restart_all(self):
        sup = Supervisor(
            "root", strategy="ONE_FOR_ALL", max_restarts=5, backoff="CONSTANT", backoff_base=0.0
        )
        sup._restart_counts["a"] = 0

        called = []
        sup._restart_all_children = AsyncMock(side_effect=lambda: called.append("all"))  # type: ignore[method-assign]
        sup._escalate = AsyncMock()  # type: ignore[method-assign]

        await sup._handle_crash("a", ValueError("x"))

        assert called == ["all"]

    @pytest.mark.asyncio
    async def test_rest_for_one_calls_restart_rest(self):
        sup = Supervisor(
            "root", strategy="REST_FOR_ONE", max_restarts=5, backoff="CONSTANT", backoff_base=0.0
        )
        sup._restart_counts["a"] = 0

        called = []
        sup._restart_rest_for_one = AsyncMock(side_effect=lambda n: called.append(n))  # type: ignore[method-assign]
        sup._escalate = AsyncMock()  # type: ignore[method-assign]

        await sup._handle_crash("a", ValueError("x"))

        assert called == ["a"]

    @pytest.mark.asyncio
    async def test_exceeding_max_restarts_calls_escalate(self):
        sup = Supervisor(
            "root",
            strategy="ONE_FOR_ONE",
            max_restarts=1,
            backoff="CONSTANT",
            backoff_base=0.0,
            restart_window=60.0,
        )
        sup._restart_counts["a"] = 0

        # Pre-fill 2 timestamps to exceed max_restarts=1
        now = time.time()
        sup._restart_timestamps.extend([now - 5, now - 3])

        escalated = []
        sup._escalate = AsyncMock(side_effect=lambda n, e: escalated.append(n))  # type: ignore[method-assign]
        sup._restart_child = AsyncMock()  # type: ignore[method-assign]

        await sup._handle_crash("a", ValueError("x"))

        assert "a" in escalated
        sup._restart_child.assert_not_called()


class TestCrashCallbacks:
    @pytest.mark.asyncio
    async def test_callback_invoked_on_crash(self):
        """add_crash_callback() registers a callback invoked with (name, exc) (FD-01/FD-03)."""
        sup = Supervisor(
            "root", strategy="ONE_FOR_ONE", max_restarts=5, backoff="CONSTANT", backoff_base=0.0
        )
        sup._restart_child = AsyncMock()  # type: ignore[method-assign]
        received: list[tuple[str, Exception]] = []

        async def callback(name: str, exc: Exception) -> None:
            received.append((name, exc))

        sup.add_crash_callback(callback)
        exc = ValueError("boom")
        await sup._handle_crash("a", exc)

        assert received == [("a", exc)]

    @pytest.mark.asyncio
    async def test_multiple_callbacks_all_invoked(self):
        """Multiple registered callbacks all run on the same crash (FD-01/FD-03)."""
        sup = Supervisor(
            "root", strategy="ONE_FOR_ONE", max_restarts=5, backoff="CONSTANT", backoff_base=0.0
        )
        sup._restart_child = AsyncMock()  # type: ignore[method-assign]
        calls: list[str] = []

        async def callback_a(name: str, exc: Exception) -> None:
            calls.append(f"a:{name}")

        async def callback_b(name: str, exc: Exception) -> None:
            calls.append(f"b:{name}")

        sup.add_crash_callback(callback_a)
        sup.add_crash_callback(callback_b)
        await sup._handle_crash("worker", ValueError("x"))

        assert calls == ["a:worker", "b:worker"]

    @pytest.mark.asyncio
    async def test_raising_callback_does_not_block_restart(self):
        """A crash callback that raises is logged, restart still proceeds (FD-01/FD-03)."""
        sup = Supervisor(
            "root", strategy="ONE_FOR_ONE", max_restarts=5, backoff="CONSTANT", backoff_base=0.0
        )
        restarted: list[str] = []
        sup._restart_child = AsyncMock(side_effect=lambda n: restarted.append(n))  # type: ignore[method-assign]

        async def bad_callback(name: str, exc: Exception) -> None:
            raise RuntimeError("callback exploded")

        sup.add_crash_callback(bad_callback)
        await sup._handle_crash("a", ValueError("x"))

        assert restarted == ["a"]


# ---------------------------------------------------------------------------
# all_agents / all_supervisors
# ---------------------------------------------------------------------------


class TestTreeCollectors:
    def test_all_agents_flat(self):
        a = NullAgent("a")
        b = NullAgent("b")
        sup = Supervisor("root", children=[a, b])
        assert set(agent.name for agent in sup.all_agents()) == {"a", "b"}

    def test_all_agents_nested(self):
        a = NullAgent("a")
        b = NullAgent("b")
        child_sup = Supervisor("child", children=[b])
        root = Supervisor("root", children=[a, child_sup])
        names = {agent.name for agent in root.all_agents()}
        assert names == {"a", "b"}

    def test_all_supervisors_includes_self_and_children(self):
        child_sup = Supervisor("child")
        root = Supervisor("root", children=[child_sup])
        names = {s.name for s in root.all_supervisors()}
        assert names == {"root", "child"}


# ---------------------------------------------------------------------------
# _compute_backoff — unknown policy fallback
# ---------------------------------------------------------------------------


class TestBackoffFallback:
    def test_unknown_backoff_policy_falls_back_to_base(self):
        sup = make_supervisor(backoff="CONSTANT", backoff_base=3.0)
        # Patch internal enum value to simulate an unrecognised policy
        sup.backoff = "UNKNOWN_POLICY"  # type: ignore[assignment]
        assert sup._compute_backoff(1) == 3.0


# ---------------------------------------------------------------------------
# _on_child_done — cancelled task is ignored
# ---------------------------------------------------------------------------


class TestOnChildDone:
    def test_cancelled_task_not_treated_as_crash(self):
        sup = make_supervisor()
        sup._running = True

        task = MagicMock()
        task.cancelled.return_value = True

        # Should return early — no crash handling scheduled
        sup._handle_crash = AsyncMock()  # type: ignore[method-assign]
        sup._on_child_done("worker", task)
        sup._handle_crash.assert_not_called()

    def test_not_running_still_enqueues_crash(self):
        """Crashes landing while stopped are queued, not dropped (H2, #30).

        The old code checked _running at enqueue time, dropping crashes that
        arrived during a nested stop/start window. The drain loop now decides
        at dequeue time (discard after final stop, stale-skip after restart)."""
        sup = make_supervisor()
        sup._running = False

        task = MagicMock()
        task.cancelled.return_value = False
        task.exception.return_value = ValueError("boom")

        sup._on_child_done("worker", task)
        assert len(sup._pending_crash_events) == 1


# ---------------------------------------------------------------------------
# _handle_crash — sliding window timestamp pruning
# ---------------------------------------------------------------------------


class TestHandleCrashTimestampPruning:
    @pytest.mark.asyncio
    async def test_old_timestamps_pruned_from_window(self):
        """Timestamps outside restart_window are removed before checking limit."""
        sup = make_supervisor(
            max_restarts=2, restart_window=10.0, backoff="CONSTANT", backoff_base=0.0
        )
        sup._restart_child = AsyncMock()  # type: ignore[method-assign]
        sup._escalate = AsyncMock()  # type: ignore[method-assign]

        now = time.time()
        # Two timestamps well outside the 10s window
        sup._restart_timestamps.extend([now - 30.0, now - 20.0])

        await sup._handle_crash("agent", ValueError("x"))

        # Old timestamps pruned — only 1 in window — should restart, not escalate
        sup._restart_child.assert_called_once()
        sup._escalate.assert_not_called()


# ---------------------------------------------------------------------------
# Heartbeat monitor — _start / _stop / loop
# ---------------------------------------------------------------------------


class TestHeartbeatMonitor:
    @pytest.mark.asyncio
    async def test_heartbeat_not_started_without_remote_children(self):
        """_start_heartbeat_monitor is a no-op when there are no remote children."""
        sup = make_supervisor()
        await sup._start_heartbeat_monitor()
        assert sup._heartbeat_task is None

    @pytest.mark.asyncio
    async def test_heartbeat_task_created_with_remote_children(self):
        """_start_heartbeat_monitor creates a task when remote children exist."""
        sup = make_supervisor()
        sup.add_remote_child("remote_a", heartbeat_interval=60.0)

        # Patch the loop to avoid actually running it
        sup._heartbeat_loop = AsyncMock(return_value=None)  # type: ignore[method-assign]
        await sup._start_heartbeat_monitor()

        assert sup._heartbeat_task is not None
        sup._heartbeat_task.cancel()
        try:
            await sup._heartbeat_task
        except (asyncio.CancelledError, Exception):
            pass

    @pytest.mark.asyncio
    async def test_stop_heartbeat_monitor_cancels_task(self):
        """_stop_heartbeat_monitor cancels the task and sets it to None."""
        sup = make_supervisor()
        sup.add_remote_child("remote_a", heartbeat_interval=60.0)
        sup._running = True

        # Start a long-running task as stand-in for the heartbeat loop
        sup._heartbeat_task = asyncio.create_task(asyncio.sleep(999))
        await sup._stop_heartbeat_monitor()

        assert sup._heartbeat_task is None

    @pytest.mark.asyncio
    async def test_heartbeat_ack_resets_missed_counter(self):
        """A successful heartbeat reply resets the missed counter to 0."""
        sup = make_supervisor()
        sup.add_remote_child(
            "remote_a",
            heartbeat_interval=0.01,
            heartbeat_timeout=1.0,
            missed_heartbeats_threshold=3,
        )
        sup._running = True
        sup._missed_heartbeats["remote_a"] = 2  # already has missed beats

        mock_bus = AsyncMock()
        mock_bus.request = AsyncMock(return_value=MagicMock())
        sup._bus = mock_bus

        # Let one iteration run then stop the loop via mocked sleep
        async def _stop_after_sleep(*_a: object, **_kw: object) -> None:
            sup._running = False

        with patch("civitas.supervisor.asyncio.sleep", side_effect=_stop_after_sleep):
            await sup._heartbeat_loop()

        assert sup._missed_heartbeats["remote_a"] == 0

    @pytest.mark.asyncio
    async def test_heartbeat_timeout_increments_missed_counter(self):
        """A TimeoutError on heartbeat increments the missed counter."""
        sup = make_supervisor()
        sup.add_remote_child(
            "remote_a",
            heartbeat_interval=0.01,
            heartbeat_timeout=0.01,
            missed_heartbeats_threshold=5,
        )
        sup._running = True

        mock_bus = AsyncMock()
        mock_bus.request = AsyncMock(side_effect=TimeoutError())
        sup._bus = mock_bus

        async def _stop_after_sleep(*_a: object, **_kw: object) -> None:
            sup._running = False

        with patch("civitas.supervisor.asyncio.sleep", side_effect=_stop_after_sleep):
            await sup._heartbeat_loop()

        assert sup._missed_heartbeats.get("remote_a", 0) == 1

    @pytest.mark.asyncio
    async def test_heartbeat_threshold_enqueues_crash_event(self):
        """Threshold breach hands a HeartbeatTimeout to the crash queue (H4, #31)
        — never an inline _handle_crash call, whose backoff sleep would stall
        heartbeat monitoring of every other remote child."""
        sup = make_supervisor()
        sup.add_remote_child(
            "remote_a",
            heartbeat_interval=0.01,
            heartbeat_timeout=0.01,
            missed_heartbeats_threshold=2,
        )
        sup._running = True
        sup._missed_heartbeats["remote_a"] = 1  # one away from threshold

        mock_bus = AsyncMock()
        mock_bus.request = AsyncMock(side_effect=TimeoutError())
        sup._bus = mock_bus

        sup._handle_crash = AsyncMock()  # type: ignore[method-assign]

        async def _stop_after_sleep(*_a: object, **_kw: object) -> None:
            sup._running = False

        with patch("civitas.supervisor.asyncio.sleep", side_effect=_stop_after_sleep):
            await sup._heartbeat_loop()

        sup._handle_crash.assert_not_called()
        assert len(sup._pending_crash_events) == 1
        name, exc, task = next(iter(sup._pending_crash_events.values()))
        assert name == "remote_a"
        assert isinstance(exc, HeartbeatTimeout)
        assert task is None  # remote children have no incarnation task
        assert sup._missed_heartbeats.get("remote_a", 0) == 0  # reset after trigger

    @pytest.mark.asyncio
    async def test_heartbeats_sent_at_priority_one(self):
        """Liveness probes ride the priority channel (H4, #31)."""
        sup = make_supervisor()
        sup.add_remote_child("remote_a", heartbeat_interval=0.01, heartbeat_timeout=0.01)
        sup._running = True

        seen: list = []

        async def _record(message, timeout):
            seen.append(message)
            sup._running = False
            raise TimeoutError

        mock_bus = AsyncMock()
        mock_bus.request = AsyncMock(side_effect=_record)
        sup._bus = mock_bus

        async def _noop_sleep(*_a: object, **_kw: object) -> None:
            return None

        with patch("civitas.supervisor.asyncio.sleep", side_effect=_noop_sleep):
            await sup._heartbeat_loop()

        assert seen and all(m.priority == 1 for m in seen)

    @pytest.mark.asyncio
    async def test_monitor_keeps_pinging_others_after_breach(self):
        """A breached child must not stall monitoring of its siblings (H4):
        the restart happens on the drain task, not inline in the loop."""
        sup = make_supervisor()
        sup.add_remote_child(
            "dead_a",
            heartbeat_interval=0.01,
            heartbeat_timeout=0.01,
            missed_heartbeats_threshold=1,
        )
        sup.add_remote_child("live_b", heartbeat_interval=0.01, heartbeat_timeout=0.01)
        sup._running = True
        sup._handle_crash = AsyncMock()  # type: ignore[method-assign]

        pings: list[str] = []

        async def _selective(message, timeout):
            pings.append(message.recipient)
            if message.recipient == "dead_a":
                raise TimeoutError
            return Message(type="_agency.heartbeat_ack", sender="live_b", recipient=sup.name)

        mock_bus = AsyncMock()
        mock_bus.request = AsyncMock(side_effect=_selective)
        sup._bus = mock_bus

        await sup._start_heartbeat_monitor()
        await asyncio.sleep(0.08)
        sup._running = False
        await sup._stop_heartbeat_monitor()

        first_breach = pings.index("dead_a")
        pings_to_b_after = pings[first_breach:].count("live_b")
        assert pings_to_b_after >= 2, "monitoring of siblings stalled after a breach"
        sup._handle_crash.assert_not_called()  # queued as a crash event, not inline
        assert len(sup._pending_crash_events) >= 1

    @pytest.mark.asyncio
    async def test_heartbeat_loop_continues_on_generic_exception(
        self, caplog: pytest.LogCaptureFixture
    ):
        """A non-timeout exception is warned but does not crash the loop."""
        sup = make_supervisor()
        sup.add_remote_child(
            "remote_a",
            heartbeat_interval=0.01,
            heartbeat_timeout=1.0,
            missed_heartbeats_threshold=3,
        )
        sup._running = True

        mock_bus = AsyncMock()
        mock_bus.request = AsyncMock(side_effect=RuntimeError("unexpected"))
        sup._bus = mock_bus

        async def _stop_after_sleep(*_a: object, **_kw: object) -> None:
            sup._running = False

        with caplog.at_level(logging.WARNING, logger="civitas.supervisor"):
            with patch("civitas.supervisor.asyncio.sleep", side_effect=_stop_after_sleep):
                await sup._heartbeat_loop()

        assert any("heartbeat error" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _restart_child — remote and Supervisor branches
# ---------------------------------------------------------------------------


class TestRestartChildBranches:
    @pytest.mark.asyncio
    async def test_restart_child_remote_delegates_to_remote_restart(self):
        """_restart_child calls _restart_remote_child for remote children."""
        sup = make_supervisor()
        sup.add_remote_child("remote_a")

        remote_calls: list = []
        sup._restart_remote_child = AsyncMock(side_effect=lambda n: remote_calls.append(n))  # type: ignore[method-assign]

        await sup._restart_child("remote_a")
        assert remote_calls == ["remote_a"]

    @pytest.mark.asyncio
    async def test_restart_child_supervisor_restarts_subtree(self):
        """_restart_child stops, budget-clears, and restarts a Supervisor child (H1, #28)."""
        child_sup = Supervisor("inner")
        sup = Supervisor("root", children=[child_sup])

        child_sup._restart_timestamps.extend([1.0, 2.0])  # exhausted budget
        child_sup._restart_counts["a"] = 2
        child_sup.stop = AsyncMock()  # type: ignore[method-assign]
        child_sup.start = AsyncMock()  # type: ignore[method-assign]

        await sup._restart_child("inner")

        child_sup.stop.assert_awaited_once()
        child_sup.start.assert_awaited_once()
        # Fresh incarnation gets a fresh budget — no instant re-escalation
        assert len(child_sup._restart_timestamps) == 0
        assert child_sup._restart_counts == {}

    @pytest.mark.asyncio
    async def test_restart_child_preserves_registration_snapshot(self):
        """_restart_child re-registers with the full prior entry (H3, #29)."""
        from civitas.registry import LocalRegistry

        agent = NullAgent("worker")
        sup = Supervisor("root", children=[agent], backoff="CONSTANT", backoff_base=0.0)

        registry = LocalRegistry()
        registry.register("worker", capabilities=["gap.probe"], capability_metadata={"v": "1"})
        sup._registry = registry

        agent._start = AsyncMock()  # type: ignore[method-assign]
        agent._task = None

        await sup._restart_child("worker")

        entry = registry.lookup("worker")
        assert entry is not None
        assert entry.capabilities == ("gap.probe",)
        assert entry.capability_metadata == {"v": "1"}

    @pytest.mark.asyncio
    async def test_restart_remote_child_routes_restart_message(self):
        """_restart_remote_child sends a restart command via the message bus."""
        sup = make_supervisor()
        mock_bus = AsyncMock()
        sup._bus = mock_bus

        await sup._restart_remote_child("remote_a")

        mock_bus.route.assert_called_once()
        msg = mock_bus.route.call_args[0][0]
        assert msg.type == "_agency.restart"
        assert msg.payload["agent_name"] == "remote_a"


# ---------------------------------------------------------------------------
# _restart_all_children — Supervisor children handled correctly
# ---------------------------------------------------------------------------


class TestRestartAllChildren:
    @pytest.mark.asyncio
    async def test_restart_all_stops_and_starts_supervisor_children(self):
        """ONE_FOR_ALL: child Supervisors are stop()ed then start()ed."""
        child_sup = Supervisor("inner")
        child_sup.stop = AsyncMock()  # type: ignore[method-assign]
        child_sup.start = AsyncMock()  # type: ignore[method-assign]

        sup = Supervisor("root", strategy="ONE_FOR_ALL", children=[child_sup])
        await sup._restart_all_children()

        child_sup.stop.assert_called_once()
        child_sup.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_restart_all_uses_registry_for_agent_children(self):
        """ONE_FOR_ALL: agent children are deregistered and re-registered."""
        agent = NullAgent("worker")
        agent._status = ProcessStatus.RUNNING
        agent._stop = AsyncMock()  # type: ignore[method-assign]
        agent._start = AsyncMock()  # type: ignore[method-assign]
        agent._task = None

        sup = Supervisor("root", strategy="ONE_FOR_ALL", children=[agent])
        from civitas.registry import LocalRegistry

        registry = LocalRegistry()
        registry.register("worker", capabilities=["gap.probe"])
        sup._registry = registry

        await sup._restart_all_children()

        entry = registry.lookup("worker")
        assert entry is not None
        assert entry.capabilities == ("gap.probe",)  # snapshot preserved (H3)


# ---------------------------------------------------------------------------
# _restart_rest_for_one — Supervisor children handled correctly
# ---------------------------------------------------------------------------


class TestRestartRestForOne:
    @pytest.mark.asyncio
    async def test_rest_for_one_stops_and_starts_supervisor_children(self):
        """REST_FOR_ONE: Supervisor children after the crash point are stop()ed and start()ed."""
        crashed = NullAgent("a")
        crashed._status = ProcessStatus.CRASHED
        crashed._stop = AsyncMock()  # type: ignore[method-assign]
        crashed._start = AsyncMock()  # type: ignore[method-assign]
        crashed._task = None

        child_sup = Supervisor("inner")
        child_sup.stop = AsyncMock()  # type: ignore[method-assign]
        child_sup.start = AsyncMock()  # type: ignore[method-assign]

        sup = Supervisor("root", strategy="REST_FOR_ONE", children=[crashed, child_sup])
        await sup._restart_rest_for_one("a")

        child_sup.stop.assert_called_once()
        child_sup.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_rest_for_one_uses_registry_for_agent_children(self):
        """REST_FOR_ONE: agent children after crash point are deregistered and re-registered."""
        first = NullAgent("first")
        first._status = ProcessStatus.CRASHED
        first._stop = AsyncMock()  # type: ignore[method-assign]
        first._start = AsyncMock()  # type: ignore[method-assign]
        first._task = None

        second = NullAgent("second")
        second._status = ProcessStatus.RUNNING
        second._stop = AsyncMock()  # type: ignore[method-assign]
        second._start = AsyncMock()  # type: ignore[method-assign]
        second._task = None

        sup = Supervisor("root", strategy="REST_FOR_ONE", children=[first, second])
        mock_registry = MagicMock()
        sup._registry = mock_registry

        await sup._restart_rest_for_one("first")

        # Both agents should be deregistered and re-registered
        deregister_calls = [c.args[0] for c in mock_registry.deregister.call_args_list]
        register_calls = [c.args[0] for c in mock_registry.register.call_args_list]
        assert "first" in deregister_calls
        assert "second" in deregister_calls
        assert "first" in register_calls
        assert "second" in register_calls


# ---------------------------------------------------------------------------
# v0.9.0 E4 Phase C — introspection + Q3 suspend hard-rejection (D-E4-4, D-E4-9)
# ---------------------------------------------------------------------------


class TestSupervisionStatus:
    @pytest.mark.asyncio
    async def test_status_snapshot_reports_children_and_window(self):
        a = NullAgent("a")
        a._status = ProcessStatus.RUNNING
        b = NullAgent("b")
        b._status = ProcessStatus.CRASHED
        sup = Supervisor("root", children=[a, b], strategy="ONE_FOR_ALL", max_restarts=7)
        sup._restart_counts["b"] = 2
        sup._engine.window.append(time.monotonic())

        snapshot = sup._status_snapshot()

        assert snapshot["name"] == "root"
        assert snapshot["strategy"] == "ONE_FOR_ALL"
        assert snapshot["max_restarts"] == 7
        assert snapshot["crashes_in_window"] == 1
        by_name = {c["name"]: c for c in snapshot["children"]}
        assert by_name["a"] == {
            "name": "a",
            "kind": "agent",
            "status": "RUNNING",
            "restart_count": 0,
        }
        assert by_name["b"]["status"] == "CRASHED"
        assert by_name["b"]["restart_count"] == 2

    @pytest.mark.asyncio
    async def test_status_snapshot_reports_child_supervisor_kind(self):
        child_sup = Supervisor("child")
        sup = Supervisor("root", children=[child_sup])

        snapshot = sup._status_snapshot()

        assert snapshot["children"][0] == {
            "name": "child",
            "kind": "supervisor",
            "status": child_sup._status.value,
            "restart_count": 0,
        }

    @pytest.mark.asyncio
    async def test_handle_routes_status_message_via_reply(self):
        sup = Supervisor("root")
        request = Message(
            type="civitas.supervision.status",
            sender="caller",
            recipient="root",
            correlation_id="cid-1",
        )
        sup._current_message = request

        reply = await sup.handle(request)

        assert reply is not None
        assert reply.recipient == "caller"
        assert reply.correlation_id == "cid-1"
        assert reply.payload["name"] == "root"

    @pytest.mark.asyncio
    async def test_handle_unknown_message_falls_through_to_base(self):
        """Unrecognized message types still reach AgentProcess.handle() (None, fire-and-forget)."""
        sup = Supervisor("root")
        message = Message(type="not.a.real.type", sender="x", recipient="root")

        assert await sup.handle(message) is None


class TestSuspendHardRejection:
    @pytest.mark.asyncio
    async def test_suspend_raises_immediately(self):
        """Q3/D-E4-9: the direct-call path is a genuine hard reject."""
        sup = Supervisor("root")
        with pytest.raises(RuntimeError, match="cannot be suspended"):
            await sup.suspend("testing")
        # No half-effect: no suspend intent was recorded.
        assert sup._suspend_requested is False

    def test_suspend_allowed_is_false_on_supervisor(self):
        sup = Supervisor("root")
        assert sup._suspend_allowed() is False

    def test_suspend_allowed_defaults_true_on_plain_agent(self):
        """Regression guard: the new hook must not change behavior for every
        existing AgentProcess subclass (default-preserving, D-E4-9)."""
        agent = NullAgent("plain")
        assert agent._suspend_allowed() is True

    @pytest.mark.asyncio
    async def test_agency_suspend_message_dropped_with_warning(self, caplog):
        """Q3/D-E4-9: the message path never reaches handle() — it's intercepted
        inline in _message_loop, before _current_message is even set, so a
        reply is not the mechanism here; a loud WARNING + drop is."""
        sup = Supervisor("root")
        await sup._start()
        try:
            with caplog.at_level(logging.WARNING):
                await sup._mailbox.put(
                    Message(
                        type="_agency.suspend",
                        sender="_runtime",
                        recipient="root",
                        payload={"reason": "test"},
                        priority=1,
                    )
                )
                assert await _wait_until(
                    lambda: any("rejecting _agency.suspend" in r.message for r in caplog.records)
                )
            assert sup._status != ProcessStatus.SUSPENDED
            assert sup._suspend_requested is False
        finally:
            await sup._stop()

    @pytest.mark.asyncio
    async def test_plain_agent_suspend_message_unaffected_by_new_hook(self):
        """Regression guard: a plain agent's _agency.suspend still works exactly
        as before — the new hook must be a true no-op on the default path."""
        agent = NullAgent("plain")
        await agent._start()
        try:
            await agent._mailbox.put(
                Message(
                    type="_agency.suspend",
                    sender="_runtime",
                    recipient="plain",
                    payload={"reason": "test"},
                    priority=1,
                )
            )
            assert await _wait_until(lambda: agent._status == ProcessStatus.SUSPENDED)
        finally:
            await agent._stop()


async def _wait_until(predicate, timeout: float = 2.0, interval: float = 0.01) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()
