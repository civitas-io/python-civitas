"""Tests for DynamicSupervisor — spawn, despawn, stop, governance, restart semantics."""

from __future__ import annotations

import asyncio
import textwrap
from pathlib import Path
from typing import Any

import pytest

from civitas import AgentProcess, DynamicSupervisor, NullSink, Runtime, Supervisor
from civitas.errors import ErrorAction, SpawnError
from civitas.messages import Message
from civitas.process import ProcessStatus
from civitas.registry import LocalRegistry
from civitas.supervisor import RestartMode
from tests.conftest import EchoAgent, wait_for, wait_for_status

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class NullAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return None


class ConfigAgent(AgentProcess):
    async def on_start(self) -> None:
        self.state["seen_config"] = dict(self.config)

    async def handle(self, message: Message) -> Message | None:
        return self.reply({"config": self.state.get("seen_config", {})})


class CleanExitAgent(AgentProcess):
    """Agent that stops cleanly on the first message."""

    async def handle(self, message: Message) -> Message | None:
        self._status = ProcessStatus.STOPPING
        return None


class CrashAgent(AgentProcess):
    """Agent that crashes on the first message."""

    async def handle(self, message: Message) -> Message | None:
        raise RuntimeError("intentional crash")

    async def on_error(self, error: Exception, message: Message):
        from civitas.errors import ErrorAction

        return ErrorAction.ESCALATE


class TerminationRecorder(AgentProcess):
    """Agent that records on_child_terminated calls."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.terminated: list[tuple[str, str]] = []

    async def on_child_terminated(self, name: str, reason: str) -> None:
        self.terminated.append((name, reason))


class _StubMetrics:
    def message_handled(self, agent_name: str, latency_ms: float) -> None:
        pass

    def message_sent(self, agent_name: str) -> None:
        pass

    def agent_error(self, agent_name: str) -> None:
        pass

    def agent_restarted(self, agent_name: str, reason: str = "") -> None:
        pass


def _make_dyn(**kwargs: Any) -> DynamicSupervisor:
    return DynamicSupervisor(name="workers", **kwargs)


def _fake_message(msg_type: str, payload: dict[str, Any]) -> Message:
    return Message(type=msg_type, sender="orchestrator", recipient="workers", payload=payload)


# ---------------------------------------------------------------------------
# Construction and defaults
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_starts_with_no_children(self):
        ds = _make_dyn()
        assert ds._dynamic_children == {}
        assert ds._total_spawns == 0

    def test_default_limits_are_unbounded(self):
        ds = _make_dyn()
        assert ds.max_children is None
        assert ds.max_total_spawns is None

    def test_default_restart_mode_is_transient(self):
        ds = _make_dyn()
        assert ds._restart_mode == RestartMode.TRANSIENT

    def test_custom_limits_stored(self):
        ds = _make_dyn(max_children=10, max_total_spawns=100)
        assert ds.max_children == 10
        assert ds.max_total_spawns == 100

    def test_per_spawner_quota_defaults_and_stored(self):
        assert _make_dyn().max_children_per_spawner is None
        assert _make_dyn().max_total_spawns_per_spawner is None
        ds = _make_dyn(max_children_per_spawner=3, max_total_spawns_per_spawner=9)
        assert ds.max_children_per_spawner == 3
        assert ds.max_total_spawns_per_spawner == 9

    def test_all_dynamic_agents_initially_empty(self):
        ds = _make_dyn()
        assert ds.all_dynamic_agents() == []

    def test_restart_mode_never(self):
        ds = _make_dyn(restart="never")
        assert ds._restart_mode == RestartMode.NEVER

    def test_restart_mode_permanent(self):
        ds = _make_dyn(restart="permanent")
        assert ds._restart_mode == RestartMode.PERMANENT


# ---------------------------------------------------------------------------
# handle() — unit tests via direct dispatch (no bus needed)
# ---------------------------------------------------------------------------


async def _dispatch(ds: DynamicSupervisor, msg: Message) -> Message | None:
    """Call handle() as _dispatch() would: sets _current_message so reply() works."""
    ds._current_message = msg
    result = await ds.handle(msg)
    ds._current_message = None
    return result


class TestHandleSpawn:
    async def test_invalid_class_path_returns_error(self):
        ds = _make_dyn()
        msg = _fake_message(
            "civitas.dynamic.spawn",
            {"class_path": "NoModule", "name": "w1", "config": {}, "spawner": ""},
        )
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "invalid class path" in reply.payload["reason"]

    async def test_unresolvable_module_returns_error(self):
        ds = _make_dyn()
        msg = _fake_message(
            "civitas.dynamic.spawn",
            {
                "class_path": "totally.nonexistent.Module.ClassName",
                "name": "w1",
                "config": {},
                "spawner": "",
            },
        )
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "cannot import" in reply.payload["reason"]

    async def test_duplicate_name_returns_error(self):
        ds = _make_dyn()
        # Manually plant a child to simulate duplicate
        ds._dynamic_children["w1"] = NullAgent("w1")
        msg = _fake_message(
            "civitas.dynamic.spawn",
            {"class_path": "tests.conftest.EchoAgent", "name": "w1", "config": {}, "spawner": ""},
        )
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "already running" in reply.payload["reason"]

    async def test_max_children_limit_returns_error(self):
        ds = _make_dyn(max_children=1)
        ds._dynamic_children["w1"] = NullAgent("w1")
        msg = _fake_message(
            "civitas.dynamic.spawn",
            {"class_path": "tests.conftest.EchoAgent", "name": "w2", "config": {}, "spawner": ""},
        )
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "max_children" in reply.payload["reason"]

    async def test_max_total_spawns_limit_returns_error(self):
        ds = _make_dyn(max_total_spawns=0)
        msg = _fake_message(
            "civitas.dynamic.spawn",
            {"class_path": "tests.conftest.EchoAgent", "name": "w1", "config": {}, "spawner": ""},
        )
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "max_total_spawns" in reply.payload["reason"]

    async def test_max_children_per_spawner_limit_returns_error(self):
        ds = _make_dyn(max_children_per_spawner=1)
        ds._dynamic_children["a1"] = NullAgent("a1")
        ds._spawner_names["a1"] = "agent-a"
        msg = _fake_message(
            "civitas.dynamic.spawn",
            {
                "class_path": "tests.conftest.EchoAgent",
                "name": "a2",
                "config": {},
                "spawner": "agent-a",
            },
        )
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "max_children_per_spawner" in reply.payload["reason"]

    async def test_max_total_spawns_per_spawner_limit_returns_error(self):
        ds = _make_dyn(max_total_spawns_per_spawner=1)
        ds._spawner_total_counts["agent-a"] = 1
        msg = _fake_message(
            "civitas.dynamic.spawn",
            {
                "class_path": "tests.conftest.EchoAgent",
                "name": "a1",
                "config": {},
                "spawner": "agent-a",
            },
        )
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "max_total_spawns_per_spawner" in reply.payload["reason"]

    async def test_governance_veto_returns_error(self):
        class VetoSupervisor(DynamicSupervisor):
            async def on_spawn_requested(self, agent_class, name, config) -> bool:
                return False

        ds = VetoSupervisor(name="workers")
        msg = _fake_message(
            "civitas.dynamic.spawn",
            {"class_path": "tests.conftest.EchoAgent", "name": "w1", "config": {}, "spawner": ""},
        )
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "governance" in reply.payload["reason"]

    async def test_unknown_message_type_returns_none(self):
        ds = _make_dyn()
        msg = _fake_message("civitas.unknown", {})
        result = await _dispatch(ds, msg)
        assert result is None


class TestHandleDespawnStop:
    async def test_despawn_unknown_name_returns_error(self):
        ds = _make_dyn()
        msg = _fake_message("civitas.dynamic.despawn", {"name": "ghost"})
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "ghost" in reply.payload["reason"]

    async def test_stop_unknown_name_returns_error(self):
        ds = _make_dyn()
        msg = _fake_message("civitas.dynamic.stop", {"name": "ghost", "drain": "current"})
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "ghost" in reply.payload["reason"]


# ---------------------------------------------------------------------------
# AgentProcess.spawn() / despawn() / stop() — SpawnError when no ancestor
# ---------------------------------------------------------------------------


class TestSpawnMethodNoAncestor:
    async def test_spawn_raises_when_no_dyn_supervisor(self):
        agent = NullAgent("agent")
        with pytest.raises(SpawnError, match="No DynamicSupervisor"):
            await agent.spawn(EchoAgent, name="echo-1")

    async def test_despawn_raises_when_no_dyn_supervisor(self):
        agent = NullAgent("agent")
        with pytest.raises(SpawnError, match="No DynamicSupervisor"):
            await agent.despawn("echo-1")

    async def test_stop_raises_when_no_dyn_supervisor(self):
        agent = NullAgent("agent")
        with pytest.raises(SpawnError, match="No DynamicSupervisor"):
            await agent.stop("echo-1")


# ---------------------------------------------------------------------------
# Integration tests — full Runtime lifecycle
# ---------------------------------------------------------------------------


def _build_runtime(dyn: DynamicSupervisor, extra_children: list | None = None) -> Runtime:
    children: list = extra_children or []
    children.append(dyn)
    return Runtime(supervisor=Supervisor("root", children=children))


class TestRuntimeSpawn:
    @pytest.mark.asyncio
    async def test_spawn_creates_running_agent(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            name = await rt.spawn("workers", EchoAgent, name="echo-1")
            assert name == "echo-1"
            reply = await rt.ask("echo-1", {"msg": "hello"})
            assert reply.payload["echo"]["msg"] == "hello"
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_spawn_config_applied_to_agent(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            name = await rt.spawn("workers", ConfigAgent, name="cfg-1", config={"topic": "quantum"})
            reply = await rt.ask(name, {})
            assert reply.payload["config"] == {"topic": "quantum"}
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_spawn_inherits_audit_sink_and_metrics(self):
        audit = NullSink()
        metrics = _StubMetrics()
        dyn = _make_dyn()
        rt = Runtime(supervisor=Supervisor("root", children=[dyn]), metrics=metrics)
        rt._audit_sink = audit
        await rt.start()
        try:
            await rt.spawn("workers", EchoAgent, name="echo-1")
            child = dyn._dynamic_children["echo-1"].agent
            assert child._audit_sink is audit
            assert child._metrics is metrics
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_spawn_increments_total_spawns(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt.spawn("workers", EchoAgent, name="echo-1")
            await rt.spawn("workers", EchoAgent, name="echo-2")
            assert dyn._total_spawns == 2
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_spawn_enforces_max_children(self):
        dyn = _make_dyn(max_children=1)
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt.spawn("workers", EchoAgent, name="echo-1")
            with pytest.raises(SpawnError, match="max_children"):
                await rt.spawn("workers", EchoAgent, name="echo-2")
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_max_children_per_spawner_independent_and_enforced(self):
        dyn = _make_dyn(max_children_per_spawner=1)
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            r1 = await rt.ask(
                "workers",
                {
                    "class_path": "tests.conftest.EchoAgent",
                    "name": "a1",
                    "config": {},
                    "spawner": "agent-a",
                    "wait": True,
                },
                message_type="civitas.dynamic.spawn",
            )
            assert r1.payload["status"] == "ok"
            r2 = await rt.ask(
                "workers",
                {
                    "class_path": "tests.conftest.EchoAgent",
                    "name": "a2",
                    "config": {},
                    "spawner": "agent-a",
                    "wait": True,
                },
                message_type="civitas.dynamic.spawn",
            )
            assert r2.payload["status"] == "error"
            assert "max_children_per_spawner" in r2.payload["reason"]
            r3 = await rt.ask(
                "workers",
                {
                    "class_path": "tests.conftest.EchoAgent",
                    "name": "b1",
                    "config": {},
                    "spawner": "agent-b",
                    "wait": True,
                },
                message_type="civitas.dynamic.spawn",
            )
            assert r3.payload["status"] == "ok"
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_max_total_spawns_per_spawner_not_refunded(self):
        dyn = _make_dyn(max_total_spawns_per_spawner=1)
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            r1 = await rt.ask(
                "workers",
                {
                    "class_path": "tests.conftest.EchoAgent",
                    "name": "a1",
                    "config": {},
                    "spawner": "agent-a",
                    "wait": True,
                },
                message_type="civitas.dynamic.spawn",
            )
            assert r1.payload["status"] == "ok"
            await rt.despawn("workers", "a1")
            r2 = await rt.ask(
                "workers",
                {
                    "class_path": "tests.conftest.EchoAgent",
                    "name": "a2",
                    "config": {},
                    "spawner": "agent-a",
                    "wait": True,
                },
                message_type="civitas.dynamic.spawn",
            )
            assert r2.payload["status"] == "error"
            assert "max_total_spawns_per_spawner" in r2.payload["reason"]
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_spawn_enforces_max_total_spawns(self):
        dyn = _make_dyn(max_total_spawns=1)
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt.spawn("workers", EchoAgent, name="echo-1")
            # despawn to free the slot, but total_spawns is still 1
            await rt.despawn("workers", "echo-1")
            with pytest.raises(SpawnError, match="max_total_spawns"):
                await rt.spawn("workers", EchoAgent, name="echo-2")
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_despawn_removes_agent(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt.spawn("workers", EchoAgent, name="echo-1")
            assert "echo-1" in dyn._dynamic_children
            await rt.despawn("workers", "echo-1")
            assert "echo-1" not in dyn._dynamic_children
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_stop_agent_drain_current(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt.spawn("workers", EchoAgent, name="echo-1")
            await rt.stop_agent("workers", "echo-1", drain="current")
            assert "echo-1" not in dyn._dynamic_children
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_stop_agent_drain_all(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt.spawn("workers", EchoAgent, name="echo-1")
            await rt.stop_agent("workers", "echo-1", drain="all", timeout=2.0)
            assert "echo-1" not in dyn._dynamic_children
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_slot_freed_after_despawn_allows_respawn(self):
        dyn = _make_dyn(max_children=1)
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt.spawn("workers", EchoAgent, name="echo-1")
            await rt.despawn("workers", "echo-1")
            # slot is freed — should be able to spawn again
            name = await rt.spawn("workers", EchoAgent, name="echo-1")
            assert name == "echo-1"
        finally:
            await rt.stop()


class TestAgentSpawnMethod:
    @pytest.mark.asyncio
    async def test_agent_spawn_method_wires_dyn_sup_name(self):
        orchestrator = NullAgent("orchestrator")
        dyn = _make_dyn()
        rt = Runtime(supervisor=Supervisor("root", children=[orchestrator, dyn]))
        await rt.start()
        try:
            assert orchestrator._dynamic_supervisor_name == "workers"
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_dynamic_supervisor_wires_itself(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            assert dyn._dynamic_supervisor_name == "workers"
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_agent_spawn_creates_child(self):
        class OrchestratorAgent(AgentProcess):
            def __init__(self) -> None:
                super().__init__("orchestrator")
                self.spawn_result: str | None = None

            async def handle(self, message: Message) -> Message | None:
                self.spawn_result = await self.spawn(EchoAgent, name="echo-1")
                return self.reply({"done": True})

        orchestrator = OrchestratorAgent()
        dyn = _make_dyn()
        rt = Runtime(supervisor=Supervisor("root", children=[orchestrator, dyn]))
        await rt.start()
        try:
            await rt.ask("orchestrator", {})
            assert orchestrator.spawn_result == "echo-1"
            reply = await rt.ask("echo-1", {"msg": "ping"})
            assert reply.payload["echo"]["msg"] == "ping"
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_agent_despawn_removes_child(self):
        class OrchestratorAgent(AgentProcess):
            def __init__(self) -> None:
                super().__init__("orchestrator")
                self.phase = 0

            async def handle(self, message: Message) -> Message | None:
                if self.phase == 0:
                    await self.spawn(EchoAgent, name="echo-1")
                    self.phase = 1
                elif self.phase == 1:
                    await self.despawn("echo-1")
                    self.phase = 2
                return self.reply({"phase": self.phase})

        orchestrator = OrchestratorAgent()
        dyn = _make_dyn()
        rt = Runtime(supervisor=Supervisor("root", children=[orchestrator, dyn]))
        await rt.start()
        try:
            await rt.ask("orchestrator", {})  # spawn
            await rt.ask("orchestrator", {})  # despawn
            assert "echo-1" not in dyn._dynamic_children
        finally:
            await rt.stop()


class TestDynamicSupervisorAncestorWiring:
    @pytest.mark.asyncio
    async def test_nested_supervisor_wires_dyn_sup_name(self):
        orchestrator = NullAgent("orchestrator")
        dyn = _make_dyn()
        inner_sup = Supervisor("inner", children=[orchestrator, dyn])
        rt = Runtime(supervisor=Supervisor("root", children=[inner_sup]))
        await rt.start()
        try:
            assert orchestrator._dynamic_supervisor_name == "workers"
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_no_dyn_sup_leaves_name_as_none(self):
        agent = NullAgent("agent")
        rt = Runtime(supervisor=Supervisor("root", children=[agent]))
        await rt.start()
        try:
            assert agent._dynamic_supervisor_name is None
        finally:
            await rt.stop()


class TestRestartSemantics:
    @pytest.mark.asyncio
    async def test_transient_clean_exit_removes_without_restart(self):
        """CleanExitAgent stops cleanly — transient mode should NOT restart."""
        dyn = _make_dyn(restart="transient")
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt.spawn("workers", CleanExitAgent, name="clean-1")
            agent = dyn._dynamic_children["clean-1"].agent
            # Trigger clean exit by sending a message
            await agent._mailbox.put(Message(type="go", sender="_test", recipient="clean-1"))
            # Wait for exit and removal
            await wait_for(
                lambda: "clean-1" not in dyn._dynamic_children, timeout=2.0, msg="child removal"
            )
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_child_terminated_notification_on_restart_exhaustion(self):
        """on_child_terminated is called on spawner when restarts are exhausted."""
        recorder = TerminationRecorder("orchestrator")
        dyn = _make_dyn(restart="transient", max_restarts=1, restart_window=60.0)

        rt = Runtime(supervisor=Supervisor("root", children=[recorder, dyn]))
        await rt.start()
        try:
            # Spawn with spawner="orchestrator"
            reply = await rt.ask(
                "workers",
                {
                    "class_path": "tests.unit.test_dynamic_supervisor.CrashAgent",
                    "name": "crash-1",
                    "config": {},
                    "spawner": "orchestrator",
                },
                message_type="civitas.dynamic.spawn",
            )
            assert reply.payload["status"] == "ok"

            # Trigger crash by sending messages — agent crashes immediately on handle()
            agent = dyn._dynamic_children["crash-1"].agent
            for _ in range(5):
                await agent._mailbox.put(Message(type="go", sender="_test", recipient="crash-1"))

            # Wait for restarts to be exhausted and notification to arrive
            await wait_for(
                lambda: any(name == "crash-1" for name, _ in recorder.terminated),
                timeout=5.0,
                msg="terminated notification",
            )
            assert recorder.terminated[0][1] == "restarts_exhausted"
        finally:
            await rt.stop()


# ---------------------------------------------------------------------------
# on_child_terminated default implementation
# ---------------------------------------------------------------------------


async def test_on_child_terminated_default_logs_warning(caplog: pytest.LogCaptureFixture):
    agent = NullAgent("agent")
    with caplog.at_level("WARNING", logger="civitas.process"):
        await agent.on_child_terminated("worker-1", "restarts_exhausted")
    assert any("worker-1" in r.message for r in caplog.records)
    assert any("restarts_exhausted" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Runtime.from_config — type: dynamic_supervisor
# ---------------------------------------------------------------------------


class TestFromConfigDynamicSupervisor:
    @pytest.mark.asyncio
    async def test_yaml_type_dynamic_supervisor_parsed(self, tmp_path: Path):
        yaml_text = textwrap.dedent("""\
            supervision:
              name: root
              strategy: ONE_FOR_ONE
              children:
                - name: workers
                  type: dynamic_supervisor
                  max_children: 5
                  max_total_spawns: 50
                  restart: transient
        """)
        config_file = tmp_path / "topology.yaml"
        config_file.write_text(yaml_text)

        rt = Runtime.from_config(config_file)
        await rt.start()
        try:
            dyn = rt._root_supervisor._children_by_name.get("workers")
            assert isinstance(dyn, DynamicSupervisor)
            assert dyn.max_children == 5
            assert dyn.max_total_spawns == 50
            assert dyn._restart_mode == RestartMode.TRANSIENT
        finally:
            await rt.stop()


# ---------------------------------------------------------------------------
# print_tree shows [dyn] label
# ---------------------------------------------------------------------------


def test_print_tree_shows_dyn_label():
    dyn = _make_dyn()
    rt = Runtime(supervisor=Supervisor("root", children=[dyn]))
    tree = rt.print_tree()
    assert "[dyn]" in tree
    assert "workers" in tree


# ---------------------------------------------------------------------------
# R1 non-blocking spawn — helper agents, gates, fixtures
# ---------------------------------------------------------------------------

# Module-level maps let spawned agents (created deep inside the supervisor) report
# lifecycle phases and be gated mid-on_start without passing non-serializable
# objects through the spawn config.
_LIFECYCLE: dict[str, list[Any]] = {}
_START_GATES: dict[str, asyncio.Event] = {}

_CLASS = "tests.unit.test_dynamic_supervisor.LifecycleAgent"


@pytest.fixture(autouse=True)
def _reset_r1_state():
    _LIFECYCLE.clear()
    _START_GATES.clear()
    yield
    _LIFECYCLE.clear()
    _START_GATES.clear()


class LifecycleAgent(AgentProcess):
    """Config-driven agent that records start-lifecycle phases into _LIFECYCLE.

    config keys: ``fail_on`` ("restore"|"on_start"), ``throw_on_stop`` (bool),
    ``gate`` (block in on_start until ``_START_GATES[name]`` is set).
    """

    async def _restore_state(self) -> None:
        _LIFECYCLE.setdefault(self.name, []).append("restore")
        if self.config.get("fail_on") == "restore":
            raise RuntimeError("restore boom")
        await super()._restore_state()

    async def on_start(self) -> None:
        if self.config.get("gate"):
            gate = _START_GATES.get(self.name)
            if gate is not None:
                await gate.wait()
        _LIFECYCLE.setdefault(self.name, []).append("on_start")
        if self.config.get("fail_on") == "on_start":
            raise RuntimeError("on_start boom")

    async def on_stop(self) -> None:
        _LIFECYCLE.setdefault(self.name, []).append("on_stop")
        if self.config.get("throw_on_stop"):
            raise RuntimeError("on_stop boom too")

    async def handle(self, message: Message) -> Message | None:
        _LIFECYCLE.setdefault(self.name, []).append(("handle", message.payload.get("seq")))
        return self.reply({"echo": message.payload})


class SuspendedCrashAgent(AgentProcess):
    """Escalates on any dispatched message — used to crash a SUSPENDED child."""

    async def handle(self, message: Message) -> Message | None:
        raise RuntimeError("crash while suspended")

    async def on_error(self, error: Exception, message: Message) -> ErrorAction:
        return ErrorAction.ESCALATE


async def _spawn_via_ask(
    rt: Runtime,
    name: str,
    *,
    wait: bool,
    config: dict[str, Any] | None = None,
    spawner: str = "",
    class_path: str = _CLASS,
) -> Message:
    """Send a civitas.dynamic.spawn message directly so spawner/wait can be set."""
    return await rt.ask(
        "workers",
        {
            "class_path": class_path,
            "name": name,
            "config": config or {},
            "spawner": spawner,
            "wait": wait,
        },
        message_type="civitas.dynamic.spawn",
    )


# ---------------------------------------------------------------------------
# R1 — reply timing / backward compatibility
# ---------------------------------------------------------------------------


class TestR1ReplyTiming:
    @pytest.mark.asyncio
    async def test_wait_true_success_reply_ready_and_running(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            reply = await _spawn_via_ask(rt, "w1", wait=True, class_path="tests.conftest.EchoAgent")
            assert reply.payload["status"] == "ok"
            assert reply.payload["ready"] is True
            assert reply.payload["state"] == "RUNNING"
            echo = await rt.ask("w1", {"msg": "hi"}, timeout=3.0)
            assert echo.payload["echo"]["msg"] == "hi"
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_wait_false_success_reply_not_ready(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            _START_GATES["gw"] = asyncio.Event()
            reply = await _spawn_via_ask(rt, "gw", wait=False, config={"gate": True})
            assert reply.payload["status"] == "ok"
            assert reply.payload["ready"] is False
            agent = dyn._dynamic_children["gw"].agent
            assert agent.status == ProcessStatus.INITIALIZING
            _START_GATES["gw"].set()
            await wait_for_status(agent, ProcessStatus.RUNNING, timeout=3.0)
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_wait_false_buffered_asks_resolve_fifo(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            _START_GATES["fifo"] = asyncio.Event()
            reply = await _spawn_via_ask(rt, "fifo", wait=False, config={"gate": True})
            assert reply.payload["ready"] is False
            tasks = []
            for seq in range(3):
                tasks.append(asyncio.create_task(rt.ask("fifo", {"seq": seq}, timeout=5.0)))
                await asyncio.sleep(0.02)
            _START_GATES["fifo"].set()
            replies = await asyncio.gather(*tasks)
            assert [r.payload["echo"]["seq"] for r in replies] == [0, 1, 2]
            handled = [x for x in _LIFECYCLE["fifo"] if isinstance(x, tuple)]
            assert handled == [("handle", 0), ("handle", 1), ("handle", 2)]
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_spawn_nowait_alias_returns_before_on_start(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            _START_GATES["nw"] = asyncio.Event()
            name = await rt.spawn_nowait("workers", LifecycleAgent, "nw", config={"gate": True})
            assert name == "nw"
            agent = dyn._dynamic_children["nw"].agent
            assert agent.status == ProcessStatus.INITIALIZING
            _START_GATES["nw"].set()
            await wait_for_status(agent, ProcessStatus.RUNNING, timeout=3.0)
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_wait_true_suspended_marker_reply_state_suspended(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt._state_store.set(
                "susp", {AgentProcess._SUSPEND_STATE_KEY: {"reason": "seed", "since": 0.0}}
            )
            reply = await _spawn_via_ask(
                rt, "susp", wait=True, class_path="tests.conftest.EchoAgent"
            )
            assert reply.payload["status"] == "ok"
            assert reply.payload["ready"] is True
            assert reply.payload["state"] == "SUSPENDED"
            agent = dyn._dynamic_children["susp"].agent
            assert agent.status == ProcessStatus.SUSPENDED
            assert agent._reached_loop is True
        finally:
            await rt.stop()


# ---------------------------------------------------------------------------
# R1 — failure semantics
# ---------------------------------------------------------------------------


class TestR1FailureSemantics:
    @pytest.mark.asyncio
    async def test_wait_true_on_start_failure_error_reply_and_cleanup(self):
        recorder = TerminationRecorder("orchestrator")
        dyn = _make_dyn()
        rt = Runtime(supervisor=Supervisor("root", children=[recorder, dyn]))
        await rt.start()
        try:
            reply = await _spawn_via_ask(
                rt, "f", wait=True, config={"fail_on": "on_start"}, spawner="orchestrator"
            )
            assert reply.payload["status"] == "error"
            assert reply.payload["phase"] == "on_start"
            assert "on_start boom" in reply.payload["error"]
            # Cleaned up before the reply is observed (B2/D7).
            assert "f" not in dyn._dynamic_children
            assert not rt._registry.has("f")
            assert "f" not in rt._transport._handlers
            # wait=True failure reply IS the notification — no on_child_terminated (D6).
            assert recorder.terminated == []
            # on_stop ran on on_start failure; restore-phase happened first.
            assert _LIFECYCLE["f"] == ["restore", "on_start", "on_stop"]
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_wait_false_on_start_failure_fails_pending_ask_and_notifies(
        self, caplog: pytest.LogCaptureFixture
    ):
        recorder = TerminationRecorder("orchestrator")
        dyn = _make_dyn()
        rt = Runtime(supervisor=Supervisor("root", children=[recorder, dyn]))
        await rt.start()
        try:
            _START_GATES["c1"] = asyncio.Event()
            reply = await _spawn_via_ask(
                rt,
                "c1",
                wait=False,
                config={"gate": True, "fail_on": "on_start"},
                spawner="orchestrator",
            )
            assert reply.payload["ready"] is False
            ask_task = asyncio.create_task(rt.ask("c1", {"q": 1}, timeout=5.0))
            await asyncio.sleep(0.05)
            await rt.send("c1", {"fire": 1})
            await asyncio.sleep(0.05)
            with caplog.at_level("INFO", logger="civitas.bus"):
                _START_GATES["c1"].set()
                pending = await asyncio.wait_for(ask_task, timeout=3.0)
            assert pending.payload["status"] == "error"
            await wait_for(
                lambda: any(n == "c1" for n, _ in recorder.terminated),
                timeout=3.0,
                msg="terminated notification",
            )
            assert recorder.terminated[0][1].startswith("on_start:")
            assert any("dropping buffered message" in r.message for r in caplog.records)
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_restore_failure_skips_on_stop(self):
        recorder = TerminationRecorder("orchestrator")
        dyn = _make_dyn()
        rt = Runtime(supervisor=Supervisor("root", children=[recorder, dyn]))
        await rt.start()
        try:
            reply = await _spawn_via_ask(
                rt, "rf", wait=True, config={"fail_on": "restore"}, spawner="orchestrator"
            )
            assert reply.payload["status"] == "error"
            assert reply.payload["phase"] == "restore"
            assert _LIFECYCLE["rf"] == ["restore"]
            assert "rf" not in dyn._dynamic_children
            assert not rt._registry.has("rf")
            assert recorder.terminated == []
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_restore_failure_wait_false_notifies_restore_phase(self):
        recorder = TerminationRecorder("orchestrator")
        dyn = _make_dyn()
        rt = Runtime(supervisor=Supervisor("root", children=[recorder, dyn]))
        await rt.start()
        try:
            await _spawn_via_ask(
                rt, "rf2", wait=False, config={"fail_on": "restore"}, spawner="orchestrator"
            )
            await wait_for(
                lambda: any(n == "rf2" for n, _ in recorder.terminated),
                timeout=3.0,
                msg="terminated notification",
            )
            assert recorder.terminated[0][1].startswith("restore:")
            assert _LIFECYCLE["rf2"] == ["restore"]
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_cancel_during_on_start_no_restart_no_notify(self):
        recorder = TerminationRecorder("orchestrator")
        dyn = _make_dyn(restart="permanent")
        rt = Runtime(supervisor=Supervisor("root", children=[recorder, dyn]))
        await rt.start()
        try:
            _START_GATES["cx"] = asyncio.Event()
            await _spawn_via_ask(
                rt, "cx", wait=False, config={"gate": True}, spawner="orchestrator"
            )
            rec = dyn._dynamic_children["cx"]
            rec.task.cancel()
            await asyncio.gather(rec.task, return_exceptions=True)
            await asyncio.sleep(0.05)
            assert recorder.terminated == []
            assert dyn._child_restart_counts.get("cx", 0) == 0
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_cancel_during_running_no_restart_no_notify(self):
        recorder = TerminationRecorder("orchestrator")
        dyn = _make_dyn(restart="permanent")
        rt = Runtime(supervisor=Supervisor("root", children=[recorder, dyn]))
        await rt.start()
        try:
            reply = await _spawn_via_ask(
                rt, "cr", wait=True, spawner="orchestrator", class_path="tests.conftest.EchoAgent"
            )
            assert reply.payload["ready"] is True
            rec = dyn._dynamic_children["cr"]
            rec.task.cancel()
            await asyncio.gather(rec.task, return_exceptions=True)
            await asyncio.sleep(0.05)
            assert recorder.terminated == []
            assert dyn._child_restart_counts.get("cr", 0) == 0
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_d8_init_failure_is_terminal_no_restart(self):
        dyn = _make_dyn(restart="permanent")
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            reply = await _spawn_via_ask(rt, "t", wait=True, config={"fail_on": "on_start"})
            assert reply.payload["status"] == "error"
            assert "t" not in dyn._dynamic_children
            assert dyn._child_restart_counts.get("t", 0) == 0
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_d8_post_running_crash_restarts(self):
        dyn = _make_dyn(restart="permanent", max_restarts=5)
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            reply = await _spawn_via_ask(
                rt,
                "crash-1",
                wait=True,
                class_path="tests.unit.test_dynamic_supervisor.CrashAgent",
            )
            assert reply.payload["ready"] is True
            agent = dyn._dynamic_children["crash-1"].agent
            await agent._mailbox.put(Message(type="go", sender="_t", recipient="crash-1"))
            await wait_for(
                lambda: dyn._child_restart_counts.get("crash-1", 0) >= 1,
                timeout=3.0,
                msg="restart after post-RUNNING crash",
            )
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_d8_suspended_then_crash_restarts(self):
        dyn = _make_dyn(restart="permanent", max_restarts=5)
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt._state_store.set(
                "sc", {AgentProcess._SUSPEND_STATE_KEY: {"reason": "seed", "since": 0.0}}
            )
            reply = await _spawn_via_ask(
                rt,
                "sc",
                wait=True,
                class_path="tests.unit.test_dynamic_supervisor.SuspendedCrashAgent",
            )
            assert reply.payload["state"] == "SUSPENDED"
            agent = dyn._dynamic_children["sc"].agent
            assert agent._reached_loop is True
            await agent._mailbox.put(Message(type="go", sender="_t", recipient="sc", priority=1))
            await wait_for(
                lambda: dyn._child_restart_counts.get("sc", 0) >= 1,
                timeout=3.0,
                msg="restart after suspended crash",
            )
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_d12_throwing_on_stop_is_swallowed(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            reply = await _spawn_via_ask(
                rt, "d12", wait=True, config={"fail_on": "on_start", "throw_on_stop": True}
            )
            assert reply.payload["status"] == "error"
            assert reply.payload["phase"] == "on_start"
            assert "on_start boom" in reply.payload["error"]
            assert "on_stop boom" not in reply.payload["error"]
            assert _LIFECYCLE["d12"] == ["restore", "on_start", "on_stop"]
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_d7_double_terminal_cleanup_is_idempotent(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            reply = await _spawn_via_ask(rt, "idem", wait=True, config={"fail_on": "on_start"})
            assert reply.payload["status"] == "error"
            assert "idem" not in dyn._dynamic_children
            assert not rt._registry.has("idem")
            await dyn._terminal_cleanup("idem")
            assert "idem" not in dyn._dynamic_children
            assert not rt._registry.has("idem")
        finally:
            await rt.stop()


# ---------------------------------------------------------------------------
# R1 — accounting
# ---------------------------------------------------------------------------


class TestR1Accounting:
    @pytest.mark.asyncio
    async def test_max_total_spawns_not_refunded_on_failure(self):
        dyn = _make_dyn(max_total_spawns=1)
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            reply = await _spawn_via_ask(rt, "f1", wait=True, config={"fail_on": "on_start"})
            assert reply.payload["status"] == "error"
            assert dyn._total_spawns == 1
            with pytest.raises(SpawnError, match="max_total_spawns"):
                await rt.spawn("workers", EchoAgent, name="f2")
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_max_children_slot_freed_after_failed_spawn(self):
        dyn = _make_dyn(max_children=1)
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            reply = await _spawn_via_ask(rt, "mc1", wait=True, config={"fail_on": "on_start"})
            assert reply.payload["status"] == "error"
            assert "mc1" not in dyn._dynamic_children
            name = await rt.spawn("workers", EchoAgent, name="mc2")
            assert name == "mc2"
        finally:
            await rt.stop()


# ---------------------------------------------------------------------------
# R2 cross-tree spawn — spawn_into(), P0 collision guard, current_spawner,
# spawner_allowlist + audit, reserved marker capability
# ---------------------------------------------------------------------------

_MARKER = "_agency.dynamic_supervisor"


class SpawnIntoDriver(AgentProcess):
    """Agent that calls spawn_into()/spawn() on request and records terminations."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.terminated: list[tuple[str, str]] = []

    async def on_child_terminated(self, name: str, reason: str) -> None:
        self.terminated.append((name, reason))

    async def handle(self, message: Message) -> Message | None:
        p = message.payload
        agent_cls = LifecycleAgent if p.get("lifecycle") else EchoAgent
        try:
            if p.get("op") == "spawn":
                name = await self.spawn(
                    agent_cls, p["child"], p.get("config"), wait=p.get("wait", True)
                )
            else:
                name = await self.spawn_into(
                    p["target"], agent_cls, p["child"], p.get("config"), wait=p.get("wait", True)
                )
            return self.reply({"ok": True, "name": name})
        except SpawnError as exc:
            return self.reply({"ok": False, "error": str(exc)})


class _RecordingAuditSink:
    """AuditSink that records emitted events for assertions."""

    def __init__(self) -> None:
        self.events: list[Any] = []

    async def emit(self, event: Any) -> None:
        self.events.append(event)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _RaceRegistry(LocalRegistry):
    """LocalRegistry whose register() always raises — models the cross-supervisor race.

    lookup() still returns None for unknown names, so the global pre-check passes and
    execution reaches the wrapped register() call (the load-bearing P0 guard).
    """

    def register(
        self,
        name: str,
        address: str | None = None,
        *,
        is_local: bool = True,
        capabilities: Any = None,
        capability_metadata: Any = None,
    ) -> None:
        raise ValueError(f"Process already registered: {name!r}")


def _two_dynsup_runtime() -> tuple[Runtime, SpawnIntoDriver, DynamicSupervisor, DynamicSupervisor]:
    driver = SpawnIntoDriver("x")
    a = DynamicSupervisor("A")
    b = DynamicSupervisor("B")
    rt = Runtime(supervisor=Supervisor("root", children=[driver, a, b]))
    return rt, driver, a, b


class TestP0CollisionGuard:
    @pytest.mark.asyncio
    async def test_collision_precheck_error_reply_supervisor_survives(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn, extra_children=[EchoAgent("dup")])
        await rt.start()
        try:
            with pytest.raises(SpawnError, match="already registered"):
                await rt.spawn("workers", EchoAgent, "dup")
            workers = rt.get_agent("workers")
            assert workers is not None
            assert workers.status == ProcessStatus.RUNNING
            # The supervisor is unharmed — a fresh spawn still succeeds.
            assert await rt.spawn("workers", EchoAgent, "fresh") == "fresh"
        finally:
            await rt.stop()

    async def test_collision_register_valueerror_path_no_crash(self):
        ds = _make_dyn()
        ds._registry = _RaceRegistry()
        msg = _fake_message(
            "civitas.dynamic.spawn",
            {"class_path": "tests.conftest.EchoAgent", "name": "dup", "config": {}, "spawner": ""},
        )
        reply = await _dispatch(ds, msg)
        assert reply is not None
        assert reply.payload["status"] == "error"
        assert "already registered" in reply.payload["reason"]
        assert "dup" not in ds._dynamic_children
        assert ds._total_spawns == 0


class TestReservedMarker:
    @pytest.mark.asyncio
    async def test_marker_present_on_code_first_dynsup(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            entry = rt._registry.lookup("workers")
            assert entry is not None
            assert _MARKER in entry.capabilities
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_marker_survives_yaml_capabilities_override(self, tmp_path: Path):
        yaml_text = textwrap.dedent("""\
            supervision:
              name: root
              children:
                - name: workers
                  type: dynamic_supervisor
                  capabilities: [custom.tag]
        """)
        config_file = tmp_path / "topology.yaml"
        config_file.write_text(yaml_text)
        rt = Runtime.from_config(config_file)
        await rt.start()
        try:
            entry = rt._registry.lookup("workers")
            assert entry is not None
            assert _MARKER in entry.capabilities
            assert "custom.tag" in entry.capabilities
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_nested_dynamic_dynsup_gets_marker(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            await rt.spawn("workers", DynamicSupervisor, "sub")
            entry = rt._registry.lookup("sub")
            assert entry is not None
            assert _MARKER in entry.capabilities
        finally:
            await rt.stop()


class TestSpawnInto:
    @pytest.mark.asyncio
    async def test_places_child_under_named_supervisor(self):
        rt, _driver, a, b = _two_dynsup_runtime()
        await rt.start()
        try:
            reply = await rt.ask("x", {"op": "spawn_into", "target": "B", "child": "c1"})
            assert reply.payload["ok"] is True
            assert "c1" in b._dynamic_children
            assert "c1" not in a._dynamic_children
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_spawn_still_targets_nearest_ancestor(self):
        rt, _driver, a, b = _two_dynsup_runtime()
        await rt.start()
        try:
            reply = await rt.ask("x", {"op": "spawn", "child": "c2"})
            assert reply.payload["ok"] is True
            assert "c2" in a._dynamic_children
            assert "c2" not in b._dynamic_children
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_unknown_supervisor_raises(self):
        rt, _driver, _a, _b = _two_dynsup_runtime()
        await rt.start()
        try:
            reply = await rt.ask("x", {"op": "spawn_into", "target": "nope", "child": "c"})
            assert reply.payload["ok"] is False
            assert "no such supervisor" in reply.payload["error"]
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_non_supervisor_target_raises(self):
        driver = SpawnIntoDriver("x")
        a = DynamicSupervisor("A")
        plain = EchoAgent("plain")
        rt = Runtime(supervisor=Supervisor("root", children=[driver, a, plain]))
        await rt.start()
        try:
            reply = await rt.ask("x", {"op": "spawn_into", "target": "plain", "child": "c"})
            assert reply.payload["ok"] is False
            assert "not a DynamicSupervisor" in reply.payload["error"]
        finally:
            await rt.stop()

    async def test_self_target_raises(self):
        agent = NullAgent("solo")
        with pytest.raises(SpawnError, match="cannot spawn into self"):
            await agent.spawn_into("solo", EchoAgent, "child")

    @pytest.mark.asyncio
    async def test_wait_false_cross_tree_success(self):
        rt, _driver, _a, b = _two_dynsup_runtime()
        await rt.start()
        try:
            _START_GATES["okc"] = asyncio.Event()
            reply = await rt.ask(
                "x",
                {
                    "op": "spawn_into",
                    "target": "B",
                    "child": "okc",
                    "wait": False,
                    "lifecycle": True,
                    "config": {"gate": True},
                },
            )
            assert reply.payload["ok"] is True
            agent = b._dynamic_children["okc"].agent
            assert agent.status == ProcessStatus.INITIALIZING
            _START_GATES["okc"].set()
            await wait_for_status(agent, ProcessStatus.RUNNING, timeout=3.0)
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_wait_false_cross_tree_start_failure_notifies_spawner(self):
        rt, driver, a, b = _two_dynsup_runtime()
        await rt.start()
        try:
            _START_GATES["cf"] = asyncio.Event()
            reply = await rt.ask(
                "x",
                {
                    "op": "spawn_into",
                    "target": "B",
                    "child": "cf",
                    "wait": False,
                    "lifecycle": True,
                    "config": {"gate": True, "fail_on": "on_start"},
                },
            )
            assert reply.payload["ok"] is True  # immediate ok before on_start runs
            assert "cf" in b._dynamic_children
            assert "cf" not in a._dynamic_children
            _START_GATES["cf"].set()
            await wait_for(
                lambda: any(n == "cf" for n, _ in driver.terminated),
                timeout=3.0,
                msg="cross-tree terminated notification",
            )
            assert driver.terminated[0][1].startswith("on_start:")
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_suspended_target_times_out(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr("civitas.process._SPAWN_ASK_TIMEOUT", 0.3)
        rt, _driver, _a, b = _two_dynsup_runtime()
        await rt.start()
        try:
            await rt.suspend("B")
            await wait_for_status(b, ProcessStatus.SUSPENDED, timeout=2.0)
            reply = await rt.ask("x", {"op": "spawn_into", "target": "B", "child": "c"})
            assert reply.payload["ok"] is False
            assert "timed out" in reply.payload["error"]
        finally:
            await rt.stop()


class TestCurrentSpawner:
    @pytest.mark.asyncio
    async def test_visible_in_hook_and_none_outside(self):
        seen: dict[str, str | None] = {}

        class RecordingSup(DynamicSupervisor):
            async def on_spawn_requested(self, agent_class, name, config) -> bool:
                seen["during"] = self.current_spawner
                return True

        driver = SpawnIntoDriver("x")
        sup = RecordingSup("A")
        rt = Runtime(supervisor=Supervisor("root", children=[driver, sup]))
        await rt.start()
        try:
            reply = await rt.ask("x", {"op": "spawn", "child": "c"})
            assert reply.payload["ok"] is True
            assert seen["during"] == "x"
            assert sup.current_spawner is None
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_cleared_after_governance_denial(self):
        class DenySup(DynamicSupervisor):
            async def on_spawn_requested(self, agent_class, name, config) -> bool:
                return False

        sup = DenySup("A")
        rt = Runtime(supervisor=Supervisor("root", children=[sup]))
        await rt.start()
        try:
            reply = await rt.ask(
                "A",
                {
                    "class_path": "tests.conftest.EchoAgent",
                    "name": "c",
                    "config": {},
                    "spawner": "z",
                },
                message_type="civitas.dynamic.spawn",
            )
            assert reply.payload["status"] == "error"
            assert sup.current_spawner is None
        finally:
            await rt.stop()


class TestSpawnerAllowlist:
    @pytest.mark.asyncio
    async def test_default_none_allows_any_spawner(self):
        dyn = _make_dyn()
        rt = _build_runtime(dyn)
        await rt.start()
        try:
            assert await rt.spawn("workers", EchoAgent, "c") == "c"
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_unlisted_spawner_rejected_before_hook(self):
        hook_calls: list[str] = []

        class TrackingSup(DynamicSupervisor):
            async def on_spawn_requested(self, agent_class, name, config) -> bool:
                hook_calls.append(name)
                return True

        sup = TrackingSup("A", spawner_allowlist={"allowed"})
        rt = Runtime(supervisor=Supervisor("root", children=[sup]))
        await rt.start()
        try:
            reply = await rt.ask(
                "A",
                {
                    "class_path": "tests.conftest.EchoAgent",
                    "name": "c1",
                    "config": {},
                    "spawner": "blocked",
                },
                message_type="civitas.dynamic.spawn",
            )
            assert reply.payload["status"] == "error"
            assert "not allowed" in reply.payload["reason"]
            assert "c1" not in sup._dynamic_children
            assert hook_calls == []
        finally:
            await rt.stop()

    @pytest.mark.asyncio
    async def test_allowed_spawner_spawns_and_emits_audit(self):
        sink = _RecordingAuditSink()
        sup = DynamicSupervisor("A", spawner_allowlist={"allowed"})
        rt = Runtime(supervisor=Supervisor("root", children=[sup]))
        rt._audit_sink = sink
        await rt.start()
        try:
            reply = await rt.ask(
                "A",
                {
                    "class_path": "tests.conftest.EchoAgent",
                    "name": "c1",
                    "config": {},
                    "spawner": "allowed",
                },
                message_type="civitas.dynamic.spawn",
            )
            assert reply.payload["status"] == "ok"
            assert "c1" in sup._dynamic_children
            spawn_events = [e for e in sink.events if e["event"] == "dynamic.spawn"]
            assert len(spawn_events) == 1
            assert spawn_events[0]["agent"] == "A"
            assert spawn_events[0]["details"] == {
                "spawner": "allowed",
                "child": "c1",
                "class_path": "tests.conftest.EchoAgent",
                "supervisor": "A",
            }
        finally:
            await rt.stop()
