"""Tests for DynamicSupervisor — spawn, despawn, stop, governance, restart semantics."""

from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

import pytest

from civitas import AgentProcess, DynamicSupervisor, NullSink, Runtime, Supervisor
from civitas.errors import SpawnError
from civitas.messages import Message
from civitas.process import ProcessStatus
from civitas.supervisor import RestartMode
from tests.conftest import EchoAgent, wait_for

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
            child = dyn._dynamic_children["echo-1"]
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
            agent = dyn._dynamic_children["clean-1"]
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
            agent = dyn._dynamic_children["crash-1"]
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
