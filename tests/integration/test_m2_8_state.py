"""M2.8 — State Persistence & Crash Recovery testable criteria.

Tests validate that agent state survives process crashes and restarts via
SQLiteStateStore, that checkpointing works correctly, and that stateless
agents are unaffected.
"""

import asyncio
import os
import tempfile

import pytest

# The SQLite state store moved to civitas-contrib in the core/contrib split (#40):
# this module integration-tests persistence against it and skips cleanly without
# contrib (InMemory checkpoint behavior is unit-covered in tests/unit).
pytest.importorskip(
    "civitas_contrib.plugins.sqlite_store",
    reason="civitas-contrib not installed — skipping SQLite state-persistence integration tests",
)

from civitas_contrib.plugins.sqlite_store import SQLiteStateStore

from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message
from civitas.plugins.loader import load_plugin
from civitas.plugins.state import InMemoryStateStore

# ---------------------------------------------------------------------------
# Test agents
# ---------------------------------------------------------------------------


class StatefulCounter(AgentProcess):
    """Counts messages and checkpoints after each one."""

    async def on_start(self) -> None:
        if "count" not in self.state:
            self.state["count"] = 0

    async def handle(self, message: Message) -> Message | None:
        self.state["count"] += 1
        await self.checkpoint()
        return self.reply({"count": self.state["count"]})


class MultiStepWorkflow(AgentProcess):
    """Processes steps with checkpointing — can resume from last checkpoint."""

    async def on_start(self) -> None:
        if "step" not in self.state:
            self.state["step"] = 0
            self.state["results"] = []

    async def handle(self, message: Message) -> Message | None:
        total = message.payload.get("total_steps", 3)
        start = self.state["step"]

        for i in range(start, total):
            self.state["step"] = i + 1
            self.state["results"].append(f"step_{i + 1}")
            await self.checkpoint()

        results = self.state["results"]
        # Reset for next run
        self.state = {"step": 0, "results": []}
        await self.checkpoint()
        return self.reply({"completed": True, "results": results})


class StatelessAgent(AgentProcess):
    """Agent that never checkpoints — should be unaffected by state system."""

    async def handle(self, message: Message) -> Message | None:
        return self.reply({"echo": message.payload.get("text", "")})


class CrashOnStep(AgentProcess):
    """Crashes on a specific step to test crash recovery."""

    async def on_start(self) -> None:
        if "step" not in self.state:
            self.state["step"] = 0

    async def handle(self, message: Message) -> Message | None:
        crash_at = message.payload.get("crash_at", -1)
        total = message.payload.get("total_steps", 5)

        for i in range(self.state["step"], total):
            self.state["step"] = i + 1
            await self.checkpoint()
            if self.state["step"] == crash_at:
                raise RuntimeError(f"Crash at step {crash_at}")

        step = self.state["step"]
        self.state = {"step": 0}
        await self.checkpoint()
        return self.reply({"completed": True, "final_step": step})


# ---------------------------------------------------------------------------
# SQLiteStateStore tests
# ---------------------------------------------------------------------------


async def test_sqlite_store_set_and_get():
    """SQLiteStateStore persists and retrieves agent state."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = SQLiteStateStore(db_path)
        await store.set("agent_a", {"count": 42, "data": [1, 2, 3]})

        result = await store.get("agent_a")
        assert result == {"count": 42, "data": [1, 2, 3]}
        store.close()
    finally:
        os.unlink(db_path)


async def test_sqlite_store_get_missing():
    """SQLiteStateStore returns None for unknown agents."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = SQLiteStateStore(db_path)
        result = await store.get("nonexistent")
        assert result is None
        store.close()
    finally:
        os.unlink(db_path)


async def test_sqlite_store_upsert():
    """SQLiteStateStore updates existing state (upsert)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = SQLiteStateStore(db_path)
        await store.set("agent_a", {"v": 1})
        await store.set("agent_a", {"v": 2})

        result = await store.get("agent_a")
        assert result == {"v": 2}
        store.close()
    finally:
        os.unlink(db_path)


async def test_sqlite_store_delete():
    """SQLiteStateStore deletes agent state."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = SQLiteStateStore(db_path)
        await store.set("agent_a", {"v": 1})
        await store.delete("agent_a")

        result = await store.get("agent_a")
        assert result is None
        store.close()
    finally:
        os.unlink(db_path)


async def test_sqlite_store_scoped_per_agent():
    """State is scoped per-agent — agent A state does not affect agent B."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = SQLiteStateStore(db_path)
        await store.set("agent_a", {"x": 1})
        await store.set("agent_b", {"y": 2})

        assert await store.get("agent_a") == {"x": 1}
        assert await store.get("agent_b") == {"y": 2}

        await store.delete("agent_a")
        assert await store.get("agent_a") is None
        assert await store.get("agent_b") == {"y": 2}
        store.close()
    finally:
        os.unlink(db_path)


async def test_sqlite_store_list_agents():
    """SQLiteStateStore lists all agents with persisted state."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = SQLiteStateStore(db_path)
        await store.set("charlie", {"v": 1})
        await store.set("alice", {"v": 2})
        await store.set("bob", {"v": 3})

        agents = await store.list_agents()
        assert agents == ["alice", "bob", "charlie"]  # alphabetical
        store.close()
    finally:
        os.unlink(db_path)


async def test_sqlite_store_survives_reopen():
    """State persists across store close/reopen (simulates process restart)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        # Write state
        store1 = SQLiteStateStore(db_path)
        await store1.set("agent_a", {"step": 3, "data": "hello"})
        store1.close()

        # Reopen and read
        store2 = SQLiteStateStore(db_path)
        result = await store2.get("agent_a")
        assert result == {"step": 3, "data": "hello"}
        store2.close()
    finally:
        os.unlink(db_path)


# ---------------------------------------------------------------------------
# Agent checkpoint/restore tests
# ---------------------------------------------------------------------------


async def test_agent_checkpoint_saves_state():
    """Agent checkpoint() saves self.state to the store."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = SQLiteStateStore(db_path)
        runtime = Runtime(
            supervisor=Supervisor("root", children=[StatefulCounter("counter")]),
            state_store=store,
        )
        await runtime.start()
        try:
            await runtime.ask("counter", {})
            await runtime.ask("counter", {})

            # Verify state was persisted
            saved = await store.get("counter")
            assert saved == {"count": 2}
        finally:
            await runtime.stop()
            store.close()
    finally:
        os.unlink(db_path)


async def test_agent_restores_from_checkpoint():
    """Agent self.state is restored from checkpoint on restart."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        # First run — count to 3
        store1 = SQLiteStateStore(db_path)
        runtime1 = Runtime(
            supervisor=Supervisor("root", children=[StatefulCounter("counter")]),
            state_store=store1,
        )
        await runtime1.start()
        await runtime1.ask("counter", {})
        await runtime1.ask("counter", {})
        await runtime1.ask("counter", {})
        await runtime1.stop()
        store1.close()

        # Second run — should resume from count=3
        store2 = SQLiteStateStore(db_path)
        runtime2 = Runtime(
            supervisor=Supervisor("root", children=[StatefulCounter("counter")]),
            state_store=store2,
        )
        await runtime2.start()
        try:
            result = await runtime2.ask("counter", {})
            assert result.payload["count"] == 4  # resumed from 3, incremented to 4
        finally:
            await runtime2.stop()
            store2.close()
    finally:
        os.unlink(db_path)


async def test_supervisor_restart_triggers_state_restore():
    """Supervisor restart automatically restores agent state from checkpoint."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = SQLiteStateStore(db_path)

        crash_count = 0

        class CrashThenRecover(AgentProcess):
            async def on_start(self) -> None:
                if "processed" not in self.state:
                    self.state["processed"] = 0

            async def handle(self, message: Message) -> Message | None:
                nonlocal crash_count
                self.state["processed"] += 1
                await self.checkpoint()

                crash_count += 1
                if crash_count == 1:
                    raise RuntimeError("Crash after checkpoint")

                return self.reply({"processed": self.state["processed"]})

        runtime = Runtime(
            supervisor=Supervisor(
                "root",
                children=[CrashThenRecover("worker")],
                max_restarts=3,
                backoff="CONSTANT",
                backoff_base=0.1,
            ),
            state_store=store,
        )
        await runtime.start()
        try:
            # First ask: increments to 1, checkpoints, then crashes
            with pytest.raises(TimeoutError):
                await runtime.ask("worker", {}, timeout=1.0)

            await asyncio.sleep(0.5)

            # After restart, state should be restored (processed=1)
            result = await runtime.ask("worker", {}, timeout=5.0)
            # processed was 1 from checkpoint, then incremented to 2
            assert result.payload["processed"] == 2
        finally:
            await runtime.stop()
            store.close()
    finally:
        os.unlink(db_path)


async def test_stateless_agent_unaffected():
    """Stateless agents (no checkpoint calls) are unaffected by state system."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = SQLiteStateStore(db_path)
        runtime = Runtime(
            supervisor=Supervisor("root", children=[StatelessAgent("echo")]),
            state_store=store,
        )
        await runtime.start()
        try:
            result = await runtime.ask("echo", {"text": "hello"})
            assert result.payload["echo"] == "hello"

            # No state persisted
            saved = await store.get("echo")
            assert saved is None
        finally:
            await runtime.stop()
            store.close()
    finally:
        os.unlink(db_path)


async def test_workflow_resumes_from_checkpoint():
    """Multi-step workflow resumes from last checkpoint after restart."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        # Simulate partial completion by writing state directly
        store1 = SQLiteStateStore(db_path)
        await store1.set("workflow", {"step": 2, "results": ["step_1", "step_2"]})
        store1.close()

        # Start runtime — agent should resume from step 2
        store2 = SQLiteStateStore(db_path)
        runtime = Runtime(
            supervisor=Supervisor("root", children=[MultiStepWorkflow("workflow")]),
            state_store=store2,
        )
        await runtime.start()
        try:
            result = await runtime.ask("workflow", {"total_steps": 5})
            assert result.payload["completed"] is True
            # Should have steps 1-2 from checkpoint + 3-5 from this run
            assert result.payload["results"] == ["step_1", "step_2", "step_3", "step_4", "step_5"]
        finally:
            await runtime.stop()
            store2.close()
    finally:
        os.unlink(db_path)


async def test_checkpoint_with_in_memory_store():
    """Checkpoint works with InMemoryStateStore (no persistence across restarts)."""
    store = InMemoryStateStore()
    runtime = Runtime(
        supervisor=Supervisor("root", children=[StatefulCounter("counter")]),
        state_store=store,
    )
    await runtime.start()
    try:
        await runtime.ask("counter", {})
        await runtime.ask("counter", {})

        saved = await store.get("counter")
        assert saved == {"count": 2}
    finally:
        await runtime.stop()


async def test_sqlite_store_loads_via_plugin_system():
    """SQLiteStateStore can be loaded via the plugin system."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        store = load_plugin("state", "sqlite", {"db_path": db_path})
        assert isinstance(store, SQLiteStateStore)
        await store.set("test", {"v": 1})
        assert await store.get("test") == {"v": 1}
        store.close()
    finally:
        os.unlink(db_path)
