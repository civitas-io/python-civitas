"""D5 (v0.9.0 E3) — per-process liveness over real ZMQ.

The full-stack proof of the A6 fix: a remote agent blocked in a long handle()
survives a heartbeat regime that would have force-restarted it under the
per-agent scheme, while a genuinely dead remote task is detected fast.
"""

import asyncio
import os
import tempfile

import pytest

pytest.importorskip("zmq", reason="pyzmq not installed — skipping ZMQ liveness tests")

from civitas import AgentProcess, Runtime, Supervisor, Worker  # noqa: E402
from civitas.messages import Message  # noqa: E402
from tests.conftest import wait_for  # noqa: E402


class SleepyAgent(AgentProcess):
    """Blocks in handle() on command — legitimately busy, NOT dead."""

    async def handle(self, message: Message) -> Message | None:
        if message.payload.get("cmd") == "sleep":
            await asyncio.sleep(float(message.payload.get("seconds", 1.0)))
        return None


@pytest.fixture
def zmq_addrs():
    d = tempfile.mkdtemp()
    yield f"ipc://{d}/frontend.sock", f"ipc://{d}/backend.sock"
    for f in os.listdir(d):
        os.unlink(os.path.join(d, f))
    os.rmdir(d)


async def test_busy_remote_agent_survives_tight_heartbeats(zmq_addrs, caplog):
    """A6 end-to-end: 0.1s x 3 heartbeat regime vs. a 1.5s busy handle().
    Pre-v0.9 per-agent pings guaranteed a false crash here; the process-level
    probe answers off-mailbox and reports the agent healthy."""
    frontend, backend = zmq_addrs
    root = Supervisor("root", children=[])
    root.add_remote_child(
        "sleepy", heartbeat_interval=0.1, heartbeat_timeout=0.3, missed_heartbeats_threshold=3
    )
    runtime = Runtime(
        supervisor=root,
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await runtime.start()
    worker = Worker(agents=[SleepyAgent("sleepy")], zmq_pub_addr=frontend, zmq_sub_addr=backend)
    await worker.start()
    try:
        await wait_for(
            lambda: runtime._registry.lookup("sleepy") is not None, msg="sleepy announced"
        )
        entry = runtime._registry.lookup("sleepy")
        assert entry is not None and entry.health_channel == worker._health_channel

        await runtime.send("sleepy", {"cmd": "sleep", "seconds": 1.5})
        await asyncio.sleep(1.2)  # 4x the old false-positive window (interval*threshold=0.3s)

        assert worker._restart_counts.get("sleepy", 0) == 0, (
            "healthy-but-busy remote agent was force-restarted (A6 regression)"
        )
        assert root._crash_queue.qsize() == 0
        # Guard against a vacuous pass: probes must actually be SUCCEEDING —
        # a broken probe path also produces "no crashes" (seen during E3 dev
        # when _agency.health_probe was missing from SYSTEM_MESSAGE_TYPES).
        assert not any("health probe error" in r.getMessage() for r in caplog.records)
    finally:
        await worker.stop()
        await runtime.stop()


async def test_dead_remote_task_restarted_fast(zmq_addrs):
    """Fast remote crash detection: kill the remote agent's task; the next
    probe's snapshot reports it dead and a restart command reaches the worker
    well inside what a starvation cycle would have taken."""
    frontend, backend = zmq_addrs
    root = Supervisor("root", children=[])
    root.add_remote_child(
        "victim", heartbeat_interval=0.1, heartbeat_timeout=0.5, missed_heartbeats_threshold=3
    )
    runtime = Runtime(
        supervisor=root,
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await runtime.start()
    worker = Worker(agents=[SleepyAgent("victim")], zmq_pub_addr=frontend, zmq_sub_addr=backend)
    await worker.start()
    try:
        await wait_for(
            lambda: runtime._registry.lookup("victim") is not None, msg="victim announced"
        )
        # Simulate a hard task death the agent cannot report itself.
        victim = worker._agents["victim"]
        assert victim._task is not None
        victim._task.cancel()
        await asyncio.sleep(0)

        await wait_for(
            lambda: worker._restart_counts.get("victim", 0) >= 1,
            timeout=3.0,
            msg="remote restart triggered by snapshot",
        )
        # And the fresh incarnation is serving again (D1a on the worker side).
        await wait_for(
            lambda: worker._agents["victim"] is not victim, timeout=3.0, msg="fresh incarnation"
        )
    finally:
        await worker.stop()
        await runtime.stop()
