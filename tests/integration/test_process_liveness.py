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

from civitas import AgentProcess, Runtime, Supervisor, TopologyServer, Worker  # noqa: E402
from civitas.messages import Message  # noqa: E402
from tests.conftest import wait_for  # noqa: E402


async def _async_http_get_json(host: str, port: int, path: str) -> dict:
    """Async HTTP GET (v0.9.1, D-DASH-3) — NOT urllib.request.urlopen(), which
    is a blocking call that would starve this same event loop the server
    itself needs to run on (client and server share one process/loop in this
    test) — a real deadlock caught while writing this exact test, not a
    theoretical concern. Mirrors test_topology_server.py's _http_get.
    """
    import json

    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(
            f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n".encode()
        )
        await writer.drain()
        raw = await reader.read(65536)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    header_end = raw.find(b"\r\n\r\n")
    body = raw[header_end + 4 :] if header_end != -1 else b""
    return json.loads(body)


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
        assert not root._pending_crash_events
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


async def test_topology_server_processes_endpoint_over_real_zmq(zmq_addrs):
    """v0.9.1 (dashboard-v2, D-DASH-3) end-to-end: /processes reaches a real
    Worker over real ZMQ via the D5 _agency.health_probe wire protocol —
    the same protocol this file's other tests already prove for liveness,
    now carrying process resource stats too. Real HTTP GET against
    TopologyServer's socket, real Worker process (this same OS process, but
    a real independent psutil.Process(os.getpid()) sample).
    """
    frontend, backend = zmq_addrs
    ts = TopologyServer(name="topo", port=16799)
    root = Supervisor("root", children=[ts])
    root.add_remote_child("sleepy", heartbeat_interval=0.1, heartbeat_timeout=0.5)
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
        # Wait for the health channel's OWN registry entry too, not just the
        # agent it hosts — announced as a separate message in the same
        # startup loop (worker.py's announce_names); a real, observed race
        # otherwise (caught by this test on its first run, not assumed away).
        await wait_for(
            lambda: runtime._registry.lookup(worker._health_channel) is not None,
            msg="worker health channel announced",
        )

        data = await _async_http_get_json("127.0.0.1", 16799, "/processes")

        kinds = {p["kind"] for p in data["processes"]}
        assert "runtime" in kinds
        assert "worker" in kinds
        worker_entry = next(p for p in data["processes"] if p["kind"] == "worker")
        assert worker_entry["pid"] > 0
        assert worker_entry["cpu_percent"] >= 0.0
        assert worker_entry["rss_bytes"] > 0
    finally:
        await worker.stop()
        await runtime.stop()
