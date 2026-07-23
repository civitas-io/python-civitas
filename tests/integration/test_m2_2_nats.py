"""M2.2 — NATS Distributed Transport testable criteria.

Tests validate that NATSTransport implements the Transport protocol correctly
and that the same agent code works identically over NATS as over InProcess/ZMQ.

Requires a NATS server running on localhost:4222. Tests are skipped if the
server is unavailable. To run:
    nats-server &
    pytest tests/integration/test_m2_2_nats.py -v
"""

import asyncio
import os
import shutil
import socket
import subprocess
import tempfile
import time

import pytest

from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message, _new_span_id, _uuid7
from civitas.process import ProcessStatus
from civitas.serializer import MsgpackSerializer
from civitas.transport.nats import NATSTransport
from civitas.worker import Worker

# ---------------------------------------------------------------------------
# NATS server fixture — start/stop a local nats-server per test session
# ---------------------------------------------------------------------------


def _find_nats_server() -> str | None:
    """Find nats-server binary on PATH."""
    return shutil.which("nats-server")


@pytest.fixture(scope="session")
def nats_server():
    """Start a nats-server for the test session on a random port."""
    binary = _find_nats_server()
    if binary is None:
        pytest.skip("nats-server not found on PATH")

    # G1 (v0.8.2): genuinely random — the previous hardcoded 14222 (despite
    # this very comment) collided on parallel/simultaneous runs.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as _s:
        _s.bind(("127.0.0.1", 0))
        port = _s.getsockname()[1]
    proc = subprocess.Popen(
        [binary, "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Wait for server to be ready
    time.sleep(0.5)
    if proc.poll() is not None:
        pytest.skip("nats-server failed to start")

    yield f"nats://127.0.0.1:{port}"

    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture
def nats_url(nats_server):
    """Per-test NATS URL."""
    return nats_server


# ---------------------------------------------------------------------------
# Test agents (byte-for-byte identical to Phase 1 / M2.1 agents)
# ---------------------------------------------------------------------------


class Greeter(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"greeting": f"Hello, {message.payload['name']}"})


class Adder(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        a, b = message.payload["a"], message.payload["b"]
        return self.reply({"sum": a + b})


class Forwarder(AgentProcess):
    """Forwards to another agent, proving multi-hop routing works."""

    async def handle(self, message: Message) -> Message | None:
        result = await self.ask(message.payload["target"], {"name": message.payload["name"]})
        return self.reply(result.payload)


# ---------------------------------------------------------------------------
# Transport-level tests
# ---------------------------------------------------------------------------


async def test_nats_transport_connect_disconnect(nats_url):
    """NATS transport connects to server and disconnects cleanly."""
    serializer = MsgpackSerializer()
    transport = NATSTransport(serializer, servers=nats_url)
    await transport.start()
    assert transport._started
    assert transport._nc is not None
    assert transport._nc.is_connected
    await transport.stop()
    assert not transport._started


async def test_nats_transport_publish_subscribe(nats_url):
    """Messages published on one transport arrive at subscribers."""
    serializer = MsgpackSerializer()
    transport = NATSTransport(serializer, servers=nats_url)
    await transport.start()

    received: list[bytes] = []

    async def handler(data: bytes) -> None:
        received.append(data)

    await transport.subscribe("test_addr", handler)

    msg = Message(sender="a", recipient="test_addr", payload={"x": 1})
    data = serializer.serialize(msg)
    await transport.publish("test_addr", data)

    # Wait for delivery
    await asyncio.sleep(0.2)

    assert len(received) == 1
    decoded = serializer.deserialize(received[0])
    assert decoded.payload == {"x": 1}

    await transport.stop()


async def test_nats_transport_request_reply(nats_url):
    """Request-reply works over NATS using temporary reply subscriptions."""
    serializer = MsgpackSerializer()
    transport = NATSTransport(serializer, servers=nats_url)
    await transport.start()

    # Handler that echoes the payload back
    async def echo_handler(data: bytes) -> None:
        msg = serializer.deserialize(data)
        reply = Message(
            sender="responder",
            recipient=msg.reply_to or msg.sender,
            payload=msg.payload,
            correlation_id=msg.correlation_id,
        )
        reply_data = serializer.serialize(reply)
        await transport.publish(reply.recipient, reply_data)

    await transport.subscribe("responder", echo_handler)

    msg = Message(sender="caller", recipient="responder", payload={"echo": "hello"})
    data = serializer.serialize(msg)
    reply_data = await transport.request("responder", data, timeout=5.0)
    reply = serializer.deserialize(reply_data)

    assert reply.payload == {"echo": "hello"}
    await transport.stop()


async def test_nats_two_transports_communicate(nats_url):
    """Two NATSTransport instances sharing a server can exchange messages."""
    serializer = MsgpackSerializer()

    transport_a = NATSTransport(serializer, servers=nats_url)
    await transport_a.start()

    transport_b = NATSTransport(serializer, servers=nats_url)
    await transport_b.start()

    received_by_b: list[bytes] = []

    async def handler_b(data: bytes) -> None:
        received_by_b.append(data)

    await transport_b.subscribe("agent_b", handler_b)

    # Small delay for subscription to propagate
    await asyncio.sleep(0.1)

    # A publishes to B
    msg = Message(sender="agent_a", recipient="agent_b", payload={"from": "A"})
    data = serializer.serialize(msg)
    await transport_a.publish("agent_b", data)

    await asyncio.sleep(0.2)

    assert len(received_by_b) == 1
    decoded = serializer.deserialize(received_by_b[0])
    assert decoded.payload == {"from": "A"}

    await transport_b.stop()
    await transport_a.stop()


async def test_nats_cross_transport_request_reply(nats_url):
    """Request-reply works across two separate NATSTransport instances."""
    serializer = MsgpackSerializer()

    transport_a = NATSTransport(serializer, servers=nats_url)
    await transport_a.start()

    transport_b = NATSTransport(serializer, servers=nats_url)
    await transport_b.start()

    # B has a handler that replies
    async def handler_b(data: bytes) -> None:
        msg = serializer.deserialize(data)
        reply = Message(
            sender="service_b",
            recipient=msg.reply_to or msg.sender,
            payload={"answer": 42},
            correlation_id=msg.correlation_id,
        )
        await transport_b.publish(reply.recipient, serializer.serialize(reply))

    await transport_b.subscribe("service_b", handler_b)
    await asyncio.sleep(0.1)

    # A does request-reply to B
    msg = Message(sender="client_a", recipient="service_b", payload={"question": "?"})
    data = serializer.serialize(msg)
    reply_data = await transport_a.request("service_b", data, timeout=5.0)
    reply = serializer.deserialize(reply_data)

    assert reply.payload == {"answer": 42}

    await transport_b.stop()
    await transport_a.stop()


# ---------------------------------------------------------------------------
# Runtime-level tests — same agent code over NATS
# ---------------------------------------------------------------------------


async def test_runtime_nats_hello_agent(nats_url):
    """Same Greeter agent works identically on NATS transport."""
    runtime = Runtime(
        supervisor=Supervisor("root", children=[Greeter("greeter")]),
        transport="nats",
        nats_servers=nats_url,
    )
    await runtime.start()
    try:
        agent = runtime.get_agent("greeter")
        assert agent is not None
        assert agent.status == ProcessStatus.RUNNING

        result = await runtime.ask("greeter", {"name": "NATS"})
        assert result.payload["greeting"] == "Hello, NATS"
    finally:
        await runtime.stop()


async def test_runtime_nats_multiple_agents(nats_url):
    """Multiple agents communicate over NATS transport."""
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[Greeter("greeter"), Adder("adder"), Forwarder("forwarder")],
        ),
        transport="nats",
        nats_servers=nats_url,
    )
    await runtime.start()
    try:
        # Direct ask to greeter
        r1 = await runtime.ask("greeter", {"name": "Alice"})
        assert r1.payload["greeting"] == "Hello, Alice"

        # Direct ask to adder
        r2 = await runtime.ask("adder", {"a": 3, "b": 4})
        assert r2.payload["sum"] == 7

        # Forwarded message: forwarder → greeter → back
        r3 = await runtime.ask("forwarder", {"target": "greeter", "name": "Bob"})
        assert r3.payload["greeting"] == "Hello, Bob"
    finally:
        await runtime.stop()


async def test_runtime_nats_shutdown_clean(nats_url):
    """NATS runtime shuts down cleanly — all agents STOPPED."""
    runtime = Runtime(
        supervisor=Supervisor("root", children=[Greeter("greeter"), Adder("adder")]),
        transport="nats",
        nats_servers=nats_url,
    )
    await runtime.start()
    greeter = runtime.get_agent("greeter")
    adder = runtime.get_agent("adder")

    await runtime.stop()
    assert greeter.status == ProcessStatus.STOPPED
    assert adder.status == ProcessStatus.STOPPED


async def test_runtime_nats_supervision_restart(nats_url):
    """Supervisor detects crash and restarts agent over NATS transport."""
    crash_count = 0

    class CrashOnce(AgentProcess):
        async def handle(self, message: Message) -> Message | None:
            nonlocal crash_count
            crash_count += 1
            if crash_count == 1:
                raise RuntimeError("Deliberate crash")
            return self.reply({"status": "recovered", "crashes": crash_count})

    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[CrashOnce("crasher")],
            strategy="ONE_FOR_ONE",
            max_restarts=3,
            backoff="CONSTANT",
            backoff_base=0.1,
        ),
        transport="nats",
        nats_servers=nats_url,
    )
    await runtime.start()
    try:
        # First ask triggers crash → supervisor restarts
        with pytest.raises(TimeoutError):
            await runtime.ask("crasher", {"go": True}, timeout=1.0)

        # Wait for supervisor to restart the agent
        await asyncio.sleep(0.5)

        # Second ask should succeed (agent restarted)
        result = await runtime.ask("crasher", {"go": True}, timeout=5.0)
        assert result.payload["status"] == "recovered"
    finally:
        await runtime.stop()


# ---------------------------------------------------------------------------
# Cross-process tests — Worker over NATS
# ---------------------------------------------------------------------------


async def test_worker_nats_hosts_agent(nats_url):
    """Worker hosts an agent that communicates with Runtime over NATS.

    Simulates distributed scenario: Runtime sends messages to an agent
    hosted in a Worker via NATS server.
    """

    class Orchestrator(AgentProcess):
        async def handle(self, message: Message) -> Message | None:
            result = await self.ask("remote_greeter", {"name": message.payload["name"]})
            return self.reply({"forwarded": result.payload["greeting"]})

    runtime = Runtime(
        supervisor=Supervisor("root", children=[Orchestrator("orchestrator")]),
        transport="nats",
        nats_servers=nats_url,
    )
    await runtime.start()

    worker = Worker(
        agents=[Greeter("remote_greeter")],
        transport="nats",
        nats_servers=nats_url,
    )
    await worker.start()

    try:
        result = await runtime.ask("orchestrator", {"name": "Distributed"})
        assert result.payload["forwarded"] == "Hello, Distributed"
    finally:
        await worker.stop()
        await runtime.stop()


async def test_worker_nats_heartbeat_response(nats_url):
    """Agent in Worker responds to heartbeat pings over NATS."""
    runtime = Runtime(
        supervisor=Supervisor("root", children=[]),
        transport="nats",
        nats_servers=nats_url,
    )
    await runtime.start()

    worker = Worker(
        agents=[Greeter("hb_agent")],
        transport="nats",
        nats_servers=nats_url,
    )
    await worker.start()

    try:
        heartbeat = Message(
            type="_agency.heartbeat",
            sender="test_supervisor",
            recipient="hb_agent",
            correlation_id=_uuid7(),
            span_id=_new_span_id(),
        )
        ack = await runtime._bus.request(heartbeat, timeout=5.0)
        assert ack.type == "_agency.heartbeat_ack"
        assert ack.sender == "hb_agent"
    finally:
        await worker.stop()
        await runtime.stop()


async def test_worker_nats_stop_cleans_up(nats_url):
    """Worker stops all agents and disconnects cleanly over NATS."""
    runtime = Runtime(
        supervisor=Supervisor("root", children=[]),
        transport="nats",
        nats_servers=nats_url,
    )
    await runtime.start()

    worker = Worker(
        agents=[Greeter("w_greeter")],
        transport="nats",
        nats_servers=nats_url,
    )
    await worker.start()
    assert worker.started

    await worker.stop()
    assert not worker.started

    await runtime.stop()


# ---------------------------------------------------------------------------
# Config and identity tests
# ---------------------------------------------------------------------------


async def test_runtime_from_config_nats():
    """Runtime.from_config reads NATS transport settings from YAML."""
    yaml_content = """
transport:
  type: nats
  servers: "nats://10.0.0.1:4222"
  jetstream: true
  stream_name: MY_STREAM

supervision:
  name: root
  strategy: ONE_FOR_ONE
  children: []
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        runtime = Runtime.from_config(f.name)

    assert runtime._transport_type == "nats"
    assert runtime._nats_servers == "nats://10.0.0.1:4222"
    assert runtime._nats_jetstream is True
    assert runtime._nats_stream_name == "MY_STREAM"

    os.unlink(f.name)


async def test_agent_code_identical_across_transports(nats_url):
    """Agent code is byte-for-byte identical — same classes, same behavior.

    Validates M2.2 criterion: 'Agent code is byte-for-byte identical to M1.7 and M2.1'.
    """
    # Run on InProcess
    rt_inproc = Runtime(supervisor=Supervisor("root", children=[Greeter("greeter")]))
    await rt_inproc.start()
    r_inproc = await rt_inproc.ask("greeter", {"name": "Test"})
    await rt_inproc.stop()

    # Run on NATS
    rt_nats = Runtime(
        supervisor=Supervisor("root", children=[Greeter("greeter")]),
        transport="nats",
        nats_servers=nats_url,
    )
    await rt_nats.start()
    r_nats = await rt_nats.ask("greeter", {"name": "Test"})
    await rt_nats.stop()

    # Identical behavior
    assert r_inproc.payload == r_nats.payload
