"""M2.1 — ZMQ Multi-Process Transport testable criteria.

Tests validate that ZMQTransport implements the Transport protocol correctly
and that the same agent code works identically over ZMQ as over InProcess.
"""

import asyncio
import os
import tempfile

import pytest

from civitas.registry import LocalRegistry
from civitas.worker import Worker

# F11-5: skip entire module if pyzmq is not installed
pytest.importorskip("zmq", reason="pyzmq not installed — skipping ZMQ transport tests")

from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message, _new_span_id, _uuid7
from civitas.process import ProcessStatus
from civitas.serializer import MsgpackSerializer
from civitas.transport.zmq import ZMQProxy, ZMQTransport

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def zmq_addrs():
    """Unique IPC addresses per test to avoid port conflicts."""
    d = tempfile.mkdtemp()
    frontend = f"ipc://{d}/frontend.sock"
    backend = f"ipc://{d}/backend.sock"
    yield frontend, backend
    for f in os.listdir(d):
        os.unlink(os.path.join(d, f))
    os.rmdir(d)


@pytest.fixture
def zmq_addrs_2():
    """Second set of IPC addresses for tests needing two proxies."""
    d = tempfile.mkdtemp()
    frontend = f"ipc://{d}/frontend.sock"
    backend = f"ipc://{d}/backend.sock"
    yield frontend, backend
    for f in os.listdir(d):
        os.unlink(os.path.join(d, f))
    os.rmdir(d)


# ---------------------------------------------------------------------------
# Test agents (byte-for-byte identical to Phase 1 agents)
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


async def test_zmq_proxy_starts_and_stops(zmq_addrs):
    """ZMQ proxy starts in a background thread and stops cleanly."""
    frontend, backend = zmq_addrs
    proxy = ZMQProxy(frontend=frontend, backend=backend)
    proxy.start()
    assert proxy._thread is not None
    assert proxy._thread.is_alive()
    proxy.stop()
    assert proxy._thread is None or not proxy._thread.is_alive()


async def test_zmq_transport_publish_subscribe(zmq_addrs):
    """Messages published on one transport arrive at subscribers."""
    frontend, backend = zmq_addrs
    serializer = MsgpackSerializer()

    transport = ZMQTransport(serializer, pub_addr=frontend, sub_addr=backend, start_proxy=True)
    await transport.start()

    received: list[bytes] = []

    async def handler(data: bytes) -> None:
        received.append(data)

    await transport.subscribe("test_addr", handler)
    await transport.wait_ready()

    msg = Message(sender="a", recipient="test_addr", payload={"x": 1})
    data = serializer.serialize(msg)
    await transport.publish("test_addr", data)

    # Wait for delivery
    await asyncio.sleep(0.2)

    assert len(received) == 1
    decoded = serializer.deserialize(received[0])
    assert decoded.payload == {"x": 1}

    await transport.stop()


async def test_zmq_transport_request_reply(zmq_addrs):
    """Request-reply works over ZMQ PUB/SUB using temporary reply topics."""
    frontend, backend = zmq_addrs
    serializer = MsgpackSerializer()

    transport = ZMQTransport(serializer, pub_addr=frontend, sub_addr=backend, start_proxy=True)
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
    await transport.wait_ready()

    msg = Message(sender="caller", recipient="responder", payload={"echo": "hello"})
    data = serializer.serialize(msg)
    reply_data = await transport.request("responder", data, timeout=5.0)
    reply = serializer.deserialize(reply_data)

    assert reply.payload == {"echo": "hello"}
    await transport.stop()


async def test_zmq_two_transports_communicate(zmq_addrs):
    """Two ZMQTransport instances sharing a proxy can exchange messages."""
    frontend, backend = zmq_addrs
    serializer = MsgpackSerializer()

    # Transport A starts the proxy
    transport_a = ZMQTransport(serializer, pub_addr=frontend, sub_addr=backend, start_proxy=True)
    await transport_a.start()

    # Transport B connects to the same proxy
    transport_b = ZMQTransport(serializer, pub_addr=frontend, sub_addr=backend, start_proxy=False)
    await transport_b.start()

    received_by_b: list[bytes] = []

    async def handler_b(data: bytes) -> None:
        received_by_b.append(data)

    await transport_b.subscribe("agent_b", handler_b)
    await transport_b.wait_ready()

    # A publishes to B
    msg = Message(sender="agent_a", recipient="agent_b", payload={"from": "A"})
    data = serializer.serialize(msg)
    await transport_a.publish("agent_b", data)

    await asyncio.sleep(0.3)

    assert len(received_by_b) == 1
    decoded = serializer.deserialize(received_by_b[0])
    assert decoded.payload == {"from": "A"}

    await transport_b.stop()
    await transport_a.stop()


async def test_zmq_cross_transport_request_reply(zmq_addrs):
    """Request-reply works across two separate ZMQTransport instances."""
    frontend, backend = zmq_addrs
    serializer = MsgpackSerializer()

    transport_a = ZMQTransport(serializer, pub_addr=frontend, sub_addr=backend, start_proxy=True)
    await transport_a.start()

    transport_b = ZMQTransport(serializer, pub_addr=frontend, sub_addr=backend, start_proxy=False)
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
    await transport_b.wait_ready()

    # A does request-reply to B
    msg = Message(sender="client_a", recipient="service_b", payload={"question": "?"})
    data = serializer.serialize(msg)
    reply_data = await transport_a.request("service_b", data, timeout=5.0)
    reply = serializer.deserialize(reply_data)

    assert reply.payload == {"answer": 42}

    await transport_b.stop()
    await transport_a.stop()


# ---------------------------------------------------------------------------
# Runtime-level tests — same agent code over ZMQ
# ---------------------------------------------------------------------------


async def test_runtime_zmq_hello_agent(zmq_addrs):
    """Same Greeter agent works identically on ZMQ transport."""
    frontend, backend = zmq_addrs
    runtime = Runtime(
        supervisor=Supervisor("root", children=[Greeter("greeter")]),
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await runtime.start()
    try:
        agent = runtime.get_agent("greeter")
        assert agent is not None
        assert agent.status == ProcessStatus.RUNNING

        result = await runtime.ask("greeter", {"name": "ZMQ"})
        assert result.payload["greeting"] == "Hello, ZMQ"
    finally:
        await runtime.stop()


async def test_runtime_zmq_multiple_agents(zmq_addrs):
    """Multiple agents communicate over ZMQ transport."""
    frontend, backend = zmq_addrs
    runtime = Runtime(
        supervisor=Supervisor(
            "root",
            children=[Greeter("greeter"), Adder("adder"), Forwarder("forwarder")],
        ),
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
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


async def test_runtime_zmq_shutdown_clean(zmq_addrs):
    """ZMQ runtime shuts down cleanly — all agents STOPPED."""
    frontend, backend = zmq_addrs
    runtime = Runtime(
        supervisor=Supervisor("root", children=[Greeter("greeter"), Adder("adder")]),
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await runtime.start()
    greeter = runtime.get_agent("greeter")
    adder = runtime.get_agent("adder")

    await runtime.stop()
    assert greeter.status == ProcessStatus.STOPPED
    assert adder.status == ProcessStatus.STOPPED


async def test_runtime_zmq_supervision_restart(zmq_addrs):
    """Supervisor detects crash and restarts agent over ZMQ transport."""
    frontend, backend = zmq_addrs

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
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await runtime.start()
    try:
        # First ask triggers crash → supervisor restarts
        # Need to wait for restart before re-asking
        with pytest.raises(TimeoutError):
            await runtime.ask("crasher", {"go": True}, timeout=1.0)

        # Wait for supervisor to restart the agent
        await asyncio.sleep(0.5)

        # Second ask should succeed (agent restarted)
        result = await runtime.ask("crasher", {"go": True}, timeout=5.0)
        assert result.payload["status"] == "recovered"
    finally:
        await runtime.stop()


async def test_worker_hosts_agent_cross_transport(zmq_addrs):
    """Worker hosts an agent that communicates with the main Runtime over ZMQ.

    Simulates the M2.1 multi-process scenario: Runtime (Process A) sends
    messages to an agent hosted in a Worker (Process B) via ZMQ.
    """
    frontend, backend = zmq_addrs

    # Process A: Runtime with an orchestrator that asks a remote greeter
    class Orchestrator(AgentProcess):
        async def handle(self, message: Message) -> Message | None:
            result = await self.ask("remote_greeter", {"name": message.payload["name"]})
            return self.reply({"forwarded": result.payload["greeting"]})

    runtime = Runtime(
        supervisor=Supervisor("root", children=[Orchestrator("orchestrator")]),
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await runtime.start()

    # Process B: Worker hosting the greeter
    worker = Worker(
        agents=[Greeter("remote_greeter")],
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
    )
    await worker.start()

    try:
        # Runtime asks orchestrator, which asks remote_greeter in the worker
        result = await runtime.ask("orchestrator", {"name": "CrossProcess"})
        assert result.payload["forwarded"] == "Hello, CrossProcess"
    finally:
        await worker.stop()
        await runtime.stop()


async def test_heartbeat_response(zmq_addrs):
    """Agent in Worker responds to heartbeat pings from supervisor."""
    frontend, backend = zmq_addrs

    # Start a worker with an agent
    runtime = Runtime(
        supervisor=Supervisor("root", children=[]),
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await runtime.start()

    worker = Worker(
        agents=[Greeter("hb_agent")],
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
    )
    await worker.start()

    try:
        # Send a heartbeat via the bus and expect an ack
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


async def test_worker_stop_cleans_up(zmq_addrs):
    """Worker stops all agents and disconnects cleanly."""
    frontend, backend = zmq_addrs

    runtime = Runtime(
        supervisor=Supervisor("root", children=[]),
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await runtime.start()

    worker = Worker(
        agents=[Greeter("w_greeter"), Adder("w_adder")],
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
    )
    await worker.start()
    assert worker.started

    # Verify agents respond
    result = await runtime._bus.request(
        Message(
            type="message",
            sender="_runtime",
            recipient="w_greeter",
            payload={"name": "Stop"},
            correlation_id=_uuid7(),
        ),
        timeout=5.0,
    )
    assert result.payload["greeting"] == "Hello, Stop"

    # Stop worker
    await worker.stop()
    assert not worker.started

    await runtime.stop()


async def test_worker_restart_command(zmq_addrs):
    """Worker handles restart commands from supervisor."""
    frontend, backend = zmq_addrs

    call_count = 0

    class CountingAgent(AgentProcess):
        async def on_start(self) -> None:
            nonlocal call_count
            call_count += 1

        async def handle(self, message: Message) -> Message | None:
            return self.reply({"starts": call_count})

    runtime = Runtime(
        supervisor=Supervisor("root", children=[]),
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await runtime.start()

    worker = Worker(
        agents=[CountingAgent("counter")],
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
    )
    await worker.start()

    # Agent started once
    r1 = await runtime._bus.request(
        Message(
            type="message",
            sender="_runtime",
            recipient="counter",
            payload={},
            correlation_id=_uuid7(),
        ),
        timeout=5.0,
    )
    assert r1.payload["starts"] == 1

    # Send restart command
    restart_msg = Message(
        type="_agency.restart",
        sender="test",
        recipient="_agency.worker.restart",
        payload={"agent_name": "counter"},
    )
    await runtime._bus.route(restart_msg)
    await asyncio.sleep(0.5)

    # Agent should have restarted (on_start called again)
    r2 = await runtime._bus.request(
        Message(
            type="message",
            sender="_runtime",
            recipient="counter",
            payload={},
            correlation_id=_uuid7(),
        ),
        timeout=5.0,
    )
    assert r2.payload["starts"] == 2

    await worker.stop()
    await runtime.stop()


async def test_supervisor_remote_child_registration(zmq_addrs):
    """Supervisor can register remote children for heartbeat monitoring."""
    frontend, backend = zmq_addrs
    sup = Supervisor("root", children=[])
    sup.add_remote_child("remote_agent", heartbeat_interval=1.0)
    assert "remote_agent" in sup._remote_children
    assert sup._missed_heartbeats["remote_agent"] == 0


async def test_registry_remote_stub():
    """Registry supports remote agent entries for cross-process lookup."""
    reg = LocalRegistry()
    reg.register_remote("remote_1")
    reg.register_remote("remote_2")

    assert reg.has("remote_1")
    assert reg.has("remote_2")
    result = reg.lookup("remote_1")
    assert result is not None
    assert result.name == "remote_1"
    assert result.is_local is False

    # Pattern matching works with remote entries
    matches = reg.lookup_all("remote_*")
    assert len(matches) == 2


async def test_runtime_from_config_zmq(zmq_addrs):
    """Runtime.from_config reads ZMQ transport settings from YAML."""
    frontend, backend = zmq_addrs
    yaml_content = f"""
transport:
  type: zmq
  pub_addr: "{frontend}"
  sub_addr: "{backend}"
  start_proxy: true

supervision:
  name: root
  strategy: ONE_FOR_ONE
  children: []
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        runtime = Runtime.from_config(f.name)

    assert runtime._transport_type == "zmq"
    assert runtime._zmq_pub_addr == frontend
    assert runtime._zmq_sub_addr == backend
    assert runtime._zmq_start_proxy is True

    os.unlink(f.name)


async def test_agent_code_identical_to_phase1(zmq_addrs):
    """Agent code is byte-for-byte identical — same classes, same behavior.

    Validates M2.1 testable criterion: 'Agent code is byte-for-byte identical to M1.7'.
    """
    frontend, backend = zmq_addrs

    # Run the same agent on InProcess
    rt_inproc = Runtime(supervisor=Supervisor("root", children=[Greeter("greeter")]))
    await rt_inproc.start()
    r_inproc = await rt_inproc.ask("greeter", {"name": "Test"})
    await rt_inproc.stop()

    # Run the same agent on ZMQ
    rt_zmq = Runtime(
        supervisor=Supervisor("root", children=[Greeter("greeter")]),
        transport="zmq",
        zmq_pub_addr=frontend,
        zmq_sub_addr=backend,
        zmq_start_proxy=True,
    )
    await rt_zmq.start()
    r_zmq = await rt_zmq.ask("greeter", {"name": "Test"})
    await rt_zmq.stop()

    # Identical behavior
    assert r_inproc.payload == r_zmq.payload


# ---------------------------------------------------------------------------
# #41 — subscription-propagation races (v0.8.1 V3 regressions)
# ---------------------------------------------------------------------------


async def test_wait_subscribed_barrier_makes_fresh_topic_routable(zmq_addrs):
    """A single publish immediately after wait_subscribed() must be delivered.

    Without the barrier, a freshly-subscribed topic's first messages are
    silently dropped by the sender's PUB socket until the subscription
    propagates (SUB → XPUB → XSUB → peer PUBs; measured 5–25 ms, #41).
    """
    frontend, backend = zmq_addrs
    serializer = MsgpackSerializer()
    a = ZMQTransport(serializer, pub_addr=frontend, sub_addr=backend, start_proxy=True)
    b = ZMQTransport(serializer, pub_addr=frontend, sub_addr=backend)
    await a.start()
    await b.start()
    await a.wait_ready()
    await b.wait_ready()
    try:
        got: list[bytes] = []

        async def handler(data: bytes) -> None:
            got.append(data)

        # Deliberately AFTER wait_ready — a fresh, late subscription.
        await b.subscribe("fresh.topic", handler)
        await b.wait_subscribed("fresh.topic")

        await a.publish("fresh.topic", b"first-and-only")
        for _ in range(100):
            if got:
                break
            await asyncio.sleep(0.01)
        assert got == [b"first-and-only"], "first publish after barrier was dropped"
    finally:
        await b.stop()
        await a.stop()


async def test_request_reply_with_instant_responder(zmq_addrs):
    """An instantly-replying responder must not lose the reply (#41 leg 2).

    Replies ride a stable per-transport prefix subscription made at start()
    (its propagation absorbed by wait_ready), not a per-request subscription
    that races its own first use — a ~1 ms reply used to lose to the ~5–25 ms
    propagation window and vanish inside the replier's PUB socket.
    """
    frontend, backend = zmq_addrs
    serializer = MsgpackSerializer()
    a = ZMQTransport(serializer, pub_addr=frontend, sub_addr=backend, start_proxy=True)
    b = ZMQTransport(serializer, pub_addr=frontend, sub_addr=backend)
    await a.start()
    await b.start()

    async def instant_echo(data: bytes) -> None:
        msg = serializer.deserialize(data)
        reply = Message(
            type="reply",
            sender="echo",
            recipient=msg.reply_to,
            payload={"echoed": True},
            correlation_id=msg.correlation_id,
        )
        await b.publish(msg.reply_to, serializer.serialize(reply))

    await b.subscribe("echo", instant_echo)
    await a.wait_ready()
    await b.wait_ready()
    try:
        request = Message(type="q", sender="a", recipient="echo", payload={}, correlation_id="c1")
        reply_data = await a.request("echo", serializer.serialize(request), timeout=3.0)
        assert serializer.deserialize(reply_data).payload == {"echoed": True}
    finally:
        await b.stop()
        await a.stop()


async def test_wait_subscribed_rejects_unsubscribed_address(zmq_addrs):
    """Calling the barrier for a topic that was never subscribed is a bug."""
    frontend, backend = zmq_addrs
    a = ZMQTransport(MsgpackSerializer(), pub_addr=frontend, sub_addr=backend, start_proxy=True)
    await a.start()
    try:
        with pytest.raises(ValueError):
            await a.wait_subscribed("never.subscribed")
    finally:
        await a.stop()
