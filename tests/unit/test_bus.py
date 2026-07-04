"""Tests for MessageBus routing and validation."""

import pytest

from civitas.bus import MessageBus
from civitas.errors import MessageRoutingError, MessageValidationError
from civitas.messages import Message, _uuid7
from civitas.observability.tracer import Tracer
from civitas.process import AgentProcess
from civitas.registry import LocalRegistry
from civitas.serializer import MsgpackSerializer
from civitas.transport.inprocess import InProcessTransport


class CollectorAgent(AgentProcess):
    """Test agent that collects received messages."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.received: list[Message] = []

    async def handle(self, message: Message) -> Message | None:
        self.received.append(message)
        return self.reply({"ack": True})


@pytest.fixture
def components():
    serializer = MsgpackSerializer()
    transport = InProcessTransport(serializer)
    registry = LocalRegistry()
    tracer = Tracer()
    bus = MessageBus(transport, registry, serializer, tracer)
    return bus, transport, registry, serializer, tracer


async def test_route_delivers_to_agent(components):
    """route() delivers a message to the agent's mailbox via transport."""
    bus, transport, registry, _, _ = components
    await transport.start()

    agent = CollectorAgent("agent_a")
    agent._bus = bus
    registry.register("agent_a")
    await bus.setup_agent(agent)

    msg = Message(
        type="test",
        sender="sender",
        recipient="agent_a",
        payload={"data": 1},
    )
    await bus.route(msg)

    # Message should be in the agent's mailbox
    received = await agent._mailbox.get()
    assert received.type == "test"
    assert received.payload == {"data": 1}

    await transport.stop()


async def test_route_span_closed_on_serialize_error(components):
    """Span is closed even when serializer raises."""
    bus, transport, registry, _, tracer = components
    await transport.start()

    registry.register("agent_a")

    # Spy on tracer to capture the span before it's returned
    captured: list = []
    original_start = tracer.start_send_span

    def spy(message):
        span = original_start(message)
        captured.append(span)
        return span

    tracer.start_send_span = spy
    bus._serializer.serialize = lambda _: (_ for _ in ()).throw(RuntimeError("bad serializer"))

    msg = Message(type="test", sender="s", recipient="agent_a")
    with pytest.raises(RuntimeError, match="bad serializer"):
        await bus.route(msg)

    assert len(captured) == 1
    assert captured[0].end_time is not None  # span was closed despite exception

    await transport.stop()


async def test_route_raises_on_unknown_recipient(components):
    """route() raises MessageRoutingError when recipient is not registered."""
    bus, transport, _, _, _ = components
    await transport.start()

    msg = Message(
        type="test",
        sender="sender",
        recipient="nobody",
        payload={},
    )
    with pytest.raises(MessageRoutingError, match="nobody"):
        await bus.route(msg)

    await transport.stop()


async def test_route_rejects_unknown_system_message(components):
    """route() raises MessageValidationError for unknown _agency.* types."""
    bus, transport, _, _, _ = components
    await transport.start()

    msg = Message(
        type="_agency.unknown_type",
        sender="sender",
        recipient="agent_a",
    )
    with pytest.raises(MessageValidationError, match="Unknown system message"):
        await bus.route(msg)

    await transport.stop()


async def test_route_allows_valid_system_message(components):
    """route() accepts known _agency.* system message types."""
    bus, transport, registry, _, _ = components
    await transport.start()

    agent = CollectorAgent("agent_a")
    agent._bus = bus
    registry.register("agent_a")
    await bus.setup_agent(agent)

    msg = Message(
        type="_agency.shutdown",
        sender="_agency",
        recipient="agent_a",
        priority=1,
    )
    # Should not raise
    await bus.route(msg)

    received = await agent._mailbox.get()
    assert received.type == "_agency.shutdown"

    await transport.stop()


async def test_request_returns_reply(components):
    """request() sends a message and returns the agent's reply."""
    bus, transport, registry, _, _ = components
    await transport.start()

    agent = CollectorAgent("agent_a")
    agent._bus = bus
    registry.register("agent_a")
    await bus.setup_agent(agent)
    await agent._start()  # spawns message loop task, waits until RUNNING

    msg = Message(
        type="ping",
        sender="caller",
        recipient="agent_a",
        payload={"x": 1},
        correlation_id=_uuid7(),  # required: message loop only routes reply when set
    )
    reply = await bus.request(msg, timeout=2.0)
    assert reply.payload["ack"] is True

    await agent._stop()
    await transport.stop()


async def test_route_allows_application_message(components):
    """route() accepts any message type that doesn't start with '_agency.'."""
    bus, transport, registry, _, _ = components
    await transport.start()

    agent = CollectorAgent("agent_a")
    agent._bus = bus
    registry.register("agent_a")
    await bus.setup_agent(agent)

    msg = Message(
        type="custom_type",
        sender="sender",
        recipient="agent_a",
    )
    await bus.route(msg)  # should not raise

    received = await agent._mailbox.get()
    assert received.type == "custom_type"

    await transport.stop()


async def test_teardown_agent_unsubscribes(components):
    """teardown_agent removes the transport handler for the agent."""
    bus, transport, registry, _, _ = components
    await transport.start()

    agent = CollectorAgent("agent_a")
    agent._bus = bus
    registry.register("agent_a")
    await bus.setup_agent(agent)
    assert "agent_a" in transport._handlers

    await bus.teardown_agent("agent_a")
    assert "agent_a" not in transport._handlers

    await transport.stop()


async def test_teardown_agent_error_replies_to_buffered_request(components):
    """A buffered request-reply message gets an error reply on teardown (D9)."""
    bus, transport, registry, _, _ = components
    await transport.start()

    child = CollectorAgent("child")
    caller = CollectorAgent("caller")
    child._bus = bus
    caller._bus = bus
    registry.register("child")
    registry.register("caller")
    await bus.setup_agent(child)
    await bus.setup_agent(caller)

    await child._mailbox.put(
        Message(
            type="ask",
            sender="caller",
            recipient="child",
            reply_to="caller",
            correlation_id="corr-1",
        )
    )

    await bus.teardown_agent("child")

    reply = await caller._mailbox.get()
    assert reply.payload["status"] == "error"
    assert reply.correlation_id == "corr-1"

    await transport.stop()


async def test_teardown_agent_drops_fire_and_forget(components, caplog):
    """A buffered fire-and-forget message is dropped and logged on teardown (D9)."""
    bus, transport, registry, _, _ = components
    await transport.start()

    child = CollectorAgent("child")
    child._bus = bus
    registry.register("child")
    await bus.setup_agent(child)

    await child._mailbox.put(Message(type="fire", sender="x", recipient="child"))

    with caplog.at_level("INFO", logger="civitas.bus"):
        await bus.teardown_agent("child")

    assert any("dropping buffered message" in r.message for r in caplog.records)

    await transport.stop()


async def test_teardown_agent_is_idempotent(components):
    """Repeat teardown and teardown of an unknown name are safe no-ops (D9)."""
    bus, transport, registry, _, _ = components
    await transport.start()

    child = CollectorAgent("child")
    child._bus = bus
    registry.register("child")
    await bus.setup_agent(child)

    await bus.teardown_agent("child")
    await bus.teardown_agent("child")
    await bus.teardown_agent("never-existed")

    await transport.stop()
