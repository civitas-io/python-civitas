"""MessageBus — routes messages between AgentProcesses via the transport layer."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from civitas.audit.types import AuditEvent, AuditSink
from civitas.errors import AgentSuspendedError, MessageRoutingError, MessageValidationError
from civitas.messages import SYSTEM_MESSAGE_TYPES, Message, _new_span_id
from civitas.observability.tracer import Tracer
from civitas.registry import Registry, RoutingEntry
from civitas.serializer import Serializer
from civitas.transport import Transport

if TYPE_CHECKING:
    from civitas.process import AgentProcess

logger = logging.getLogger(__name__)


class MessageBus:
    """Central message router.

    Routes messages from sender to recipient by name, delegates physical
    delivery to the Transport, applies serialization via the Serializer,
    and generates tracing spans for every send/receive.

    Routing precedence in route():
    1. Registry lookup by recipient name → use RoutingEntry.address
    2. Transport ephemeral reply address → publish directly (for request-reply)
    3. Neither → raise MessageRoutingError
    """

    def __init__(
        self,
        transport: Transport,
        registry: Registry,
        serializer: Serializer,
        tracer: Tracer,
        audit_sink: AuditSink | None = None,
    ) -> None:
        self._transport = transport
        self._registry = registry
        self._serializer = serializer
        self._tracer = tracer
        self._audit_sink = audit_sink
        self._local_agents: dict[str, AgentProcess] = {}

    async def setup_agent(self, agent: AgentProcess) -> None:
        """Subscribe the transport to deliver messages to an agent's mailbox."""

        async def _on_message_received(data: bytes) -> None:
            message = self._serializer.deserialize(data)
            span = self._tracer.start_receive_span(message)
            try:
                await agent.receive(message)
            finally:
                span.end()

        self._local_agents[agent.name] = agent
        await self._transport.subscribe(agent.name, _on_message_received)

    async def teardown_agent(self, name: str) -> None:
        """Unsubscribe an agent and fail any messages still bound to it (R1 · D9).

        Called when a dynamically spawned child is terminated so that callers do
        not hang. Idempotent — a repeat call for an already-torn-down agent is a
        no-op. Steps: (a) unsubscribe the transport; (b) drain the child's mailbox,
        answering buffered request-reply messages with an error reply so pending
        ``ask()`` callers fail fast instead of waiting for their timeout, and
        dropping fire-and-forget messages with a log line.
        """
        await self._transport.unsubscribe(name)

        agent = self._local_agents.pop(name, None)
        if agent is None:
            return

        for message in agent._mailbox.drain():
            if message.correlation_id is not None and (message.reply_to or message.sender):
                error_reply = Message(
                    type="reply",
                    sender=name,
                    recipient=message.reply_to or message.sender,
                    payload={"status": "error", "error": f"agent '{name}' was terminated"},
                    correlation_id=message.correlation_id,
                    trace_id=message.trace_id,
                    span_id=_new_span_id(),
                    parent_span_id=message.span_id,
                )
                try:
                    await self.route(error_reply)
                except MessageRoutingError:
                    logger.warning(
                        "teardown_agent(%r): could not deliver error reply for %r",
                        name,
                        message.type,
                    )
            else:
                logger.info("teardown_agent(%r): dropping buffered message %r", name, message.type)

    def _validate_message_type(self, message: Message) -> None:
        """Raise MessageValidationError for unknown _agency.* message types."""
        if message.type.startswith("_agency.") and message.type not in SYSTEM_MESSAGE_TYPES:
            raise MessageValidationError(
                f"Unknown system message type: {message.type}. "
                f"Application messages must not use the '_agency.' prefix."
            )

    def lookup_all(self, pattern: str) -> list[RoutingEntry]:
        """Return all registered agents matching a glob pattern."""
        return self._registry.lookup_all(pattern)

    async def route(self, message: Message) -> None:
        """Route a message to its recipient.

        Validates system message types, creates a send span, serializes the
        message, and publishes through the transport.

        Routing order:
        1. Registry lookup → use RoutingEntry.address
        2. Transport ephemeral reply address (has_reply_address) → publish directly
        3. Neither → raise MessageRoutingError
        """
        self._validate_message_type(message)

        entry = self._registry.lookup(message.recipient)
        if entry is not None:
            address = entry.address
        elif self._transport.has_reply_address(message.recipient):
            # Ephemeral reply endpoint — same-process request-reply short-circuit
            address = message.recipient
        elif message.recipient.startswith("_reply."):
            # Cross-process reply: the runtime's transport owns this ephemeral topic.
            # Route by address directly — ZMQ/NATS delivery handles it.
            address = message.recipient
        else:
            raise MessageRoutingError(f"No agent registered with name: {message.recipient!r}")

        span = self._tracer.start_send_span(message)
        # v0.9.3 (A1): when OTEL is active, start_send_span() may have
        # replaced span.trace_id/span_id with OTEL's own REAL, authoritative
        # IDs (OTEL mints its own; civitas's original ones aren't otherwise
        # honored -- see Tracer._make_span()'s docstring comment). Sync that
        # back onto the outgoing Message before it hits the wire, so the
        # receiving side's handle_span/recv_span parent to a span OTEL
        # actually emitted, not a dangling made-up ID.
        message.trace_id = span.trace_id
        message.span_id = span.span_id
        try:
            data = self._serializer.serialize(message)
            await self._transport.publish(address, data)
        finally:
            span.end()

        if self._audit_sink is not None:
            await self._audit_sink.emit(
                AuditEvent(
                    event="message.route",
                    ts=datetime.now(UTC).isoformat(),
                    agent=message.sender,
                    signer_id=message.sender,  # verified sender == signer when signing is active
                    details={
                        "sender": message.sender,
                        "recipient": message.recipient,
                        "type": message.type,
                        "correlation_id": message.correlation_id or "",
                        "message_id": message.id,
                    },
                )
            )

    async def request(
        self,
        message: Message,
        timeout: float | None = 30.0,
        *,
        fail_if_suspended: bool = False,
    ) -> Message:
        """Send a request message and await a reply.

        Used by ask() — delegates to transport.request() which handles
        correlation and reply routing.

        ``timeout`` (v0.10.0): ``None`` — or any value ``<= 0`` (canonically
        ``-1``) — means **wait indefinitely** (HITL approvals can take
        hours/days; the agent stays SUSPENDED until resumed). Normalized to
        ``None`` here so every transport's ``asyncio.timeout(None)`` waits
        forever uniformly. A positive value is a bounded wait, as before.

        ``fail_if_suspended`` (v0.10.0, D2): when True, raise
        ``AgentSuspendedError`` immediately if the recipient is SUSPENDED,
        instead of buffering the request until resume. Opt-in — the default path
        never consults suspension state (no cost, no behavior change).
        """
        self._validate_message_type(message)
        if timeout is not None and timeout <= 0:
            timeout = None
        if fail_if_suspended and self._registry.is_suspended(message.recipient):
            raise AgentSuspendedError(
                f"agent {message.recipient!r} is suspended; not waiting (fail_if_suspended=True)"
            )

        entry = self._registry.lookup(message.recipient)
        if entry is None:
            raise MessageRoutingError(f"No agent registered with name: {message.recipient!r}")

        span = self._tracer.start_send_span(message)
        # v0.9.3 (A1): see the identical comment in route() above.
        message.trace_id = span.trace_id
        message.span_id = span.span_id
        try:
            data = self._serializer.serialize(message)
            reply_data = await self._transport.request(entry.address, data, timeout)
            return self._serializer.deserialize(reply_data)
        finally:
            span.end()
