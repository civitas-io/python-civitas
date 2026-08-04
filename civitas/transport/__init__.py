"""Transport protocol — the pluggable boundary for message delivery."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from civitas.serializer import Serializer


class Transport(Protocol):
    """Protocol that all transports implement.

    Five methods. A new transport plugin implements these five methods and the
    entire Civitas runtime works on it.
    """

    async def start(self) -> None:
        """Initialize connections, bind sockets."""
        ...

    async def stop(self) -> None:
        """Gracefully close connections, flush pending messages."""
        ...

    async def subscribe(self, address: str, handler: Callable[[bytes], Awaitable[None]]) -> None:
        """Register a handler for messages arriving at this address."""
        ...

    async def unsubscribe(self, address: str) -> None:
        """Remove the handler for an address. No-op if not subscribed."""
        ...

    async def publish(self, address: str, data: bytes) -> None:
        """Send a message to an address (fire-and-forget)."""
        ...

    async def request(self, address: str, data: bytes, timeout: float | None) -> bytes:
        """Send a message and await a reply (request-reply)."""
        ...

    async def wait_ready(self) -> None:
        """Wait for connections and subscriptions to stabilize. No-op by default.

        Transports with slow-joiner problems (e.g. ZMQ PUB/SUB) should override
        this to sleep or poll until messages will be reliably delivered. Callers
        invoke this after all subscribe() calls are done but before publishing.
        """
        ...

    def has_reply_address(self, address: str) -> bool:
        """Return True if address is an active ephemeral reply endpoint.

        Ephemeral reply addresses are created by transport.request() and are not
        registered agents. The bus uses this to route reply messages without
        going through the Registry.
        """
        ...

    async def wait_subscribed(self, address: str, timeout: float = 2.0) -> None:
        """Block until the subscription for ``address`` is effective for peers.

        Transports where subscription takes effect synchronously (in-process
        dict insert) or broker-side (NATS) implement this as a no-op / flush.
        Transports with asynchronous subscription propagation to publisher
        sockets (ZMQ PUB/SUB) must confirm propagation — announcing a
        freshly-subscribed address before its subscription reaches peer PUB
        sockets makes peers publish into a void (#41).
        """
        ...

    def set_serializer(self, serializer: Serializer) -> None:
        """Replace the serializer this transport uses for its OWN internal
        request()/reply-address bookkeeping (v0.9.2.1 bugfix).

        Every transport that implements ``request()`` holds its own private
        serializer reference, captured at construction, separate from the
        ``MessageBus``'s. ``request()`` needs to inject ``reply_to`` into the
        message it was given — already-serialized bytes from the bus — which
        means deserializing and re-serializing internally. If the bus's
        serializer is swapped later (e.g. ``Runtime.start()`` activating
        message signing) without ALSO swapping the transport's own reference,
        that internal round-trip keeps using the stale one: it calls
        ``Message.from_dict()`` directly on a signed v2 envelope dict
        (``{"v": 2, "msg": {...}, "sig": {...}}``), none of which are real
        ``Message`` fields, silently reconstructing a blank message (found the
        hard way: ``ask()`` over ZMQ with signing enabled just times out, no
        exception, because the resulting blank ``correlation_id`` makes the
        reply-routing check in ``AgentProcess._dispatch()`` silently no-op).
        Call this whenever the bus's serializer changes after construction.
        """
        ...
