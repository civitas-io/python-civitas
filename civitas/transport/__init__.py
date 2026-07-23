"""Transport protocol — the pluggable boundary for message delivery."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol


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

    async def request(self, address: str, data: bytes, timeout: float) -> bytes:
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
