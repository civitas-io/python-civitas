"""InProcessTransport — Level 1 single-process transport using asyncio queues."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from civitas.messages import _uuid7
from civitas.serializer import Serializer


class InProcessTransport:
    """Transport for single-process deployments.

    Messages are delivered by putting serialized bytes into the recipient's
    asyncio.Queue. Despite being in-process, messages are still serialized
    through the configured Serializer to ensure transport-swap compatibility.
    """

    def __init__(self, serializer: Serializer, mailbox_size: int = 1000) -> None:
        self._serializer = serializer
        self._mailbox_size = mailbox_size
        self._handlers: dict[str, Callable[[bytes], Awaitable[None]]] = {}
        self._reply_queues: dict[str, asyncio.Queue[bytes]] = {}
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True

    async def wait_ready(self) -> None:
        """No-op for in-process transport — always ready after start()."""

    async def stop(self) -> None:
        self._started = False
        self._handlers.clear()
        self._reply_queues.clear()

    async def subscribe(self, address: str, handler: Callable[[bytes], Awaitable[None]]) -> None:
        self._handlers[address] = handler

    async def unsubscribe(self, address: str) -> None:
        self._handlers.pop(address, None)

    async def publish(self, address: str, data: bytes) -> None:
        # Check if this is a reply to a pending request
        if address in self._reply_queues:
            await self._reply_queues[address].put(data)
            return

        handler = self._handlers.get(address)
        if handler is not None:
            await handler(data)

    async def request(self, address: str, data: bytes, timeout: float) -> bytes:
        """Send a request and await a reply.

        Creates a temporary reply address, injects reply_to into the message,
        publishes the request, and awaits the reply with a timeout.
        """
        reply_address = f"_reply.{_uuid7()}"
        reply_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)
        self._reply_queues[reply_address] = reply_queue

        try:
            # Deserialize to inject reply_to, then re-serialize
            message = self._serializer.deserialize(data)
            message.reply_to = reply_address
            data = self._serializer.serialize(message)

            # Publish the request
            handler = self._handlers.get(address)
            if handler is None:
                raise RuntimeError(f"No handler registered for address: {address}")
            await handler(data)

            # Await the reply
            async with asyncio.timeout(timeout):
                reply_data = await reply_queue.get()
            return reply_data
        finally:
            self._reply_queues.pop(reply_address, None)

    def has_reply_address(self, address: str) -> bool:
        """Return True if address is an active ephemeral reply queue."""
        return address in self._reply_queues
