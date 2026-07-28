"""NATSTransport — Level 2 distributed transport using NATS.

Uses NATS pub/sub for fire-and-forget delivery and temporary reply
subscriptions for request-reply (mirroring the InProcess/ZMQ pattern).

Architecture:
    ┌──────────┐              ┌──────────┐
    │ Machine A│              │ Machine B│
    │  Agent1  │──┐        ┌──│  Agent2  │
    └──────────┘  │        │  └──────────┘
                  ▼        ▲
              ┌──────────────┐
              │  NATS Server │
              └──────────────┘

NATS subject mapping:
    Agent address "foo" → NATS subject "civitas.agent.foo"
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import nats
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg

from civitas.messages import _uuid7
from civitas.serializer import Serializer

if TYPE_CHECKING:
    from civitas.security.config import NatsTlsConfig

logger = logging.getLogger(__name__)

# Subject prefix to namespace all Civitas messages
_SUBJECT_PREFIX = "civitas.agent."


class NATSTransport:
    """Transport for distributed deployments using NATS.

    Implements the five-method Transport protocol. Messages flow through
    a NATS server. Request-reply uses temporary subscriptions with reply
    addresses, consistent with InProcess and ZMQ transports.

    Parameters:
        serializer: Serializer for message encode/decode.
        servers: NATS server URL(s) to connect to.
        jetstream: If True, use JetStream durable subscriptions.
        stream_name: JetStream stream name (only used if jetstream=True).
    """

    def __init__(
        self,
        serializer: Serializer,
        servers: str | list[str] = "nats://localhost:4222",
        jetstream: bool = False,
        stream_name: str = "AGENCY",
        create_stream_if_missing: bool = True,
        tls_config: NatsTlsConfig | None = None,
    ) -> None:
        self._serializer = serializer
        self._servers = servers if isinstance(servers, list) else [servers]
        self._use_jetstream = jetstream
        self._stream_name = stream_name
        self._create_stream_if_missing = create_stream_if_missing
        self._tls_config = tls_config

        self._nc: NATSClient | None = None
        self._js: nats.js.JetStreamContext | None = None
        self._handlers: dict[str, Callable[[bytes], Awaitable[None]]] = {}
        self._subscriptions: dict[str, Any] = {}  # Subscription or PushSubscription
        self._reply_queues: dict[str, asyncio.Queue[bytes]] = {}
        self._started = False

    def _to_subject(self, address: str) -> str:
        """Map an agent address to a NATS subject."""
        return f"{_SUBJECT_PREFIX}{address}"

    async def start(self) -> None:
        """Connect to the NATS server."""
        if self._started:
            return

        async def _on_disconnected() -> None:
            logger.warning("[NATSTransport] disconnected from server")

        async def _on_reconnected() -> None:
            logger.info("[NATSTransport] reconnected to server")

        async def _on_error(exc: Exception) -> None:
            logger.error("[NATSTransport] error: %s", exc)

        connect_kwargs: dict[str, Any] = {
            "servers": self._servers,
            "disconnected_cb": _on_disconnected,
            "reconnected_cb": _on_reconnected,
            "error_cb": _on_error,
        }

        if self._tls_config is not None and self._tls_config.enabled:
            connect_kwargs["tls"] = self._tls_config.build_ssl_context()

        if self._tls_config is not None and self._tls_config.nkey_seed:
            try:
                import nkeys
            except ImportError as exc:
                raise ImportError(
                    "NATS nkeys auth requires 'nkeys'. "
                    "Install it with: pip install 'civitas[nkeys]'"
                ) from exc
            connect_kwargs["nkeys_seed"] = nkeys.from_seed(self._tls_config.nkey_seed.encode())

        self._nc = await nats.connect(**connect_kwargs)

        if self._use_jetstream:
            self._js = self._nc.jetstream()
            try:
                await self._js.find_stream_name_by_subject(f"{_SUBJECT_PREFIX}>")
            except nats.js.errors.NotFoundError as exc:
                if not self._create_stream_if_missing:
                    raise RuntimeError(
                        f"JetStream stream '{self._stream_name}' not found and "
                        f"create_stream_if_missing=False"
                    ) from exc
                logger.warning(
                    "[NATSTransport] auto-creating JetStream stream '%s' — "
                    "set create_stream_if_missing=False in production",
                    self._stream_name,
                )
                await self._js.add_stream(
                    name=self._stream_name,
                    subjects=[f"{_SUBJECT_PREFIX}>"],
                )

        self._started = True

    async def wait_ready(self) -> None:
        """No-op for NATS — connection is established synchronously in start()."""

    async def stop(self) -> None:
        """Disconnect from NATS, clean up subscriptions."""
        self._started = False

        for sub in self._subscriptions.values():
            try:
                await sub.unsubscribe()
            except Exception:  # noqa: BLE001 — best-effort cleanup during shutdown
                continue
        self._subscriptions.clear()

        if self._nc is not None and self._nc.is_connected:
            await self._nc.drain()
            self._nc = None

        self._handlers.clear()
        self._reply_queues.clear()

    async def subscribe(self, address: str, handler: Callable[[bytes], Awaitable[None]]) -> None:
        """Subscribe to messages arriving at this address."""
        if self._nc is None:
            raise RuntimeError("NATSTransport not started")
        self._handlers[address] = handler
        subject = self._to_subject(address)

        async def _on_msg(msg: Msg) -> None:
            # Reply queues take priority (for request-reply responses)
            if address in self._reply_queues:
                await self._reply_queues[address].put(msg.data)
                return
            h = self._handlers.get(address)
            if h is not None:
                await h(msg.data)

        sub: Any
        if self._use_jetstream and self._js is not None:
            sub = await self._js.subscribe(
                subject,
                durable=address.replace(".", "_").replace("-", "_"),
                cb=_on_msg,
            )
        else:
            sub = await self._nc.subscribe(subject, cb=_on_msg)

        self._subscriptions[address] = sub

    async def unsubscribe(self, address: str) -> None:
        """Remove a handler and best-effort unsubscribe the NATS subscription."""
        self._handlers.pop(address, None)
        sub = self._subscriptions.pop(address, None)
        if sub is not None:
            try:
                await sub.unsubscribe()
            except Exception as exc:  # noqa: BLE001 — best-effort cleanup
                logger.warning("[NATSTransport] unsubscribe(%r) failed: %s", address, exc)

    async def publish(self, address: str, data: bytes) -> None:
        """Publish a message to an address (fire-and-forget).

        Checks local reply queues first (same-process request-reply
        short-circuit), then publishes via NATS.
        """
        # Short-circuit for local reply queues
        if address in self._reply_queues:
            await self._reply_queues[address].put(data)
            return

        if self._nc is None:
            raise RuntimeError("NATSTransport not started")
        subject = self._to_subject(address)
        await self._nc.publish(subject, data)

    async def request(self, address: str, data: bytes, timeout: float) -> bytes:
        """Send a request and await a reply.

        Creates a temporary reply address, subscribes to it, injects reply_to
        into the message, publishes the request, and awaits the reply.
        Mirrors the InProcess/ZMQ pattern for consistency.
        """
        if self._nc is None:
            raise RuntimeError("NATSTransport not started")

        reply_address = f"_reply.{_uuid7()}"
        reply_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)
        reply_subject = self._to_subject(reply_address)

        # Subscribe to the reply subject
        async def _on_reply(msg: Msg) -> None:
            if reply_address in self._reply_queues:
                await self._reply_queues[reply_address].put(msg.data)

        sub = await self._nc.subscribe(reply_subject, cb=_on_reply)
        self._reply_queues[reply_address] = reply_queue

        try:
            # Inject reply_to and re-serialize
            message = self._serializer.deserialize(data)
            message.reply_to = reply_address
            data = self._serializer.serialize(message)

            # Publish the request
            subject = self._to_subject(address)
            await self._nc.publish(subject, data)
            await self._nc.flush()

            # Await the reply
            async with asyncio.timeout(timeout):
                reply_data = await reply_queue.get()
            return reply_data
        finally:
            self._reply_queues.pop(reply_address, None)
            await sub.unsubscribe()

    def has_reply_address(self, address: str) -> bool:
        """Return True if address is an active ephemeral reply queue."""
        return address in self._reply_queues

    def set_serializer(self, serializer: Serializer) -> None:
        """Replace the serializer used by request()'s internal reply_to
        round-trip (v0.9.2.1 bugfix — see Transport.set_serializer's
        docstring for the full story: this transport's own serializer
        reference was never updated when Runtime.start() activated message
        signing, silently corrupting every ask() into a blank message).
        """
        self._serializer = serializer

    async def wait_subscribed(self, address: str, timeout: float = 2.0) -> None:
        """Flush the connection — the broker owns routing, so once the server has
        processed the SUB (flush round-trips it), peers' publishes are routed."""
        if self._nc is not None and self._nc.is_connected:
            await self._nc.flush(timeout=max(1, int(timeout)))
