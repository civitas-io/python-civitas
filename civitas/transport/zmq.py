"""ZMQTransport — Level 2 multi-process transport using ZeroMQ.

Uses an XSUB/XPUB proxy for PUB/SUB bridging across OS processes.
Request-reply is implemented over PUB/SUB using temporary reply topics,
mirroring the InProcessTransport pattern for consistency.

Architecture:
    ┌──────────┐              ┌──────────┐
    │ Process A│              │ Process B│
    │ PUB  SUB │──┐        ┌──│ PUB  SUB │
    └──────────┘  │        │  └──────────┘
                  ▼        ▲
              ┌──────────────┐
              │  ZMQ Proxy   │
              │ XSUB ↔ XPUB │
              └──────────────┘
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import zmq
import zmq.asyncio

from civitas.messages import _uuid7
from civitas.serializer import Serializer

if TYPE_CHECKING:
    from civitas.security.config import ZmqCurveConfig

logger = logging.getLogger(__name__)

# Null-byte topic separator prevents prefix collisions
# (e.g., subscribing to "foo" won't match "foobar")
_TOPIC_SEP = b"\x00"


class ZMQProxy:
    """Lightweight XSUB/XPUB forwarder that bridges PUB/SUB across processes.

    Runs zmq.proxy() in a background daemon thread. Adds negligible latency
    and can handle millions of messages per second.
    """

    def __init__(
        self,
        frontend: str = "tcp://127.0.0.1:5559",
        backend: str = "tcp://127.0.0.1:5560",
        curve_config: ZmqCurveConfig | None = None,
    ) -> None:
        self._frontend_addr = frontend
        self._backend_addr = backend
        self._curve_config = curve_config
        self._ctx: zmq.Context[bytes] | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()

    def start(self) -> None:
        """Start the proxy in a background daemon thread."""
        self._ctx = zmq.Context()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        ready = self._ready.wait(timeout=5.0)
        if not ready:
            raise RuntimeError(
                f"ZMQProxy failed to start within 5 seconds "
                f"(frontend={self._frontend_addr}, backend={self._backend_addr})"
            )

    def _run(self) -> None:
        if self._ctx is None:
            raise RuntimeError("ZMQ context not initialized")
        xsub = self._ctx.socket(zmq.XSUB)
        xpub = self._ctx.socket(zmq.XPUB)
        if self._curve_config is not None and self._curve_config.enabled:
            cfg = self._curve_config
            xsub.curve_server = True
            xsub.curve_secretkey = cfg.server_secret_key.encode()
            xsub.curve_publickey = cfg.server_public_key.encode()
            xpub.curve_server = True
            xpub.curve_secretkey = cfg.server_secret_key.encode()
            xpub.curve_publickey = cfg.server_public_key.encode()
        xsub.bind(self._frontend_addr)
        xpub.bind(self._backend_addr)
        self._ready.set()
        try:
            zmq.proxy(xsub, xpub)
        except zmq.ContextTerminated:
            pass
        finally:
            xsub.close(linger=0)
            xpub.close(linger=0)

    def stop(self) -> None:
        """Stop the proxy by terminating its context."""
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None


class ZMQTransport:
    """Transport for multi-process deployments using ZeroMQ.

    Implements the five-method Transport protocol. Messages flow through
    an XSUB/XPUB proxy for PUB/SUB delivery. Request-reply uses temporary
    PUB/SUB topics with reply queues, identical to InProcessTransport.

    Parameters:
        serializer: Serializer for message encode/decode.
        pub_addr: Address of the proxy XSUB frontend (PUB connects here).
        sub_addr: Address of the proxy XPUB backend (SUB connects here).
        start_proxy: If True, start a ZMQProxy in this process.
    """

    def __init__(
        self,
        serializer: Serializer,
        pub_addr: str = "tcp://127.0.0.1:5559",
        sub_addr: str = "tcp://127.0.0.1:5560",
        start_proxy: bool = False,
        curve_config: ZmqCurveConfig | None = None,
    ) -> None:
        self._serializer = serializer
        self._pub_addr = pub_addr
        self._sub_addr = sub_addr
        self._start_proxy = start_proxy
        self._curve_config = curve_config

        self._context: zmq.asyncio.Context | None = None
        self._pub: zmq.asyncio.Socket | None = None
        self._sub: zmq.asyncio.Socket | None = None
        self._proxy: ZMQProxy | None = None

        self._handlers: dict[str, Callable[[bytes], Awaitable[None]]] = {}
        self._reply_queues: dict[str, asyncio.Queue[bytes]] = {}
        self._receiver_task: asyncio.Task[None] | None = None
        self._started = False

    async def start(self) -> None:
        """Initialize sockets and connect to the proxy."""
        if self._started:
            return

        if self._start_proxy:
            self._proxy = ZMQProxy(
                frontend=self._pub_addr,
                backend=self._sub_addr,
                curve_config=self._curve_config,
            )
            # Run blocking proxy start in a thread executor to avoid blocking
            # the event loop during the ready-wait.
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._proxy.start)

        self._context = zmq.asyncio.Context()

        # PUB connects to proxy XSUB frontend
        self._pub = self._context.socket(zmq.PUB)
        # SUB connects to proxy XPUB backend
        self._sub = self._context.socket(zmq.SUB)

        if self._curve_config is not None and self._curve_config.enabled:
            cfg = self._curve_config
            for sock in (self._pub, self._sub):
                sock.curve_serverkey = cfg.server_public_key.encode()
                sock.curve_secretkey = cfg.client_secret_key.encode()
                sock.curve_publickey = cfg.client_public_key.encode()

        self._pub.connect(self._pub_addr)
        self._sub.connect(self._sub_addr)

        # Start background receiver
        self._receiver_task = asyncio.create_task(self._receiver_loop())

        self._started = True

    async def wait_ready(self) -> None:
        """Wait for ZMQ connections and subscriptions to stabilize.

        Call after all subscribe() calls are done. Mitigates the ZMQ
        'slow joiner' problem where PUB/SUB needs time for the connection
        handshake and subscription propagation through the proxy.
        """
        await asyncio.sleep(0.3)

    async def stop(self) -> None:
        """Close sockets, stop proxy, clean up."""
        self._started = False

        if self._receiver_task is not None:
            self._receiver_task.cancel()
            try:
                await self._receiver_task
            except asyncio.CancelledError:
                pass

        if self._pub is not None:
            self._pub.close(linger=0)
        if self._sub is not None:
            self._sub.close(linger=0)
        if self._context is not None:
            self._context.term()

        if self._proxy is not None:
            self._proxy.stop()

        self._handlers.clear()
        self._reply_queues.clear()

    async def subscribe(self, address: str, handler: Callable[[bytes], Awaitable[None]]) -> None:
        """Subscribe to messages arriving at this address."""
        if self._sub is None:
            raise RuntimeError("ZMQTransport not started")
        self._handlers[address] = handler
        self._sub.subscribe(address.encode() + _TOPIC_SEP)

    async def unsubscribe(self, address: str) -> None:
        """Remove a handler and best-effort unsubscribe the SUB socket."""
        self._handlers.pop(address, None)
        if self._sub is not None:
            try:
                self._sub.unsubscribe(address.encode() + _TOPIC_SEP)
            except zmq.ZMQError as exc:
                logger.warning("[ZMQTransport] unsubscribe(%r) failed: %s", address, exc)

    async def publish(self, address: str, data: bytes) -> None:
        """Send a message to an address via PUB/SUB through the proxy.

        Same-process reply queues are checked first (short-circuit for
        local request-reply without going through the proxy).
        """
        # Short-circuit for local reply queues
        if address in self._reply_queues:
            await self._reply_queues[address].put(data)
            return

        if self._pub is None:
            raise RuntimeError("ZMQTransport not started")
        topic = address.encode() + _TOPIC_SEP
        await self._pub.send_multipart([topic, data])

    async def request(self, address: str, data: bytes, timeout: float) -> bytes:
        """Send a request and await a reply over PUB/SUB.

        Creates a temporary reply topic, subscribes to it, injects reply_to
        into the message, publishes the request, and awaits the reply.
        """
        reply_address = f"_reply.{_uuid7()}"
        reply_queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)

        # Subscribe to the reply topic
        if self._sub is None:
            raise RuntimeError("ZMQTransport not started")
        self._sub.subscribe(reply_address.encode() + _TOPIC_SEP)
        self._reply_queues[reply_address] = reply_queue

        try:
            # Inject reply_to and re-serialize
            message = self._serializer.deserialize(data)
            message.reply_to = reply_address
            data = self._serializer.serialize(message)

            # Publish the request
            if self._pub is None:
                raise RuntimeError("ZMQTransport not started")
            topic = address.encode() + _TOPIC_SEP
            await self._pub.send_multipart([topic, data])

            # Await the reply
            async with asyncio.timeout(timeout):
                reply_data = await reply_queue.get()
            return reply_data
        finally:
            self._reply_queues.pop(reply_address, None)
            if self._sub is not None:
                self._sub.unsubscribe(reply_address.encode() + _TOPIC_SEP)

    def has_reply_address(self, address: str) -> bool:
        """Return True if address is an active ephemeral reply queue."""
        return address in self._reply_queues

    async def _receiver_loop(self) -> None:
        """Background task: receive from SUB socket and dispatch to handlers."""
        if self._sub is None:
            raise RuntimeError("ZMQTransport not started")
        while True:
            try:
                frames = await self._sub.recv_multipart()
                if len(frames) != 2:
                    continue

                topic_raw, data = frames
                # Strip exactly the topic separator to recover the address
                address = topic_raw.removesuffix(_TOPIC_SEP).decode()

                # Reply queue takes priority (for cross-process request-reply)
                if address in self._reply_queues:
                    await self._reply_queues[address].put(data)
                    continue

                # Dispatch to registered handler
                handler = self._handlers.get(address)
                if handler is not None:
                    await handler(data)

            except asyncio.CancelledError:
                break
            except zmq.ZMQError as exc:
                if self._started:
                    logger.error("[ZMQTransport] receiver loop terminated: %s", exc)
                break
