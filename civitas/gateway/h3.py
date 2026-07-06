"""HTTP/3 / QUIC server — serves the same GatewayASGI app over QUIC.

Requires: pip install civitas[http3]

Design:
- H3Server wraps aioquic's QUIC server.
- Each QUIC connection spawns an HttpServerProtocol.
- Each HTTP/3 stream is handled by an _H3RequestHandler which adapts
  the stream to the standard ASGI (scope, receive, send) interface.
- The same GatewayASGI callable is reused — no HTTP/3-specific logic
  in request handling.
- Alt-Svc header injection lives in GatewayASGI._respond(), not here.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from civitas.gateway.asgi import GatewayASGI

logger = logging.getLogger(__name__)


class _H3RequestHandler:
    """Adapts a single HTTP/3 stream to the ASGI (scope, receive, send) interface."""

    def __init__(
        self,
        *,
        connection: Any,
        stream_id: int,
        scope: dict[str, Any],
        transmit: Any,
    ) -> None:
        self._connection = connection
        self._stream_id = stream_id
        self._scope = scope
        self._transmit = transmit
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    def h3_event_received(self, event: Any) -> None:
        from aioquic.h3.events import DataReceived, StreamReset

        if isinstance(event, DataReceived):
            self._queue.put_nowait(
                {
                    "type": "http.request",
                    "body": event.data,
                    "more_body": not event.stream_ended,
                }
            )
        elif isinstance(event, StreamReset):
            self._queue.put_nowait({"type": "http.disconnect"})

    async def receive(self) -> dict[str, Any]:
        return await self._queue.get()

    async def send(self, message: dict[str, Any]) -> None:
        if message["type"] == "http.response.start":
            self._connection.send_headers(
                stream_id=self._stream_id,
                headers=[(b":status", str(message["status"]).encode())]
                + [(k, v) for k, v in message.get("headers", [])],
            )
        elif message["type"] == "http.response.body":
            self._connection.send_data(
                stream_id=self._stream_id,
                data=message.get("body", b""),
                end_stream=not message.get("more_body", False),
            )
        self._transmit()

    async def run(self, app: GatewayASGI) -> None:
        try:
            await app(self._scope, self.receive, self.send)
        except Exception:
            logger.exception("H3 request handler error on stream %d", self._stream_id)


class _HttpServerProtocol:
    """QUIC connection protocol — handles H3 events and spawns request handlers."""

    def __init__(self, quic: Any, event_handler: Any, asgi_app: GatewayASGI) -> None:
        from aioquic.asyncio.protocol import QuicConnectionProtocol

        # Dynamically subclass to inject asgi_app
        self._quic = quic
        self._asgi_app = asgi_app
        self._http: Any = None
        self._handlers: dict[int, _H3RequestHandler] = {}
        self._quic_protocol = QuicConnectionProtocol(quic, event_handler)

    def quic_event_received(self, event: Any) -> None:
        from aioquic.h3.connection import H3Connection
        from aioquic.h3.events import DataReceived, HeadersReceived, StreamReset

        if self._http is None:
            self._http = H3Connection(self._quic, enable_webtransport=False)

        for h3_event in self._http.handle_event(event):
            if isinstance(h3_event, HeadersReceived):
                header_dict = dict(h3_event.headers)
                raw_path = header_dict.get(b":path", b"/").decode()
                path, _, query = raw_path.partition("?")
                scope: dict[str, Any] = {
                    "type": "http",
                    "asgi": {"version": "3.0"},
                    "http_version": "3",
                    "method": header_dict.get(b":method", b"GET").decode().upper(),
                    "path": path,
                    "query_string": query.encode(),
                    "root_path": "",
                    "scheme": header_dict.get(b":scheme", b"https").decode(),
                    "headers": [(k, v) for k, v in h3_event.headers if not k.startswith(b":")],
                    "server": None,
                }
                handler = _H3RequestHandler(
                    connection=self._http,
                    stream_id=h3_event.stream_id,
                    scope=scope,
                    transmit=self._quic_protocol.transmit,
                )
                self._handlers[h3_event.stream_id] = handler
                asyncio.ensure_future(handler.run(self._asgi_app))

            elif isinstance(h3_event, DataReceived | StreamReset):
                existing = self._handlers.get(h3_event.stream_id)
                if existing:
                    existing.h3_event_received(h3_event)


def _make_protocol_factory(asgi_app: GatewayASGI) -> Any:
    """Return a protocol factory compatible with aioquic.asyncio.serve()."""
    from aioquic.asyncio.protocol import QuicConnectionProtocol
    from aioquic.h3.connection import H3Connection
    from aioquic.h3.events import DataReceived, HeadersReceived, StreamReset

    class _Protocol(QuicConnectionProtocol):  # type: ignore[misc]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._http: H3Connection | None = None
            self._handlers: dict[int, _H3RequestHandler] = {}

        def quic_event_received(self, event: Any) -> None:
            if self._http is None:
                self._http = H3Connection(self._quic, enable_webtransport=False)
            for h3_event in self._http.handle_event(event):
                if isinstance(h3_event, HeadersReceived):
                    header_dict = dict(h3_event.headers)
                    raw_path = header_dict.get(b":path", b"/").decode()
                    path, _, query = raw_path.partition("?")
                    scope: dict[str, Any] = {
                        "type": "http",
                        "asgi": {"version": "3.0"},
                        "http_version": "3",
                        "method": header_dict.get(b":method", b"GET").decode().upper(),
                        "path": path,
                        "query_string": query.encode(),
                        "root_path": "",
                        "scheme": header_dict.get(b":scheme", b"https").decode(),
                        "headers": [(k, v) for k, v in h3_event.headers if not k.startswith(b":")],
                        "server": None,
                    }
                    handler = _H3RequestHandler(
                        connection=self._http,
                        stream_id=h3_event.stream_id,
                        scope=scope,
                        transmit=self.transmit,
                    )
                    self._handlers[h3_event.stream_id] = handler
                    asyncio.ensure_future(handler.run(asgi_app))

                elif isinstance(h3_event, DataReceived | StreamReset):
                    existing = self._handlers.get(h3_event.stream_id)
                    if existing:
                        existing.h3_event_received(h3_event)

    return _Protocol


class H3Server:
    """HTTP/3 / QUIC server wrapping aioquic.

    Started and stopped by HTTPGateway.on_start() / on_stop().
    Reuses the same GatewayASGI callable as the HTTP/1.1+2 server.
    """

    def __init__(
        self,
        *,
        asgi_app: GatewayASGI,
        host: str,
        port: int,
        certfile: str,
        keyfile: str,
    ) -> None:
        self._asgi_app = asgi_app
        self._host = host
        self._port = port
        self._certfile = certfile
        self._keyfile = keyfile
        self._server: Any = None

    async def start(self) -> None:
        try:
            from aioquic.asyncio import serve
            from aioquic.h3.connection import H3_ALPN
            from aioquic.quic.configuration import QuicConfiguration
        except ImportError as exc:
            raise RuntimeError(
                "civitas[http3] is required for HTTP/3 support. "
                "Install with: pip install 'civitas[http3]'"
            ) from exc

        configuration = QuicConfiguration(
            alpn_protocols=H3_ALPN,
            is_client=False,
        )
        configuration.load_cert_chain(self._certfile, self._keyfile)

        protocol_factory = _make_protocol_factory(self._asgi_app)
        self._server = await serve(
            self._host,
            self._port,
            configuration=configuration,
            create_protocol=protocol_factory,
        )

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
