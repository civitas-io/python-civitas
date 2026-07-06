"""HTTPGateway — supervised ASGI edge process on the Civitas message bus."""

from __future__ import annotations

import asyncio
import logging
import ssl
from dataclasses import dataclass, field
from typing import Any

from civitas.config import settings
from civitas.errors import ConfigurationError
from civitas.gateway.dispatch import GatewayDispatcher, StreamSink
from civitas.gateway.jwt_auth import _JWT_MIDDLEWARE_PATH, JwtVerifier
from civitas.gateway.mtls import _MTLS_MIDDLEWARE_PATH
from civitas.gateway.router import RouteTable
from civitas.messages import Message, _new_span_id
from civitas.process import _STREAM_CHUNK, _STREAM_END, _STREAM_ERROR, AgentProcess

logger = logging.getLogger(__name__)

_CLIENT_CERT_MODES = frozenset({"none", "optional", "required"})
_CERT_REQS = {
    "none": ssl.CERT_NONE,
    "optional": ssl.CERT_OPTIONAL,
    "required": ssl.CERT_REQUIRED,
}
# Auth middleware whose presence flips the docs default to off (M4).
_AUTH_MIDDLEWARE_PATHS = frozenset(
    {
        "civitas.gateway.auth.require_api_key",
        _JWT_MIDDLEWARE_PATH,
        _MTLS_MIDDLEWARE_PATH,
    }
)


@dataclass
class GatewayConfig:
    """Configuration for HTTPGateway.

    All fields have defaults so a gateway can be started with just a port:
        HTTPGateway("api", GatewayConfig(port=8080))
    """

    host: str = "0.0.0.0"
    port: int = 8080
    port_quic: int | None = None
    tls_cert: str | None = None
    tls_key: str | None = None
    tls_ca_cert: str | None = None
    client_cert_mode: str = "none"
    request_timeout: float = 30.0
    enable_http3: bool = False
    grpc_enabled: bool = False
    grpc_port: int | None = None
    grpc_reflection: bool = True
    routes: list[dict[str, Any]] = field(default_factory=list)
    middleware: list[str] = field(default_factory=list)
    # Tri-state: None = auto (on unless gateway auth is configured), True/False = explicit.
    docs_enabled: bool | None = None
    docs_path: str = "/docs"
    ws_routes: list[dict[str, Any]] = field(default_factory=list)
    stream_queue_maxsize: int = 256
    stream_idle_timeout: float = 300.0
    max_stream_duration: float = 3600.0

    def __post_init__(self) -> None:
        if self.enable_http3 and not (self.tls_cert and self.tls_key):
            raise ValueError("enable_http3 requires tls_cert and tls_key")
        if self.enable_http3 and self.port_quic is None:
            raise ValueError("enable_http3 requires port_quic")
        if self.grpc_enabled and self.grpc_port is None:
            raise ValueError("grpc_enabled requires grpc_port")

        if self.client_cert_mode not in _CLIENT_CERT_MODES:
            raise ConfigurationError(
                f"client_cert_mode={self.client_cert_mode!r} is invalid; "
                f"choose from {sorted(_CLIENT_CERT_MODES)}"
            )
        if self.client_cert_mode != "none":
            if not (self.tls_ca_cert and self.tls_cert and self.tls_key):
                raise ConfigurationError(
                    "client_cert_mode requires tls_ca_cert, tls_cert, and tls_key"
                )
            # aioquic hardcodes client certs off, so mTLS over HTTP/3 would silently
            # bypass — refuse the combination rather than serve an unenforced route.
            if self.enable_http3:
                raise ConfigurationError(
                    "client_cert_mode is incompatible with enable_http3 "
                    "(HTTP/3 / aioquic cannot enforce client certificates)"
                )

        if self.client_cert_mode == "optional" and self.grpc_enabled:
            raise ConfigurationError(
                "client_cert_mode='optional' is incompatible with grpc_enabled "
                "(Python's grpc.aio has no CERT_OPTIONAL equivalent — require_client_auth is binary); "
                "use 'required' or 'none' for a gateway with grpc_enabled=True"
            )

        # Once any gateway auth is configured, default docs off unless the operator
        # explicitly opted in — don't expose the API surface behind auth (M4).
        if self.docs_enabled is None:
            self.docs_enabled = not self._auth_configured()

    def _auth_configured(self) -> bool:
        if self.client_cert_mode != "none":
            return True
        return any(mw in _AUTH_MIDDLEWARE_PATHS for mw in self.middleware)


class HTTPGateway(AgentProcess):
    """Supervised HTTP/1.1 + HTTP/2 (+ optional HTTP/3 / QUIC) gateway.

    Translates inbound HTTP requests into Civitas call() / cast() messages
    and returns replies as HTTP responses. Agents behind the gateway never
    see HTTP — they handle Message like any other agent.

    Requires: pip install civitas[http]
    HTTP/3:   pip install civitas[http3]

    Usage::

        gateway = HTTPGateway("api", GatewayConfig(port=8080))
        supervisor = Supervisor("root", children=[gateway, my_agent])
        runtime = Runtime(supervisor=supervisor)
        await runtime.start()
    """

    def __init__(
        self,
        name: str,
        config: GatewayConfig | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, **kwargs)
        self._gw_config = config or GatewayConfig()
        self._route_table = RouteTable.from_config(self._gw_config.routes)
        self._uvicorn_server: Any = None
        self._server_task: asyncio.Task[None] | None = None
        self._h3_server: Any = None
        self._grpc_server: Any = None
        self._stream_sinks: dict[str, StreamSink] = {}
        self._jwt_verifier: JwtVerifier | None = None

    async def on_start(self) -> None:
        try:
            import uvicorn
        except ImportError as exc:
            raise RuntimeError(
                "civitas[http] is required for HTTPGateway. "
                "Install with: pip install 'civitas[http]'"
            ) from exc

        from civitas.gateway.asgi import GatewayASGI

        # Shared by every transport so HTTP and gRPC route identically (D3).
        dispatcher = GatewayDispatcher(
            self,
            self._gw_config.request_timeout,
            stream_idle_timeout=self._gw_config.stream_idle_timeout,
            stream_max_duration=self._gw_config.max_stream_duration,
        )

        # Constructing the ASGI app resolves the middleware chain eagerly; a bad
        # (e.g. security) middleware raises here and crashes startup (M1).
        asgi_app = GatewayASGI(
            gateway=self,
            route_table=self._route_table,
            config=self._gw_config,
            dispatcher=dispatcher,
        )

        # Build the JWT verifier once, eagerly, when require_jwt is in the chain, so
        # a misconfig or missing PyJWT fails startup instead of the first request.
        if _JWT_MIDDLEWARE_PATH in self._configured_middleware_paths():
            self._jwt_verifier = JwtVerifier.from_settings(settings)

        # D10: JWT auto-inherits onto gRPC (D6), but a bearer token over an insecure
        # (plaintext) gRPC port ships the credential in the clear — refuse to start.
        if self._gw_config.grpc_enabled and self._jwt_verifier is not None:
            if not (self._gw_config.tls_cert and self._gw_config.tls_key):
                raise ConfigurationError(
                    "JWT auth cannot be enforced over an insecure (plaintext) gRPC port — the bearer "
                    "token would be sent in cleartext metadata; configure tls_cert/tls_key for the "
                    "gRPC surface, or disable JWT for this gateway."
                )
        # D11: mTLS-only auth doesn't extend to WS (WS mTLS pending #25); make the
        # resulting silently-open WS surface loud instead of silent.
        if (
            self._gw_config.client_cert_mode != "none"
            and self._jwt_verifier is None
            and self._gw_config.ws_routes
        ):
            logger.warning(
                "client_cert_mode is set but no JWT verifier is configured; WS routes %r will be "
                "served with NO authentication (WS mTLS is not yet supported — see #25/#17)",
                [r["path"] for r in self._gw_config.ws_routes],
            )

        uv_config = uvicorn.Config(
            app=asgi_app,
            host=self._gw_config.host,
            port=self._gw_config.port,
            log_level="warning",
            ssl_certfile=self._gw_config.tls_cert,
            ssl_keyfile=self._gw_config.tls_key,
            ssl_ca_certs=self._gw_config.tls_ca_cert,
            ssl_cert_reqs=_CERT_REQS[self._gw_config.client_cert_mode],
        )
        self._uvicorn_server = uvicorn.Server(uv_config)
        self._server_task = asyncio.create_task(
            self._uvicorn_server.serve(), name=f"gateway-{self.name}"
        )
        logger.info(
            "HTTPGateway '%s' listening on %s:%d",
            self.name,
            self._gw_config.host,
            self._gw_config.port,
        )

        if self._gw_config.enable_http3:
            from civitas.gateway.h3 import H3Server

            if self._gw_config.port_quic is None:
                raise ConfigurationError(
                    "HTTPGateway: 'port_quic' is required when enable_http3=True"
                )
            if self._gw_config.tls_cert is None:
                raise ConfigurationError(
                    "HTTPGateway: 'tls_cert' is required when enable_http3=True"
                )
            if self._gw_config.tls_key is None:
                raise ConfigurationError(
                    "HTTPGateway: 'tls_key' is required when enable_http3=True"
                )
            self._h3_server = H3Server(
                asgi_app=asgi_app,
                host=self._gw_config.host,
                port=self._gw_config.port_quic,
                certfile=self._gw_config.tls_cert,
                keyfile=self._gw_config.tls_key,
            )
            await self._h3_server.start()
            logger.info(
                "HTTPGateway '%s' HTTP/3 / QUIC on UDP %s:%d",
                self.name,
                self._gw_config.host,
                self._gw_config.port_quic,
            )

        if self._gw_config.grpc_enabled:
            if self._gw_config.grpc_port is None:
                raise ConfigurationError(
                    "HTTPGateway: 'grpc_port' is required when grpc_enabled=True"
                )
            try:
                from civitas.gateway.grpc_server import GrpcServer
            except ImportError as exc:
                raise RuntimeError(
                    "civitas[grpc] is required for the gRPC surface. "
                    "Install with: pip install 'civitas[grpc]'"
                ) from exc

            self._grpc_server = GrpcServer(
                dispatcher,
                host=self._gw_config.host,
                port=self._gw_config.grpc_port,
                reflection_enabled=self._gw_config.grpc_reflection,
                tls_cert=self._gw_config.tls_cert,
                tls_key=self._gw_config.tls_key,
                tls_ca_cert=self._gw_config.tls_ca_cert,
                client_cert_mode=self._gw_config.client_cert_mode,
                jwt_verifier=self._jwt_verifier,
            )
            await self._grpc_server.start()
            logger.info(
                "HTTPGateway '%s' gRPC on %s:%d",
                self.name,
                self._gw_config.host,
                self._gw_config.grpc_port,
            )

    def _configured_middleware_paths(self) -> list[str]:
        """All middleware dotted paths in effect (global + every route)."""
        paths = list(self._gw_config.middleware)
        for entry in self._route_table.entries():
            paths.extend(entry.middleware)
        return paths

    async def on_stop(self) -> None:
        if self._grpc_server is not None:
            await self._grpc_server.stop()
            self._grpc_server = None

        if self._h3_server is not None:
            await self._h3_server.stop()
            self._h3_server = None

        if self._uvicorn_server is not None:
            self._uvicorn_server.should_exit = True
            self._uvicorn_server = None

        if self._server_task is not None:
            try:
                await asyncio.wait_for(self._server_task, timeout=5.0)
            except TimeoutError:
                self._server_task.cancel()
                try:
                    await self._server_task
                except asyncio.CancelledError:
                    pass
            self._server_task = None

        logger.info("HTTPGateway '%s' stopped", self.name)

    def _open_stream(self, correlation_id: str) -> StreamSink:
        sink = StreamSink(self._gw_config.stream_queue_maxsize)
        self._stream_sinks[correlation_id] = sink
        return sink

    def _close_stream(self, correlation_id: str) -> None:
        self._stream_sinks.pop(correlation_id, None)

    async def _send_stream_request(
        self,
        *,
        recipient: str,
        payload: dict[str, Any],
        correlation_id: str,
        msg_type: str,
        trace_id: str = "",
    ) -> None:
        # reply_to points back at the gateway so the agent's streamed chunks land
        # in our own mailbox and get demultiplexed by handle().
        if self._bus is None:
            raise RuntimeError("HTTPGateway not wired to a MessageBus")
        request = Message(
            type=msg_type,
            sender=self.name,
            recipient=recipient,
            payload=payload,
            correlation_id=correlation_id,
            reply_to=self.name,
            trace_id=trace_id,
            span_id=_new_span_id(),
        )
        await self._bus.route(request)

    async def handle(self, message: Message) -> None:
        """Demultiplex agents' streamed chunks into their per-request sinks.

        The gateway takes no inbound business traffic; the only messages routed to
        it are the streaming chunks/terminators an agent emits in reply to a stream
        request, matched back to the waiting sink by correlation_id.
        """
        sink = self._stream_sinks.get(message.correlation_id or "")
        if sink is None:
            return
        if message.type == _STREAM_CHUNK:
            sink.push(message.payload)
        elif message.type == _STREAM_END:
            sink.end()
        elif message.type == _STREAM_ERROR:
            sink.fail(str(message.payload.get("error") or "stream error"))
