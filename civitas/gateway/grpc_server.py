"""gRPC surface for the Civitas gateway (design G1).

A ``grpc.aio`` server exposing the generic ``civitas.Agent`` service. Every RPC is
normalized and dispatched onto the message bus through
:class:`~civitas.gateway.dispatch.GatewayDispatcher`, so the gRPC and HTTP surfaces
share one routing/error code path (D3).

``grpc`` is an optional extra. This module imports ``grpc`` and the
reflection/health helpers at module top, but the module itself is only imported by
:meth:`~civitas.gateway.core.HTTPGateway.on_start` when ``grpc_enabled`` is set, so
``import civitas`` never requires ``civitas[grpc]`` (the optional-dependency-gating
exception to the top-level-imports rule).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any

import grpc
from google.protobuf import struct_pb2
from google.protobuf.empty_pb2 import Empty
from google.protobuf.json_format import MessageToDict
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from civitas.config import settings
from civitas.gateway.dispatch import DispatchStatus, _StreamClosed
from civitas.gateway.jwt_auth import JwtVerifier, _InvalidToken
from civitas.gateway.mtls import (
    _check_dn,
    _dn_from_pem,
    _Forbidden,
    _MtlsMisconfigured,
    _NoCertificate,
)
from civitas.gateway.proto import civitas_pb2, civitas_pb2_grpc

if TYPE_CHECKING:
    from civitas.gateway.dispatch import GatewayDispatcher

logger = logging.getLogger(__name__)

# Transport-failure DispatchStatus -> gRPC status code (D6). AGENT_ERROR is
# deliberately absent: an agent that was reached and returned a business error is
# surfaced in-band on AgentReply.error, not as an aborted RPC.
_ABORT_CODES: dict[DispatchStatus, grpc.StatusCode] = {
    DispatchStatus.NOT_FOUND: grpc.StatusCode.NOT_FOUND,
    DispatchStatus.TIMEOUT: grpc.StatusCode.DEADLINE_EXCEEDED,
    DispatchStatus.INTERNAL: grpc.StatusCode.INTERNAL,
}

_AGENT_SERVICE: str = civitas_pb2.DESCRIPTOR.services_by_name["Agent"].full_name
_HEALTH_SERVICE: str = health_pb2.DESCRIPTOR.services_by_name["Health"].full_name

# Services exempt from auth by full service name (F4): health probes and reflection
# clients never carry the bearer token / client cert this interceptor enforces.
_EXEMPT_SERVICES: frozenset[str] = frozenset({_HEALTH_SERVICE, reflection.SERVICE_NAME})

# Stream-failure reason -> gRPC status code. A reason not listed here is treated as
# an agent business error and surfaced in-band as a final AgentReply.error (D6).
_STREAM_ABORT_CODES: dict[str, grpc.StatusCode] = {
    "slow_consumer": grpc.StatusCode.RESOURCE_EXHAUSTED,
    "stream idle timeout": grpc.StatusCode.DEADLINE_EXCEEDED,
    "max_stream_duration exceeded": grpc.StatusCode.DEADLINE_EXCEEDED,
}


def _struct_to_dict(payload: struct_pb2.Struct) -> dict[str, Any]:
    result: dict[str, Any] = MessageToDict(payload)
    return result


def _dict_to_struct(payload: dict[str, Any]) -> struct_pb2.Struct:
    struct = struct_pb2.Struct()
    struct.update(payload)
    return struct


def _bearer_token(metadata: Any) -> str | None:
    """Return the bearer token from the ``authorization`` metadata entry, or ``None``.

    Mirrors the HTTP ``require_jwt`` parsing (case-insensitive ``Bearer`` scheme) so
    both surfaces accept the same credential shape.
    """
    if metadata is None:
        return None
    for key, value in metadata:
        if key.lower() == "authorization":
            scheme, _, raw = value.partition(" ")
            token = raw.strip()
            return token if scheme.lower() == "bearer" and token else None
    return None


def _abort_handler(code: grpc.StatusCode, details: str) -> grpc.RpcMethodHandler:
    """Return an RPC handler that aborts every call with ``code``.

    A gRPC interceptor rejects an RPC by returning a handler that aborts (it cannot
    ``await context.abort`` from ``intercept_service`` itself). A unary-unary aborting
    handler rejects every cardinality — the abort raises before the response shape
    matters.
    """

    async def _abort(request: Any, context: grpc.aio.ServicerContext) -> Any:
        await context.abort(code, details)

    return grpc.unary_unary_rpc_method_handler(_abort)


class _AuthInterceptor(grpc.aio.ServerInterceptor):  # type: ignore[misc]  # grpc ships no type stubs
    """Enforce JWT (metadata) and mTLS (transport) auth on non-exempt RPCs (D3).

    JWT enforcement short-circuits before dispatch by returning an aborting handler
    when the bearer token is missing or invalid. mTLS enforcement wraps the resolved
    handler so the peer certificate's subject DN is checked inside the RPC, where
    ``context.auth_context()`` is reachable. ``Health`` and ``ServerReflection`` are
    exempt from both checks (F4).
    """

    def __init__(
        self,
        jwt_verifier: JwtVerifier | None,
        mtls_enabled: bool,
        allowed_dns: frozenset[str],
    ) -> None:
        self._jwt_verifier = jwt_verifier
        self._mtls_enabled = mtls_enabled
        self._allowed_dns = allowed_dns

    async def intercept_service(
        self,
        continuation: Callable[[Any], Awaitable[Any]],
        handler_call_details: Any,
    ) -> Any:
        service = handler_call_details.method.split("/")[1]
        if service in _EXEMPT_SERVICES:
            return await continuation(handler_call_details)

        if self._jwt_verifier is not None:
            token = _bearer_token(handler_call_details.invocation_metadata)
            if token is None:
                return _abort_handler(grpc.StatusCode.UNAUTHENTICATED, "missing bearer token")
            try:
                await self._jwt_verifier.verify(token)
            except _InvalidToken:
                return _abort_handler(grpc.StatusCode.UNAUTHENTICATED, "invalid token")

        handler = await continuation(handler_call_details)
        if handler is None or not self._mtls_enabled:
            return handler
        return self._wrap_mtls(handler)

    def _wrap_mtls(self, handler: Any) -> Any:
        """Wrap ``handler`` so the peer cert DN is authorized before the RPC runs."""
        allowed = self._allowed_dns

        async def authorize(context: grpc.aio.ServicerContext) -> None:
            certs = context.auth_context().get("x509_pem_cert")
            dn = _dn_from_pem(certs[0]) if certs else None
            try:
                _check_dn(dn, allowed)
            except _Forbidden as exc:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, str(exc))
            except _NoCertificate:
                await context.abort(grpc.StatusCode.UNAUTHENTICATED, "client certificate required")
            except _MtlsMisconfigured:
                await context.abort(grpc.StatusCode.INTERNAL, "server auth is not configured")

        if handler.unary_unary is not None:
            inner_unary = handler.unary_unary

            async def unary_unary(request: Any, context: grpc.aio.ServicerContext) -> Any:
                await authorize(context)
                return await inner_unary(request, context)

            return grpc.unary_unary_rpc_method_handler(
                unary_unary,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        # The Agent service's only non-unary method is server-streaming (Stream); the
        # exempt Health/Reflection services never reach here.
        inner_stream = handler.unary_stream

        async def unary_stream(
            request: Any, context: grpc.aio.ServicerContext
        ) -> AsyncIterator[Any]:
            await authorize(context)
            async for response in inner_stream(request, context):
                yield response

        return grpc.unary_stream_rpc_method_handler(
            unary_stream,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class _AgentServicer(civitas_pb2_grpc.AgentServicer):
    """Generic gRPC servicer proxying any agent by name onto the Civitas bus."""

    def __init__(self, dispatcher: GatewayDispatcher) -> None:
        self._dispatcher = dispatcher

    async def Invoke(
        self, request: civitas_pb2.AgentRequest, context: grpc.aio.ServicerContext
    ) -> civitas_pb2.AgentReply:
        """Unary request-reply: route to the agent via ``call()``."""
        result = await self._dispatcher.dispatch(
            recipient=request.recipient,
            msg_type=request.type or "grpc.request",
            payload=_struct_to_dict(request.payload),
            mode="call",
            correlation_id=request.correlation_id,
            trace_id=request.traceparent,
        )
        if result.status in _ABORT_CODES:
            await context.abort(_ABORT_CODES[result.status], result.error or "error")
        # OK and AGENT_ERROR both carry the agent's payload; AGENT_ERROR also sets
        # `error` in-band because the agent was reached and chose to fail (D6).
        return civitas_pb2.AgentReply(
            payload=_dict_to_struct(result.payload),
            error=result.error or "",
        )

    async def Cast(
        self, request: civitas_pb2.AgentRequest, context: grpc.aio.ServicerContext
    ) -> Empty:
        """Fire-and-forget: route to the agent via ``cast()``."""
        result = await self._dispatcher.dispatch(
            recipient=request.recipient,
            msg_type=request.type or "grpc.request",
            payload=_struct_to_dict(request.payload),
            mode="cast",
            correlation_id=request.correlation_id,
            trace_id=request.traceparent,
        )
        if result.status in _ABORT_CODES:
            await context.abort(_ABORT_CODES[result.status], result.error or "error")
        return Empty()

    async def Stream(
        self, request: civitas_pb2.AgentRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[civitas_pb2.AgentReply]:
        """Server-streaming: one AgentReply per chunk the agent emits (G3)."""
        stream = self._dispatcher.stream(
            recipient=request.recipient,
            msg_type=request.type or "grpc.request",
            payload=_struct_to_dict(request.payload),
            trace_id=request.traceparent,
        )
        try:
            async for chunk in stream:
                yield civitas_pb2.AgentReply(payload=_dict_to_struct(chunk), error="")
        except _StreamClosed as exc:
            reason = str(exc)
            code = _STREAM_ABORT_CODES.get(reason)
            if code is not None:
                await context.abort(code, reason)
            # Agent business error: surface in-band as a final reply, then complete.
            yield civitas_pb2.AgentReply(payload=_dict_to_struct({}), error=reason)


class GrpcServer:
    """Owns the ``grpc.aio`` server lifecycle for :class:`HTTPGateway`.

    Mirrors :class:`~civitas.gateway.h3.H3Server`: it is constructed in
    ``HTTPGateway.on_start`` and driven with :meth:`start` / :meth:`stop`. The same
    :class:`~civitas.gateway.dispatch.GatewayDispatcher` instance backs both the HTTP
    and gRPC surfaces so routing and error semantics stay identical.
    """

    def __init__(
        self,
        dispatcher: GatewayDispatcher,
        host: str,
        port: int,
        *,
        reflection_enabled: bool = True,
        tls_cert: str | None = None,
        tls_key: str | None = None,
        tls_ca_cert: str | None = None,
        client_cert_mode: str = "none",
        jwt_verifier: JwtVerifier | None = None,
    ) -> None:
        self._dispatcher = dispatcher
        self._host = host
        self._port = port
        self._reflection_enabled = reflection_enabled
        self._tls_cert = tls_cert
        self._tls_key = tls_key
        self._tls_ca_cert = tls_ca_cert
        self._client_cert_mode = client_cert_mode
        self._jwt_verifier = jwt_verifier
        self._server: grpc.aio.Server | None = None

    async def start(self) -> None:
        """Build, bind, and start the gRPC server, then mark it healthy."""
        interceptors: list[_AuthInterceptor] = []
        if self._jwt_verifier is not None or self._client_cert_mode != "none":
            interceptors.append(
                _AuthInterceptor(
                    self._jwt_verifier,
                    self._client_cert_mode == "required",
                    settings.gateway_mtls_allowed_dns,
                )
            )
        server = grpc.aio.server(interceptors=interceptors)
        # The generated grpc registration helper is untyped (no stub emitted).
        civitas_pb2_grpc.add_AgentServicer_to_server(  # type: ignore[no-untyped-call]
            _AgentServicer(self._dispatcher), server
        )

        health_servicer = health.aio.HealthServicer()
        health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

        service_names = [_AGENT_SERVICE, _HEALTH_SERVICE]
        if self._reflection_enabled:
            service_names.append(reflection.SERVICE_NAME)
            reflection.enable_server_reflection(service_names, server)

        address = f"{self._host}:{self._port}"
        if self._client_cert_mode == "required":
            credentials = await asyncio.to_thread(self._load_mtls_credentials)
            server.add_secure_port(address, credentials)
        elif self._tls_cert and self._tls_key:
            credentials = await asyncio.to_thread(self._load_credentials)
            server.add_secure_port(address, credentials)
        else:
            server.add_insecure_port(address)

        await server.start()
        # Advertise every registered service (plus the overall "" service) as
        # SERVING so gRPC health probes and load balancers see a ready backend.
        for name in (*service_names, ""):
            await health_servicer.set(name, health_pb2.HealthCheckResponse.SERVING)
        self._server = server

    async def stop(self) -> None:
        """Gracefully drain in-flight RPCs and stop the server."""
        if self._server is not None:
            await self._server.stop(grace=5.0)
            self._server = None

    def _load_credentials(self) -> grpc.ServerCredentials:
        with (
            open(self._tls_key, "rb") as key_file,  # type: ignore[arg-type]  # guarded by caller
            open(self._tls_cert, "rb") as cert_file,  # type: ignore[arg-type]  # guarded by caller
        ):
            return grpc.ssl_server_credentials([(key_file.read(), cert_file.read())])

    def _load_mtls_credentials(self) -> grpc.ServerCredentials:
        # client_cert_mode="required" is only reachable with all three paths set
        # (GatewayConfig.__post_init__), so the None-guard is delegated to the caller.
        with (
            open(self._tls_key, "rb") as key_file,  # type: ignore[arg-type]  # guarded by caller
            open(self._tls_cert, "rb") as cert_file,  # type: ignore[arg-type]  # guarded by caller
            open(self._tls_ca_cert, "rb") as ca_file,  # type: ignore[arg-type]  # guarded by caller
        ):
            return grpc.ssl_server_credentials(
                [(key_file.read(), cert_file.read())],
                root_certificates=ca_file.read(),
                require_client_auth=True,
            )
