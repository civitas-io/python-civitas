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
from typing import TYPE_CHECKING, Any

import grpc
from google.protobuf import struct_pb2
from google.protobuf.empty_pb2 import Empty
from google.protobuf.json_format import MessageToDict
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from civitas.gateway.dispatch import DispatchStatus
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


def _struct_to_dict(payload: struct_pb2.Struct) -> dict[str, Any]:
    result: dict[str, Any] = MessageToDict(payload)
    return result


def _dict_to_struct(payload: dict[str, Any]) -> struct_pb2.Struct:
    struct = struct_pb2.Struct()
    struct.update(payload)
    return struct


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
    ) -> Any:
        """Server-streaming: advertised in the ``.proto`` but deferred to G3."""
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "Stream is deferred to G3")


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
    ) -> None:
        self._dispatcher = dispatcher
        self._host = host
        self._port = port
        self._reflection_enabled = reflection_enabled
        self._tls_cert = tls_cert
        self._tls_key = tls_key
        self._server: grpc.aio.Server | None = None

    async def start(self) -> None:
        """Build, bind, and start the gRPC server, then mark it healthy."""
        server = grpc.aio.server()
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
        if self._tls_cert and self._tls_key:
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
