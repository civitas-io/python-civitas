from __future__ import annotations

import socket
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest
import pytest_asyncio
from google.protobuf.empty_pb2 import Empty

from civitas.errors import MessageRoutingError
from civitas.gateway.core import GatewayConfig, HTTPGateway
from civitas.gateway.dispatch import GatewayDispatcher
from civitas.gateway.grpc_server import (
    GrpcServer,
    _AgentServicer,
    _dict_to_struct,
    _struct_to_dict,
)
from civitas.gateway.proto import civitas_pb2, civitas_pb2_grpc
from civitas.messages import Message


class _Abort(Exception):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self.code = code
        self.details = details


class _FakeContext:
    async def abort(self, code: grpc.StatusCode, details: str = "") -> Any:
        raise _Abort(code, details)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _servicer_with_gateway() -> tuple[_AgentServicer, MagicMock]:
    gateway = MagicMock(spec=HTTPGateway)
    gateway.name = "api"
    dispatcher = GatewayDispatcher(gateway, request_timeout=5.0)
    return _AgentServicer(dispatcher), gateway


def _request(recipient: str = "echo", payload: dict | None = None) -> civitas_pb2.AgentRequest:
    return civitas_pb2.AgentRequest(
        recipient=recipient,
        type="test",
        payload=_dict_to_struct(payload or {}),
    )


def _reply(payload: dict) -> MagicMock:
    reply = MagicMock(spec=Message)
    reply.payload = payload
    return reply


# ---------------------------------------------------------------------------
# Struct <-> dict conversion
# ---------------------------------------------------------------------------


class TestStructConversion:
    def test_roundtrip_nested(self) -> None:
        original = {"s": "x", "b": True, "nested": {"k": "v"}, "list": ["a", "b"]}
        assert _struct_to_dict(_dict_to_struct(original)) == original

    def test_numbers_become_floats(self) -> None:
        assert _struct_to_dict(_dict_to_struct({"n": 1})) == {"n": 1.0}

    def test_empty(self) -> None:
        assert _struct_to_dict(_dict_to_struct({})) == {}


# ---------------------------------------------------------------------------
# GatewayConfig gRPC fields
# ---------------------------------------------------------------------------


class TestGrpcConfig:
    def test_defaults(self) -> None:
        config = GatewayConfig()
        assert config.grpc_enabled is False
        assert config.grpc_port is None
        assert config.grpc_reflection is True

    def test_enabled_requires_port(self) -> None:
        with pytest.raises(ValueError, match="grpc_enabled requires grpc_port"):
            GatewayConfig(grpc_enabled=True)

    def test_valid(self) -> None:
        config = GatewayConfig(grpc_enabled=True, grpc_port=50051)
        assert config.grpc_port == 50051


# ---------------------------------------------------------------------------
# _AgentServicer — hybrid D6 mapping (real dispatcher, mock gateway)
# ---------------------------------------------------------------------------


class TestServicerInvoke:
    @pytest.mark.asyncio
    async def test_ok_returns_reply(self) -> None:
        servicer, gateway = _servicer_with_gateway()
        gateway.ask = AsyncMock(return_value=_reply({"answer": "42"}))

        reply = await servicer.Invoke(_request(), _FakeContext())

        assert _struct_to_dict(reply.payload) == {"answer": "42"}
        assert reply.error == ""

    @pytest.mark.asyncio
    async def test_agent_error_is_in_band_not_abort(self) -> None:
        servicer, gateway = _servicer_with_gateway()
        gateway.ask = AsyncMock(return_value=_reply({"error": "boom", "detail": "x"}))

        reply = await servicer.Invoke(_request(), _FakeContext())

        # Hybrid (D6): call succeeds, payload preserved, error surfaced in-band.
        assert reply.error == "boom"
        assert _struct_to_dict(reply.payload) == {"error": "boom", "detail": "x"}

    @pytest.mark.asyncio
    async def test_not_found_aborts(self) -> None:
        servicer, gateway = _servicer_with_gateway()
        gateway.ask = AsyncMock(side_effect=MessageRoutingError("no agent"))

        with pytest.raises(_Abort) as exc:
            await servicer.Invoke(_request(), _FakeContext())
        assert exc.value.code == grpc.StatusCode.NOT_FOUND

    @pytest.mark.asyncio
    async def test_timeout_aborts_deadline_exceeded(self) -> None:
        servicer, gateway = _servicer_with_gateway()
        gateway.ask = AsyncMock(side_effect=TimeoutError())

        with pytest.raises(_Abort) as exc:
            await servicer.Invoke(_request(), _FakeContext())
        assert exc.value.code == grpc.StatusCode.DEADLINE_EXCEEDED

    @pytest.mark.asyncio
    async def test_unhandled_error_aborts_internal(self) -> None:
        servicer, gateway = _servicer_with_gateway()
        gateway.ask = AsyncMock(side_effect=RuntimeError("kaboom"))

        with pytest.raises(_Abort) as exc:
            await servicer.Invoke(_request(), _FakeContext())
        assert exc.value.code == grpc.StatusCode.INTERNAL


class TestServicerCast:
    @pytest.mark.asyncio
    async def test_accepted_returns_empty(self) -> None:
        servicer, gateway = _servicer_with_gateway()
        gateway.send = AsyncMock()

        reply = await servicer.Cast(_request(), _FakeContext())

        assert isinstance(reply, Empty)
        gateway.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_not_found_aborts(self) -> None:
        servicer, gateway = _servicer_with_gateway()
        gateway.send = AsyncMock(side_effect=MessageRoutingError("no agent"))

        with pytest.raises(_Abort) as exc:
            await servicer.Cast(_request(), _FakeContext())
        assert exc.value.code == grpc.StatusCode.NOT_FOUND


class TestServicerStream:
    @pytest.mark.asyncio
    async def test_stream_unimplemented(self) -> None:
        servicer, _ = _servicer_with_gateway()

        with pytest.raises(_Abort) as exc:
            await servicer.Stream(_request(), _FakeContext())
        assert exc.value.code == grpc.StatusCode.UNIMPLEMENTED


# ---------------------------------------------------------------------------
# End-to-end: real grpc.aio server + channel
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def grpc_endpoint() -> Any:
    gateway = MagicMock(spec=HTTPGateway)
    gateway.name = "api"
    dispatcher = GatewayDispatcher(gateway, request_timeout=5.0)
    port = _free_port()
    server = GrpcServer(dispatcher, "127.0.0.1", port, reflection_enabled=True)
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield channel, gateway
    finally:
        await channel.close()
        await server.stop()


class TestGrpcEndToEnd:
    @pytest.mark.asyncio
    async def test_invoke_returns_reply(self, grpc_endpoint: Any) -> None:
        channel, gateway = grpc_endpoint
        gateway.ask = AsyncMock(return_value=_reply({"answer": "hi"}))
        stub = civitas_pb2_grpc.AgentStub(channel)

        reply = await stub.Invoke(_request(payload={"q": "yo"}))

        assert _struct_to_dict(reply.payload) == {"answer": "hi"}
        assert reply.error == ""

    @pytest.mark.asyncio
    async def test_invoke_agent_error_in_band(self, grpc_endpoint: Any) -> None:
        channel, gateway = grpc_endpoint
        gateway.ask = AsyncMock(return_value=_reply({"error": "bad input"}))
        stub = civitas_pb2_grpc.AgentStub(channel)

        reply = await stub.Invoke(_request())

        assert reply.error == "bad input"

    @pytest.mark.asyncio
    async def test_invoke_missing_agent_aborts_not_found(self, grpc_endpoint: Any) -> None:
        channel, gateway = grpc_endpoint
        gateway.ask = AsyncMock(side_effect=MessageRoutingError("no agent"))
        stub = civitas_pb2_grpc.AgentStub(channel)

        with pytest.raises(grpc.aio.AioRpcError) as exc:
            await stub.Invoke(_request())
        assert exc.value.code() == grpc.StatusCode.NOT_FOUND

    @pytest.mark.asyncio
    async def test_cast_returns_empty(self, grpc_endpoint: Any) -> None:
        channel, gateway = grpc_endpoint
        gateway.send = AsyncMock()
        stub = civitas_pb2_grpc.AgentStub(channel)

        await stub.Cast(_request())

        gateway.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stream_unimplemented(self, grpc_endpoint: Any) -> None:
        channel, _ = grpc_endpoint
        stub = civitas_pb2_grpc.AgentStub(channel)

        with pytest.raises(grpc.aio.AioRpcError) as exc:
            async for _ in stub.Stream(_request()):
                pass
        assert exc.value.code() == grpc.StatusCode.UNIMPLEMENTED

    @pytest.mark.asyncio
    async def test_health_check_serving(self, grpc_endpoint: Any) -> None:
        channel, _ = grpc_endpoint
        from grpc_health.v1 import health_pb2, health_pb2_grpc

        stub = health_pb2_grpc.HealthStub(channel)
        resp = await stub.Check(health_pb2.HealthCheckRequest())

        assert resp.status == health_pb2.HealthCheckResponse.SERVING
