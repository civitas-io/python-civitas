from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock

import pytest

from civitas import AgentProcess, Runtime, Supervisor
from civitas.gateway.core import GatewayConfig, HTTPGateway
from civitas.gateway.dispatch import StreamSink, _StreamClosed
from civitas.messages import Message
from civitas.process import _STREAM_CHUNK, _STREAM_END, _STREAM_ERROR


class _Agent(AgentProcess):
    async def handle(self, message: Message) -> None:
        pass


def _wired_agent() -> tuple[_Agent, AsyncMock]:
    agent = _Agent("worker")
    route = AsyncMock()
    agent._bus = type("_Bus", (), {"route": route})()
    agent._metrics = None
    agent._current_message = Message(
        type="req", sender="caller", recipient="worker", correlation_id="cid-1", reply_to="gw"
    )
    return agent, route


# ---------------------------------------------------------------------------
# AgentProcess streaming API — message shape
# ---------------------------------------------------------------------------


class TestAgentStreamingApi:
    @pytest.mark.asyncio
    async def test_emit_routes_chunk_to_caller(self) -> None:
        agent, route = _wired_agent()
        await agent.emit({"token": "hi"})
        sent: Message = route.call_args.args[0]
        assert sent.type == _STREAM_CHUNK
        assert sent.recipient == "gw"
        assert sent.correlation_id == "cid-1"
        assert sent.payload == {"token": "hi"}

    @pytest.mark.asyncio
    async def test_end_stream_sends_terminator(self) -> None:
        agent, route = _wired_agent()
        await agent.end_stream()
        assert route.call_args.args[0].type == _STREAM_END

    @pytest.mark.asyncio
    async def test_stream_reply_auto_terminates(self) -> None:
        agent, route = _wired_agent()
        async with agent.stream_reply() as stream:
            await stream.send({"n": 1})
        types = [c.args[0].type for c in route.call_args_list]
        assert types == [_STREAM_CHUNK, _STREAM_END]

    @pytest.mark.asyncio
    async def test_stream_reply_error_terminator_on_exception(self) -> None:
        agent, route = _wired_agent()
        with pytest.raises(ValueError):
            async with agent.stream_reply():
                raise ValueError("boom")
        last = route.call_args_list[-1].args[0]
        assert last.type == _STREAM_ERROR
        assert last.payload["error"] == "boom"

    @pytest.mark.asyncio
    async def test_emit_outside_handle_raises(self) -> None:
        agent = _Agent("worker")
        agent._current_message = None
        with pytest.raises(RuntimeError, match="outside of handle"):
            await agent.emit({"x": 1})


# ---------------------------------------------------------------------------
# StreamSink
# ---------------------------------------------------------------------------


class TestStreamSink:
    @pytest.mark.asyncio
    async def test_drain_yields_then_ends(self) -> None:
        sink = StreamSink(8)
        sink.push({"a": 1})
        sink.push({"a": 2})
        sink.end()
        got = [c async for c in sink.drain(idle_timeout=1.0, max_duration=5.0)]
        assert got == [{"a": 1}, {"a": 2}]

    @pytest.mark.asyncio
    async def test_overflow_fails_slow_consumer(self) -> None:
        sink = StreamSink(2)
        for i in range(5):
            sink.push({"i": i})
        got = []
        with pytest.raises(_StreamClosed, match="slow_consumer"):
            async for chunk in sink.drain(idle_timeout=1.0, max_duration=5.0):
                got.append(chunk)
        assert got == [{"i": 0}, {"i": 1}]

    @pytest.mark.asyncio
    async def test_fail_raises_reason(self) -> None:
        sink = StreamSink(8)
        sink.fail("boom")
        with pytest.raises(_StreamClosed, match="boom"):
            async for _ in sink.drain(idle_timeout=1.0, max_duration=5.0):
                pass

    @pytest.mark.asyncio
    async def test_idle_timeout(self) -> None:
        sink = StreamSink(8)
        with pytest.raises(_StreamClosed, match="idle timeout"):
            async for _ in sink.drain(idle_timeout=0.05, max_duration=5.0):
                pass


# ---------------------------------------------------------------------------
# HTTPGateway.handle() demux
# ---------------------------------------------------------------------------


class TestGatewayDemux:
    @pytest.mark.asyncio
    async def test_handle_routes_chunks_and_end(self) -> None:
        gw = HTTPGateway("gw")
        sink = gw._open_stream("cid-9")
        await gw.handle(Message(type=_STREAM_CHUNK, correlation_id="cid-9", payload={"a": 1}))
        await gw.handle(Message(type=_STREAM_END, correlation_id="cid-9"))
        got = [c async for c in sink.drain(idle_timeout=1.0, max_duration=5.0)]
        assert got == [{"a": 1}]

    @pytest.mark.asyncio
    async def test_handle_error_terminator(self) -> None:
        gw = HTTPGateway("gw")
        sink = gw._open_stream("cid-e")
        await gw.handle(
            Message(type=_STREAM_ERROR, correlation_id="cid-e", payload={"error": "nope"})
        )
        with pytest.raises(_StreamClosed, match="nope"):
            async for _ in sink.drain(idle_timeout=1.0, max_duration=5.0):
                pass

    @pytest.mark.asyncio
    async def test_handle_unknown_correlation_ignored(self) -> None:
        gw = HTTPGateway("gw")
        await gw.handle(Message(type=_STREAM_CHUNK, correlation_id="ghost", payload={"a": 1}))
        assert "ghost" not in gw._stream_sinks


# ---------------------------------------------------------------------------
# End-to-end: SSE (real runtime + httpx streaming)
# ---------------------------------------------------------------------------


class _SseAgent(AgentProcess):
    async def handle(self, message: Message) -> None:
        async with self.stream_reply() as stream:
            for i in range(3):
                await stream.send({"n": i})


class _WsEchoAgent(AgentProcess):
    async def handle(self, message: Message) -> None:
        if message.type == "ws.close":
            return
        await self.emit({"echo": message.payload.get("text", "")})


class TestStreamingIntegration:
    @pytest.mark.asyncio
    async def test_sse_streams_events(self) -> None:
        import httpx

        config = GatewayConfig(
            port=19090,
            request_timeout=5.0,
            routes=[{"path": "/stream", "agent": "sse", "method": "GET", "mode": "stream"}],
        )
        runtime = Runtime(
            supervisor=Supervisor("root", children=[HTTPGateway("api", config), _SseAgent("sse")])
        )
        await runtime.start()
        await asyncio.sleep(0.2)
        try:
            events: list[dict[str, Any]] = []
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "GET", "http://127.0.0.1:19090/stream", timeout=5.0
                ) as resp:
                    assert resp.status_code == 200
                    assert resp.headers["content-type"].startswith("text/event-stream")
                    async for line in resp.aiter_lines():
                        if line.startswith("data:"):
                            events.append(json.loads(line[5:].strip()))
            assert events == [{"n": 0}, {"n": 1}, {"n": 2}]
        finally:
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_websocket_bidirectional(self) -> None:
        websockets = pytest.importorskip("websockets")

        config = GatewayConfig(port=19091, ws_routes=[{"path": "/ws/echo", "agent": "wsecho"}])
        runtime = Runtime(
            supervisor=Supervisor(
                "root", children=[HTTPGateway("api", config), _WsEchoAgent("wsecho")]
            )
        )
        await runtime.start()
        await asyncio.sleep(0.2)
        try:
            async with websockets.connect("ws://127.0.0.1:19091/ws/echo") as ws:
                await ws.send(json.dumps({"text": "hello"}))
                reply = await asyncio.wait_for(ws.recv(), timeout=5.0)
            assert json.loads(reply) == {"echo": "hello"}
        finally:
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_grpc_server_streaming(self) -> None:
        grpc = pytest.importorskip("grpc")
        from google.protobuf import struct_pb2
        from google.protobuf.json_format import MessageToDict

        from civitas.gateway.proto import civitas_pb2, civitas_pb2_grpc

        config = GatewayConfig(port=19092, grpc_enabled=True, grpc_port=19093)
        runtime = Runtime(
            supervisor=Supervisor("root", children=[HTTPGateway("api", config), _SseAgent("sse")])
        )
        await runtime.start()
        await asyncio.sleep(0.3)
        try:
            async with grpc.aio.insecure_channel("127.0.0.1:19093") as channel:
                stub = civitas_pb2_grpc.AgentStub(channel)
                request = civitas_pb2.AgentRequest(
                    recipient="sse", type="req", payload=struct_pb2.Struct()
                )
                got = [MessageToDict(reply.payload) async for reply in stub.Stream(request)]
            assert got == [{"n": 0.0}, {"n": 1.0}, {"n": 2.0}]
        finally:
            await runtime.stop()
