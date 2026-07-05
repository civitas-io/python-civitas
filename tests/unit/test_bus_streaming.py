from __future__ import annotations

import asyncio
from contextlib import aclosing
from typing import Any

import pytest

from civitas import AgentProcess, Runtime, Supervisor
from civitas.errors import (
    MessageValidationError,
    SlowConsumerError,
    StreamError,
    StreamInterrupted,
    StreamTimeout,
)
from civitas.messages import STREAM_MESSAGE_TYPES, Message
from civitas.process import _STREAM_CHUNK, _STREAM_END
from civitas.streaming import StreamSink, _StreamClosed


class _Producer(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        n = message.payload.get("n", 3)
        async with self.stream_reply() as stream:
            for i in range(n):
                await stream.send({"i": i})
                await asyncio.sleep(0)
        return None


class _CapProducer(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        async with self.stream_reply(max_frames=3) as stream:
            for i in range(100):
                await stream.send({"i": i})
        return None


class _SlowProducer(AgentProcess):
    log: list[int] = []

    async def handle(self, message: Message) -> Message | None:
        async with self.stream_reply() as stream:
            for i in range(100):
                type(self).log.append(i)
                await stream.send({"i": i})
                await asyncio.sleep(0.005)
        return None


class _PlainProducer(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"answer": 42})


class _Consumer(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        target = message.payload.get("target", "prod")
        break_at = message.payload.get("break_at")
        chunks: list[dict[str, Any]] = []
        async with aclosing(self.stream(target, {"n": message.payload.get("n", 3)})) as stream:
            async for chunk in stream:
                chunks.append(chunk)
                if break_at is not None and len(chunks) >= break_at:
                    break
        return self.reply({"chunks": chunks})


async def _run(*children: AgentProcess) -> Runtime:
    runtime = Runtime(supervisor=Supervisor("root", children=list(children)))
    await runtime.start()
    return runtime


# ---------------------------------------------------------------------------
# Integration: agent-to-agent streaming over the in-process bus
# ---------------------------------------------------------------------------


class TestBusStreaming:
    @pytest.mark.asyncio
    async def test_consume_in_handle_no_deadlock(self) -> None:
        runtime = await _run(_Producer("prod"), _Consumer("cons"))
        try:
            reply = await runtime.ask("cons", {"target": "prod", "n": 3}, timeout=5.0)
            assert reply.payload["chunks"] == [{"i": 0}, {"i": 1}, {"i": 2}]
        finally:
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_graceful_degradation_plain_reply(self) -> None:
        runtime = await _run(_PlainProducer("prod"), _Consumer("cons"))
        try:
            reply = await runtime.ask("cons", {"target": "prod"}, timeout=5.0)
            assert reply.payload["chunks"] == [{"answer": 42}]
        finally:
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_producer_cap(self) -> None:
        runtime = await _run(_CapProducer("prod"), _Consumer("cons"))
        try:
            reply = await runtime.ask("cons", {"target": "prod"}, timeout=5.0)
            assert len(reply.payload["chunks"]) == 3
        finally:
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_cancel_stops_producer(self) -> None:
        _SlowProducer.log = []
        runtime = await _run(_SlowProducer("prod"), _Consumer("cons"))
        try:
            reply = await runtime.ask("cons", {"target": "prod", "break_at": 2}, timeout=5.0)
            assert len(reply.payload["chunks"]) == 2
            await asyncio.sleep(0.2)
            assert len(_SlowProducer.log) < 50
        finally:
            await runtime.stop()


# ---------------------------------------------------------------------------
# StreamSink sequence integrity (R7 · D7)
# ---------------------------------------------------------------------------


class TestSinkSeq:
    @pytest.mark.asyncio
    async def test_seq_happy(self) -> None:
        sink = StreamSink(8)
        sink.push({"i": 0}, seq=0)
        sink.push({"i": 1}, seq=1)
        sink.end(total=2)
        got = [c async for c in sink.drain(idle_timeout=1.0)]
        assert got == [{"i": 0}, {"i": 1}]

    @pytest.mark.asyncio
    async def test_seq_gap(self) -> None:
        sink = StreamSink(8)
        sink.push({"i": 0}, seq=0)
        sink.push({"i": 2}, seq=2)
        with pytest.raises(_StreamClosed, match="out_of_order"):
            async for _ in sink.drain(idle_timeout=1.0):
                pass

    @pytest.mark.asyncio
    async def test_seq_duplicate(self) -> None:
        sink = StreamSink(8)
        sink.push({"i": 0}, seq=0)
        sink.push({"i": 0}, seq=0)
        with pytest.raises(_StreamClosed, match="out_of_order"):
            async for _ in sink.drain(idle_timeout=1.0):
                pass

    @pytest.mark.asyncio
    async def test_truncated(self) -> None:
        sink = StreamSink(8)
        sink.push({"i": 0}, seq=0)
        sink.end(total=5)
        with pytest.raises(_StreamClosed, match="truncated_stream"):
            async for _ in sink.drain(idle_timeout=1.0):
                pass


# ---------------------------------------------------------------------------
# Consumer demux: sender verification, interruption (R7 · D11 / D6)
# ---------------------------------------------------------------------------


class TestConsumerDemux:
    @pytest.mark.asyncio
    async def test_sender_verification_drops_imposter(self) -> None:
        agent = _Consumer("cons")
        sink = StreamSink(8)
        agent._pending_streams["cid"] = sink
        agent._stream_producers["cid"] = "prod"
        assert (
            agent._consume_stream_frame(
                Message(
                    type=_STREAM_CHUNK, sender="evil", correlation_id="cid", payload={"x": 1}, seq=0
                )
            )
            is True
        )
        agent._consume_stream_frame(
            Message(
                type=_STREAM_CHUNK, sender="prod", correlation_id="cid", payload={"y": 2}, seq=0
            )
        )
        agent._consume_stream_frame(
            Message(type=_STREAM_END, sender="prod", correlation_id="cid", seq=1)
        )
        got = [c async for c in sink.drain(idle_timeout=1.0)]
        assert got == [{"y": 2}]

    @pytest.mark.asyncio
    async def test_fail_local_streams_interrupts(self) -> None:
        agent = _Consumer("cons")
        sink = StreamSink(8)
        agent._pending_streams["cid"] = sink
        agent._fail_local_streams("agent_stopped")
        with pytest.raises(_StreamClosed, match="agent_stopped"):
            async for _ in sink.drain(idle_timeout=1.0):
                pass


# ---------------------------------------------------------------------------
# Reserved types, seq envelope, error mapping
# ---------------------------------------------------------------------------


class TestReservedAndEnvelope:
    @pytest.mark.asyncio
    async def test_send_rejects_stream_prefix(self) -> None:
        agent = _Consumer("cons")
        with pytest.raises(MessageValidationError, match="reserved"):
            await agent.send("x", {}, message_type="civitas.stream.chunk")

    @pytest.mark.asyncio
    async def test_ask_rejects_agency_prefix(self) -> None:
        agent = _Consumer("cons")
        with pytest.raises(MessageValidationError, match="reserved"):
            await agent.ask("x", {}, message_type="_agency.shutdown")

    def test_stream_message_types_registry(self) -> None:
        assert "civitas.stream.chunk" in STREAM_MESSAGE_TYPES
        assert "civitas.stream.cancel" in STREAM_MESSAGE_TYPES

    def test_message_seq_roundtrip(self) -> None:
        msg = Message(type=_STREAM_CHUNK, seq=7)
        assert msg.to_dict()["seq"] == 7
        assert Message.from_dict(msg.to_dict()).seq == 7
        assert Message().seq is None

    def test_stream_error_mapping(self) -> None:
        assert isinstance(AgentProcess._stream_error("slow_consumer"), SlowConsumerError)
        assert isinstance(AgentProcess._stream_error("stream idle timeout"), StreamTimeout)
        assert isinstance(AgentProcess._stream_error("agent_stopped"), StreamInterrupted)
        assert isinstance(AgentProcess._stream_error("boom"), StreamError)
