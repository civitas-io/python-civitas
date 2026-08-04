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
from civitas.process import _STREAM_CHUNK, _STREAM_END, _STREAM_ERROR
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


class _LeakyProducer(AgentProcess):
    """Opens a stream (one chunk) and returns WITHOUT ending it -- so the
    outbound stream stays active in _out_streams, simulating a producer that is
    mid-stream when it later stops (v0.10.1 D6)."""

    async def handle(self, message: Message) -> Message | None:
        await self.emit({"i": 0})
        return None


class _InterruptConsumer(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        try:
            async with aclosing(self.stream("prod", {}, idle_timeout=300.0)) as stream:
                async for _chunk in stream:
                    pass
            return self.reply({"result": "completed"})
        except StreamInterrupted:
            return self.reply({"result": "interrupted"})


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
    async def test_producer_stop_interrupts_consumer_immediately(self) -> None:
        """v0.10.1 (D6): a consumer mid-stream when its producer stops gets
        StreamInterrupted FAST -- not after the (300s) idle_timeout. Real two-
        agent runtime: the producer opens a stream and idles; stopping it fires
        the producer->consumer interrupt."""
        runtime = await _run(_LeakyProducer("prod"), _InterruptConsumer("cons"))
        try:
            task = asyncio.create_task(runtime.ask("cons", {}, timeout=10.0))
            # Wait until the producer actually has an active outbound stream
            # (the consumer started and chunk 0 flowed).
            prod = runtime._root_supervisor._children_by_name["prod"]
            for _ in range(200):
                if prod._out_streams:
                    break
                await asyncio.sleep(0.01)
            assert prod._out_streams  # stream is live

            # Stop just the producer -> its teardown interrupts the consumer.
            await prod._stop()
            reply = await asyncio.wait_for(task, timeout=3.0)  # << idle_timeout=300
            assert reply.payload["result"] == "interrupted"
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
        # v0.10.1 (D6): a producer that stops mid-stream interrupts its consumers.
        assert isinstance(AgentProcess._stream_error("producer_stopped"), StreamInterrupted)
        assert isinstance(AgentProcess._stream_error("boom"), StreamError)

    @pytest.mark.asyncio
    async def test_interrupt_out_streams_routes_producer_stopped_to_each_consumer(self) -> None:
        """v0.10.1 (D6): on teardown, the producer sends a producer_stopped
        stream-error to every active outbound stream's consumer, then clears
        its registry."""
        from unittest.mock import AsyncMock, MagicMock

        from civitas.process import _OutStream

        prod = _Producer("prod")
        prod._bus = MagicMock()
        prod._bus.route = AsyncMock()
        prod._out_streams["cid-1"] = _OutStream(started_at=0.0, recipient="cons-a", seq=3)
        prod._out_streams["cid-2"] = _OutStream(started_at=0.0, recipient="cons-b", seq=1)

        await prod._interrupt_out_streams()

        routed = [c.args[0] for c in prod._bus.route.call_args_list]
        by_recipient = {m.recipient: m for m in routed}
        assert set(by_recipient) == {"cons-a", "cons-b"}
        for m in routed:
            assert m.type == _STREAM_ERROR
            assert m.payload == {"error": "producer_stopped"}
        assert prod._out_streams == {}  # registry cleared
