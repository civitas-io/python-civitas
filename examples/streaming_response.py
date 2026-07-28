"""Bus-native agent-to-agent streaming (docs/streaming.md) — v0.9.2.

Two sides of the same feature: a producer that opens ``stream_reply()`` inside
``handle()`` and yields chunks as they're ready, and a consumer that async-iterates
``self.stream(...)`` to receive them as they arrive — both over the message bus
(works identically in-process, over ZMQ, or over NATS; this demo uses in-process).

Usage:
    python examples/streaming_response.py
"""

from __future__ import annotations

import asyncio

from civitas import AgentProcess, Runtime, Supervisor
from civitas.messages import Message


class TokenProducer(AgentProcess):
    """Streams a canned "response" one token at a time, as an LLM streaming
    completion would — the end terminator is sent automatically when the
    `async with` block exits."""

    async def handle(self, message: Message) -> Message | None:
        text = "Streaming works the same in-process, over ZMQ, or over NATS."
        async with self.stream_reply() as stream:
            for token in text.split(" "):
                await asyncio.sleep(0.05)  # simulate token-by-token generation
                await stream.send({"token": token})
        return None  # the stream_reply block already sent the terminator


class Aggregator(AgentProcess):
    """Consumes the producer's stream chunk-by-chunk and accumulates them into
    one reply — the consumer counterpart to TokenProducer.handle()."""

    async def handle(self, message: Message) -> Message | None:
        tokens: list[str] = []
        async for chunk in self.stream("producer", {}):
            tokens.append(chunk["token"])
            print(f"  received chunk: {chunk['token']!r}")
        return self.reply({"text": " ".join(tokens), "chunk_count": len(tokens)})


async def main() -> None:
    runtime = Runtime(
        supervisor=Supervisor(
            "root", children=[TokenProducer("producer"), Aggregator("aggregator")]
        )
    )
    await runtime.start()

    print("Streaming from 'producer' through 'aggregator'...")
    result = await runtime.ask("aggregator", {})
    print(f"\nAssembled text: {result.payload['text']!r}")
    print(f"Chunk count: {result.payload['chunk_count']}")

    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
