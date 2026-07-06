# Streaming

Civitas supports incremental, chunk-at-a-time replies both to external clients (through the
[gateway](gateway-streaming.md): SSE / WebSocket / gRPC server-streaming) and **agent-to-agent over the
message bus** (in-process, ZMQ, NATS). This page covers the bus-native, agent-to-agent path.

## Producing a stream

Inside `handle()`, open a `stream_reply()` block and `send()` chunks. The end terminator is sent
automatically when the block exits (or an error terminator if the body raises):

```python
class Summarizer(AgentProcess):
    async def handle(self, message: Message) -> None:
        async with self.stream_reply() as stream:
            async for token in self.llm_tokens(message.payload["doc"]):
                await stream.send({"delta": token})
```

Bound a producer so a slow or vanished consumer cannot make it stream forever:

```python
async with self.stream_reply(max_frames=2000, max_duration=60.0) as stream:
    ...
```

`emit()` and `end_stream()` are the lower-level manual equivalents.

## Consuming a stream

`stream()` sends a request and yields the producer's chunks as they arrive. It is safe to consume
directly inside `handle()`:

```python
class Orchestrator(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        async for chunk in self.stream("summarizer", {"doc": message.payload["doc"]}):
            await self.emit(chunk)          # re-stream onward, or accumulate
        return self.reply({"done": True})
```

Options: `timeout` (first-chunk deadline), `idle_timeout`, `max_duration`, `maxsize` (buffer bound).

If the producer answers with a plain `reply()` instead of streaming, the consumer transparently
receives a single item followed by the end — so a streaming caller never hangs against a
non-streaming handler.

### Prompt cancellation

Breaking out of the loop cancels the stream (a `civitas.stream.cancel` is sent to the producer, which
stops cooperatively). For *deterministic* cancellation, wrap the iterator in `contextlib.aclosing`:

```python
from contextlib import aclosing

async with aclosing(self.stream("worker", payload)) as stream:
    async for chunk in stream:
        if done(chunk):
            break                            # cancel is sent on block exit
```

## Errors

`stream()` raises typed errors from `civitas.errors`:

| Error | When |
|-------|------|
| `SlowConsumerError` | the consumer fell behind and the bounded buffer overflowed |
| `StreamTimeout` | idle timeout or maximum duration exceeded |
| `StreamInterrupted` | the local agent stopped while consuming |
| `StreamError` | a producer error, or a sequence gap / duplicate / reorder was detected |

## Backpressure & ordering

A fire-and-forget bus (ZMQ PUB/SUB, core NATS) has no end-to-end flow control, so streaming uses a
**bounded buffer that fails fast** with `SlowConsumerError` rather than growing without limit. Each
chunk carries a monotonic `Message.seq`; the consumer detects gaps, duplicates, and reordering
(including NATS JetStream redelivery) and raises `StreamError`.

## Reserved types

`civitas.stream.*` message types are runtime-internal — `send()` / `ask()` reject them. Use ordinary
application message types for your own traffic.

## Design

See [`design/bus-native-streaming.md`](design/bus-native-streaming.md) for the full design, decision
matrix, and threat model.
