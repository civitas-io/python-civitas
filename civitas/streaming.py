"""Shared streaming sink for bus-native and gateway streaming (R7 · D9).

A :class:`StreamSink` buffers the chunks of one in-flight stream (keyed by
``correlation_id`` by the caller) and is drained as an async iterator. Buffering
is bounded — past ``maxsize`` unconsumed chunks the stream fails fast with
``slow_consumer`` rather than growing without limit, since a fire-and-forget bus
gives no way to push backpressure onto the producing agent.

This lives in core (not the gateway) so both :class:`~civitas.process.AgentProcess`
and the HTTP gateway share one implementation without an inverted dependency.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any


class _StreamClosed(Exception):
    """Raised by :meth:`StreamSink.drain` when a stream ends abnormally.

    Carries a short reason (``slow_consumer``, ``out_of_order``, ``truncated_stream``,
    ``stream idle timeout``, ``max_stream_duration exceeded``, an agent error
    message, ...) that each caller maps onto its own error type.
    """


class StreamSink:
    """Bounded buffer collecting one stream's chunks, drained as an async iterator.

    The caller opens one sink per in-flight stream (keyed by ``correlation_id``) and
    feeds it as chunk/end/error frames arrive; a consumer drains it with
    :meth:`drain`. Past ``maxsize`` unconsumed chunks the stream fails fast with
    ``slow_consumer``.

    Sequence integrity (R7 · D7): when chunks carry a monotonic ``seq`` the sink
    detects gaps, duplicates, and reordering (``out_of_order``) and a truncated
    terminator (``truncated_stream``). Pass ``seq=None`` to disable the check — the
    gateway path does this to preserve its prior behaviour.
    """

    def __init__(self, maxsize: int) -> None:
        self._queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue()
        self._maxsize = maxsize
        self._pending = 0
        self._done = False
        self._next_seq = 0

    def push(self, payload: dict[str, Any], seq: int | None = None) -> None:
        """Buffer one chunk; fail fast on overflow or sequence violation."""
        if self._done:
            return
        if seq is not None and seq != self._next_seq:
            self._done = True
            self._queue.put_nowait(("error", {"error": "out_of_order"}))
            return
        if self._pending >= self._maxsize:
            self._done = True
            self._queue.put_nowait(("error", {"error": "slow_consumer"}))
            return
        if seq is not None:
            self._next_seq += 1
        self._pending += 1
        self._queue.put_nowait(("chunk", payload))

    def end(self, total: int | None = None) -> None:
        """Signal a clean end. ``total`` (if given) must match the chunks seen."""
        if self._done:
            return
        if total is not None and total != self._next_seq:
            self._done = True
            self._queue.put_nowait(("error", {"error": "truncated_stream"}))
            return
        self._done = True
        self._queue.put_nowait(("end", {}))

    def fail(self, error: str) -> None:
        if self._done:
            return
        self._done = True
        self._queue.put_nowait(("error", {"error": error}))

    async def drain(
        self,
        *,
        idle_timeout: float | None = None,
        max_duration: float | None = None,
        first_timeout: float | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield chunks until the stream ends; raise ``_StreamClosed`` on error/timeout.

        ``None`` timeouts mean unbounded — long-lived WebSocket sessions pass
        ``None``; request-scoped SSE / gRPC streams pass the configured limits.
        ``first_timeout`` (if set) bounds only the wait for the first chunk — used
        by :meth:`~civitas.process.AgentProcess.stream` for its ``timeout`` argument.
        """
        loop = asyncio.get_running_loop()
        deadline = (loop.time() + max_duration) if max_duration is not None else None
        first = True
        while True:
            wait = first_timeout if (first and first_timeout is not None) else idle_timeout
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise _StreamClosed("max_stream_duration exceeded")
                wait = remaining if wait is None else min(wait, remaining)
            try:
                async with asyncio.timeout(wait):
                    kind, payload = await self._queue.get()
            except TimeoutError:
                if deadline is not None and loop.time() >= deadline:
                    raise _StreamClosed("max_stream_duration exceeded") from None
                raise _StreamClosed("stream idle timeout") from None
            first = False
            if kind == "end":
                return
            if kind == "error":
                raise _StreamClosed(payload.get("error") or "stream error")
            self._pending -= 1
            yield payload
