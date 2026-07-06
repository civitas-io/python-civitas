"""Audit types — AuditEvent TypedDict and AuditSink protocol."""

from __future__ import annotations

from typing import Any, Protocol, TypedDict, runtime_checkable


class AuditEvent(TypedDict):
    """A single structured audit record.

    Fields are kept flat and JSON-serializable so any sink can write them
    without further transformation.
    """

    event: str  # "message.route" | "tool.call" | "secret.access" | "sandbox.exec" | "sandbox.deny"
    ts: str  # ISO 8601 UTC timestamp (e.g. "2026-05-03T12:00:00.123456Z")
    agent: str  # agent name performing the action; "" if system/unknown
    signer_id: str  # signing identity for message events; "" if unsigned/N/A
    details: dict[str, Any]  # event-specific payload


@runtime_checkable
class AuditSink(Protocol):
    """Protocol for audit event sinks.

    All methods are async so sinks can batch and flush without blocking
    the event loop. Implementations must be safe to call concurrently.
    """

    async def emit(self, event: AuditEvent) -> None:
        """Record a single audit event. Must not raise."""
        ...

    async def flush(self) -> None:
        """Flush any buffered events to the underlying store."""
        ...

    async def close(self) -> None:
        """Flush and release all resources."""
        ...
