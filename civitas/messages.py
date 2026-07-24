"""Message envelope and system message constants."""

from __future__ import annotations

import dataclasses
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any


def _new_span_id() -> str:
    """Generate a 16-hex-char span ID."""
    return os.urandom(8).hex()


def _uuid7() -> str:
    """Generate a UUID7 (time-sortable) as a hex string.

    Implements RFC 9562 UUID v7: 48-bit unix_ts_ms | 4-bit version | 12-bit rand_a
    | 2-bit variant | 62-bit rand_b.
    """
    timestamp_ms = int(time.time() * 1000)
    rand_bytes = os.urandom(10)

    # 48-bit timestamp
    uuid_int = timestamp_ms & ((1 << 48) - 1)
    uuid_int <<= 4
    # version = 7
    uuid_int |= 7
    uuid_int <<= 12
    # rand_a (12 bits from first 2 random bytes)
    uuid_int |= int.from_bytes(rand_bytes[:2], "big") & 0x0FFF
    uuid_int <<= 2
    # variant = 0b10
    uuid_int |= 0b10
    uuid_int <<= 62
    # rand_b (62 bits from remaining 8 random bytes)
    uuid_int |= int.from_bytes(rand_bytes[2:], "big") & ((1 << 62) - 1)

    hex_str = f"{uuid_int:032x}"
    return f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:]}"


def _now() -> float:
    return time.time()


@dataclass(slots=True)
class Message:
    """Standard message envelope for all inter-agent communication.

    Every message in Civitas is wrapped in this envelope. The envelope carries
    routing and observability metadata. The payload carries application data.
    """

    # Identity
    id: str = field(default_factory=_uuid7)
    type: str = "message"

    # Routing
    sender: str = ""
    recipient: str = ""
    correlation_id: str | None = None
    reply_to: str | None = None

    # Payload — must contain only JSON-serializable primitives
    payload: dict[str, Any] = field(default_factory=dict)

    # Metadata
    timestamp: float = field(default_factory=_now)
    trace_id: str = ""
    span_id: str = ""
    parent_span_id: str | None = None
    attempt: int = 0
    priority: int = 0
    ttl: float | None = None
    seq: int | None = None

    def __post_init__(self) -> None:
        try:
            json.dumps(self.payload)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Message payload must contain only JSON-serializable values: {exc}"
            ) from exc

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for serialization.

        Includes ``schema_version`` so receivers can detect and handle schema
        evolution. Unknown keys in received dicts are filtered by ``from_dict``.
        """
        return {
            "schema_version": 1,
            "id": self.id,
            "type": self.type,
            "sender": self.sender,
            "recipient": self.recipient,
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "attempt": self.attempt,
            "priority": self.priority,
            "ttl": self.ttl,
            "seq": self.seq,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Message:
        """Reconstruct a Message from a plain dict."""
        return cls(**{k: v for k, v in data.items() if k in _MESSAGE_FIELDS})


# Computed once at import time — used by Message.from_dict to filter unknown keys.
_MESSAGE_FIELDS: frozenset[str] = frozenset(f.name for f in dataclasses.fields(Message))


# System message types reserved for runtime internals.
# Application code must never send messages with these types.
SYSTEM_MESSAGE_TYPES: frozenset[str] = frozenset(
    {
        "_agency.heartbeat",
        "_agency.heartbeat_ack",
        "_agency.health_probe",
        "_agency.health_ack",
        "_agency.shutdown",
        "_agency.restart",
        "_agency.register",
        "_agency.deregister",
        "_agency.suspend",
        "_agency.resume",
    }
)


STREAM_MESSAGE_TYPES: frozenset[str] = frozenset(
    {
        "civitas.stream.chunk",
        "civitas.stream.end",
        "civitas.stream.error",
        "civitas.stream.cancel",
        "civitas.stream.credit",
    }
)
