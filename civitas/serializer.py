"""Serializer protocol and implementations (msgpack, JSON)."""

from __future__ import annotations

import json
from typing import Any, Protocol

import msgpack

from civitas.errors import DeserializationError
from civitas.messages import Message

try:
    import orjson
except ImportError:  # pragma: no cover - orjson ships in the civitas[fast] extra
    orjson = None  # type: ignore[assignment]


class Serializer(Protocol):
    """Protocol for message serialization/deserialization."""

    def serialize(self, message: Message) -> bytes:
        """Encode a Message to bytes for transport."""
        ...

    def deserialize(self, data: bytes) -> Message:
        """Decode bytes back into a Message.

        Raises:
            DeserializationError: if the bytes are corrupt, malformed, or in the
                wrong format. Callers never need to catch library-specific exceptions.
        """
        ...


class MsgpackSerializer:
    """Default serializer using MessagePack — fast and compact."""

    def serialize(self, message: Message) -> bytes:
        """Encode a Message to MessagePack bytes."""
        result: bytes = msgpack.packb(message.to_dict(), use_bin_type=True)
        return result

    def deserialize(self, data: bytes) -> Message:
        """Decode MessagePack bytes into a Message.

        Raises:
            DeserializationError: on corrupt or malformed bytes.
        """
        try:
            raw: dict[str, Any] = msgpack.unpackb(data, raw=False)
            return Message.from_dict(raw)
        except Exception as exc:
            raise DeserializationError(f"Failed to deserialize msgpack data: {exc}") from exc


class JsonSerializer:
    """Human-readable JSON serializer.

    Uses orjson (Rust-backed, from ``civitas[fast]``) when installed for a large
    speedup, transparently falling back to the standard library ``json`` module.
    Output is plain JSON either way, so the two backends interoperate on the wire.
    """

    def serialize(self, message: Message) -> bytes:
        """Encode a Message to JSON bytes."""
        data = message.to_dict()
        if orjson is not None:
            return orjson.dumps(data)
        return json.dumps(data).encode("utf-8")

    def deserialize(self, data: bytes) -> Message:
        """Decode JSON bytes into a Message.

        Raises:
            DeserializationError: on corrupt, malformed, or non-UTF-8 bytes.
        """
        try:
            raw = orjson.loads(data) if orjson is not None else json.loads(data.decode("utf-8"))
            return Message.from_dict(raw)
        except Exception as exc:
            raise DeserializationError(f"Failed to deserialize JSON data: {exc}") from exc
