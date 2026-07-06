"""Tests for Serializer protocol and msgpack/json implementations."""

import json

import msgpack  # type: ignore[import-untyped]
import pytest

from civitas.errors import DeserializationError
from civitas.messages import Message
from civitas.serializer import JsonSerializer, MsgpackSerializer


def test_msgpack_roundtrip():
    """MsgpackSerializer preserves all fields through serialize/deserialize."""
    ser = MsgpackSerializer()
    original = Message(
        type="research_query",
        sender="agent_a",
        recipient="agent_b",
        payload={"query": "test", "count": 42, "nested": {"a": [1, 2, 3]}},
        trace_id="trace123",
        span_id="span456",
        parent_span_id="parent789",
        attempt=1,
        priority=1,
        correlation_id="corr001",
        reply_to="agent_a",
    )
    data = ser.serialize(original)
    assert isinstance(data, bytes)

    restored = ser.deserialize(data)
    assert restored.type == original.type
    assert restored.sender == original.sender
    assert restored.recipient == original.recipient
    assert restored.payload == original.payload
    assert restored.trace_id == original.trace_id
    assert restored.span_id == original.span_id
    assert restored.parent_span_id == original.parent_span_id
    assert restored.attempt == original.attempt
    assert restored.priority == original.priority
    assert restored.correlation_id == original.correlation_id
    assert restored.reply_to == original.reply_to
    assert restored.id == original.id


def test_json_roundtrip():
    """JsonSerializer preserves all fields through serialize/deserialize."""
    ser = JsonSerializer()
    original = Message(
        type="test",
        sender="a",
        recipient="b",
        payload={"key": "value"},
    )
    data = ser.serialize(original)
    assert isinstance(data, bytes)
    # JSON should be human-readable
    text = data.decode("utf-8")
    assert '"type"' in text
    assert '"test"' in text

    restored = ser.deserialize(data)
    assert restored.type == original.type
    assert restored.payload == original.payload


def test_msgpack_compact():
    """Msgpack output is more compact than JSON for the same message."""
    msg = Message(
        type="test",
        sender="agent_a",
        recipient="agent_b",
        payload={"data": "x" * 100},
    )
    msgpack_data = MsgpackSerializer().serialize(msg)
    json_data = JsonSerializer().serialize(msg)
    assert len(msgpack_data) < len(json_data)


def test_empty_payload_roundtrip():
    """Empty payload roundtrips correctly."""
    for ser in [MsgpackSerializer(), JsonSerializer()]:
        msg = Message(type="ping", payload={})
        restored = ser.deserialize(ser.serialize(msg))
        assert restored.payload == {}


def test_none_optional_fields_roundtrip():
    """None values for optional fields roundtrip correctly."""
    for ser in [MsgpackSerializer(), JsonSerializer()]:
        msg = Message(
            correlation_id=None,
            reply_to=None,
            parent_span_id=None,
        )
        restored = ser.deserialize(ser.serialize(msg))
        assert restored.correlation_id is None
        assert restored.reply_to is None
        assert restored.parent_span_id is None


# F05-4: corrupt bytes raise DeserializationError


def test_msgpack_deserialize_corrupt_bytes():
    """MsgpackSerializer raises DeserializationError on corrupt bytes."""
    ser = MsgpackSerializer()
    with pytest.raises(DeserializationError):
        ser.deserialize(b"not valid msgpack \xff\xfe")


def test_json_deserialize_corrupt_bytes():
    """JsonSerializer raises DeserializationError on corrupt bytes."""
    ser = JsonSerializer()
    with pytest.raises(DeserializationError):
        ser.deserialize(b"not valid json {{{")


def test_json_deserialize_invalid_utf8():
    """JsonSerializer raises DeserializationError on non-UTF-8 bytes."""
    ser = JsonSerializer()
    with pytest.raises(DeserializationError):
        ser.deserialize(b"\xff\xfe invalid utf-8")


# F05-5: cross-serializer format mismatch raises DeserializationError


def test_json_bytes_fed_to_msgpack_raises():
    """JSON bytes fed to MsgpackSerializer raises DeserializationError."""
    json_bytes = JsonSerializer().serialize(Message(type="test"))
    with pytest.raises(DeserializationError):
        MsgpackSerializer().deserialize(json_bytes)


def test_msgpack_bytes_fed_to_json_raises():
    """Msgpack bytes fed to JsonSerializer raises DeserializationError."""
    msgpack_bytes = MsgpackSerializer().serialize(Message(type="test"))
    with pytest.raises(DeserializationError):
        JsonSerializer().deserialize(msgpack_bytes)


# F05-2: schema_version present in serialized output


def test_schema_version_in_msgpack_output():
    """Msgpack-serialized bytes include schema_version field."""
    msg = Message(type="test")
    raw = msgpack.unpackb(MsgpackSerializer().serialize(msg), raw=False)
    assert raw["schema_version"] == 1


def test_schema_version_in_json_output():
    """JSON-serialized bytes include schema_version field."""
    msg = Message(type="test")
    raw = json.loads(JsonSerializer().serialize(msg).decode("utf-8"))
    assert raw["schema_version"] == 1


# civitas[fast]: orjson-backed JsonSerializer stays interoperable with stdlib json


def test_json_serializer_uses_orjson_when_available():
    """civitas[fast] is installed in dev, so the orjson backend is active."""
    import civitas.serializer as serializer_module

    assert serializer_module.orjson is not None
    msg = Message(type="t", payload={"a": 1, "b": [1, 2, 3]})
    restored = JsonSerializer().deserialize(JsonSerializer().serialize(msg))
    assert restored.payload == {"a": 1, "b": [1, 2, 3]}


def test_json_serializer_reads_stdlib_json_bytes():
    """Bytes produced by the stdlib json backend deserialize regardless of backend."""
    msg = Message(type="x", payload={"k": "v", "n": 7})
    stdlib_bytes = json.dumps(msg.to_dict()).encode("utf-8")
    restored = JsonSerializer().deserialize(stdlib_bytes)
    assert restored.type == "x"
    assert restored.payload == {"k": "v", "n": 7}


def test_json_unicode_roundtrip():
    """Non-ASCII payloads roundtrip through JsonSerializer (orjson emits UTF-8)."""
    ser = JsonSerializer()
    msg = Message(type="t", payload={"emoji": "🚀", "accents": "café"})
    restored = ser.deserialize(ser.serialize(msg))
    assert restored.payload == {"emoji": "🚀", "accents": "café"}
