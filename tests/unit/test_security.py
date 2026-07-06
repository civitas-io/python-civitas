"""Tests for civitas.security — M4.2a Identity & Signing."""

from __future__ import annotations

import base64
import os
import tempfile
from pathlib import Path

import pytest

from civitas.errors import ConfigurationError, DeserializationError, SignatureError
from civitas.messages import Message
from civitas.security.config import IdentityConfig, SecurityConfig, SigningConfig
from civitas.security.identity import AgentIdentity
from civitas.security.registry import KeyRegistry
from civitas.security.signing import MessageSigner, NonceCache, SigningSerializer

# ---------------------------------------------------------------------------
# SecurityConfig
# ---------------------------------------------------------------------------


class TestSecurityConfig:
    def test_from_dict_defaults(self) -> None:
        cfg = SecurityConfig.from_dict({})
        assert cfg.identity.mode == "auto"
        assert cfg.identity.key_dir == Path("./civitas-keys")
        assert cfg.signing.enabled is False
        assert cfg.signing.require_verification is True
        assert cfg.signing.allow_unsigned is False

    def test_from_dict_full(self) -> None:
        cfg = SecurityConfig.from_dict(
            {
                "identity": {"mode": "provisioned", "key_dir": "/etc/civitas/keys"},
                "signing": {
                    "enabled": True,
                    "algorithm": "ed25519",
                    "require_verification": True,
                    "allow_unsigned": True,
                },
            }
        )
        assert cfg.identity.mode == "provisioned"
        assert cfg.identity.key_dir == Path("/etc/civitas/keys")
        assert cfg.signing.enabled is True
        assert cfg.signing.allow_unsigned is True

    def test_identity_config_defaults(self) -> None:
        cfg = IdentityConfig()
        assert cfg.mode == "auto"

    def test_signing_config_defaults(self) -> None:
        cfg = SigningConfig()
        assert cfg.enabled is False


# ---------------------------------------------------------------------------
# AgentIdentity
# ---------------------------------------------------------------------------


class TestAgentIdentity:
    def test_generate_produces_valid_identity(self) -> None:
        identity = AgentIdentity.generate("agent_a")
        assert identity.name == "agent_a"
        assert len(identity.public_key_b64()) > 0

    def test_sign_produces_64_byte_signature(self) -> None:
        identity = AgentIdentity.generate("agent_a")
        sig = identity.sign(b"hello world")
        assert len(sig) == 64

    def test_public_key_b64_is_valid_base64(self) -> None:
        identity = AgentIdentity.generate("agent_a")
        b64 = identity.public_key_b64()
        decoded = base64.b64decode(b64)
        assert len(decoded) == 32  # Ed25519 verify key is 32 bytes

    def test_save_and_load_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            key_dir = Path(tmpdir)
            original = AgentIdentity.generate("agent_a")
            original.save(key_dir)

            loaded = AgentIdentity.load("agent_a", key_dir)
            assert loaded.public_key_b64() == original.public_key_b64()

    def test_save_sets_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            key_dir = Path(tmpdir)
            identity = AgentIdentity.generate("agent_a")
            identity.save(key_dir)

            priv = key_dir / "agent_a" / "id_ed25519"
            pub = key_dir / "agent_a" / "id_ed25519.pub"
            assert oct(os.stat(priv).st_mode)[-3:] == "600"
            assert oct(os.stat(pub).st_mode)[-3:] == "644"

    def test_load_or_generate_creates_on_first_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            key_dir = Path(tmpdir)
            identity = AgentIdentity.load_or_generate("agent_a", key_dir)
            assert (key_dir / "agent_a" / "id_ed25519").exists()
            assert identity.name == "agent_a"

    def test_load_or_generate_returns_same_key_on_second_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            key_dir = Path(tmpdir)
            first = AgentIdentity.load_or_generate("agent_a", key_dir)
            second = AgentIdentity.load_or_generate("agent_a", key_dir)
            assert first.public_key_b64() == second.public_key_b64()

    def test_load_missing_key_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(FileNotFoundError):
                AgentIdentity.load("nonexistent", Path(tmpdir))

    def test_verify_key_validates_signature(self) -> None:
        identity = AgentIdentity.generate("agent_a")
        data = b"test payload"
        sig = identity.sign(data)
        # verify_key.verify should not raise
        identity.verify_key.verify(data, sig)

    def test_different_agents_have_different_keys(self) -> None:
        a = AgentIdentity.generate("agent_a")
        b = AgentIdentity.generate("agent_b")
        assert a.public_key_b64() != b.public_key_b64()

    def test_require_nacl_raises_config_error_when_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sys

        from civitas.security.identity import _require_nacl

        with monkeypatch.context() as m:
            m.setitem(sys.modules, "nacl", None)
            m.setitem(sys.modules, "nacl.signing", None)
            with pytest.raises(ConfigurationError, match="pynacl is required"):
                _require_nacl()


# ---------------------------------------------------------------------------
# KeyRegistry
# ---------------------------------------------------------------------------


class TestKeyRegistry:
    def test_register_and_get(self) -> None:
        registry = KeyRegistry()
        identity = AgentIdentity.generate("agent_a")
        registry.register("agent_a", identity.verify_key)
        assert registry.get("agent_a") is not None

    def test_get_unknown_returns_none(self) -> None:
        registry = KeyRegistry()
        assert registry.get("ghost") is None

    def test_register_b64_roundtrip(self) -> None:
        registry = KeyRegistry()
        identity = AgentIdentity.generate("agent_a")
        b64 = identity.public_key_b64()
        registry.register_b64("agent_a", b64)
        # Verify we can use the registered key for verification
        vk = registry.get("agent_a")
        assert vk is not None
        data = b"hello"
        sig = identity.sign(data)
        vk.verify(data, sig)  # should not raise

    def test_contains(self) -> None:
        registry = KeyRegistry()
        assert "agent_a" not in registry
        identity = AgentIdentity.generate("agent_a")
        registry.register("agent_a", identity.verify_key)
        assert "agent_a" in registry

    def test_len(self) -> None:
        registry = KeyRegistry()
        assert len(registry) == 0
        for i in range(3):
            ident = AgentIdentity.generate(f"agent_{i}")
            registry.register(f"agent_{i}", ident.verify_key)
        assert len(registry) == 3


# ---------------------------------------------------------------------------
# NonceCache
# ---------------------------------------------------------------------------


class TestNonceCache:
    def test_fresh_nonce_accepted(self) -> None:
        cache = NonceCache()
        assert cache.check_and_add(b"nonce_1") is True

    def test_duplicate_nonce_rejected(self) -> None:
        cache = NonceCache()
        cache.check_and_add(b"nonce_1")
        assert cache.check_and_add(b"nonce_1") is False

    def test_different_nonces_all_accepted(self) -> None:
        cache = NonceCache()
        for i in range(100):
            assert cache.check_and_add(f"nonce_{i}".encode()) is True

    def test_evicts_oldest_when_full(self) -> None:
        cache = NonceCache(maxsize=3)
        cache.check_and_add(b"a")
        cache.check_and_add(b"b")
        cache.check_and_add(b"c")
        # Adding 4th evicts 'a'
        cache.check_and_add(b"d")
        # 'a' should be fresh again (evicted)
        assert cache.check_and_add(b"a") is True


# ---------------------------------------------------------------------------
# MessageSigner
# ---------------------------------------------------------------------------


def _make_signer(
    agent_names: list[str],
    require_verification: bool = True,
    allow_unsigned: bool = False,
) -> tuple[MessageSigner, dict[str, AgentIdentity]]:
    identities = {name: AgentIdentity.generate(name) for name in agent_names}
    registry = KeyRegistry()
    for name, identity in identities.items():
        registry.register(name, identity.verify_key)
    config = SigningConfig(
        enabled=True,
        require_verification=require_verification,
        allow_unsigned=allow_unsigned,
    )
    return MessageSigner(identities, registry, config), identities


class TestMessageSigner:
    def test_sign_produces_v2_envelope(self) -> None:
        signer, _ = _make_signer(["agent_a"])
        msg = Message(sender="agent_a", recipient="agent_b", payload={"x": 1})
        envelope = signer.sign(msg.to_dict())
        assert envelope["v"] == 2
        assert "msg" in envelope
        assert "sig" in envelope

    def test_sign_includes_nonce(self) -> None:
        signer, _ = _make_signer(["agent_a"])
        envelope = signer.sign(Message(sender="agent_a").to_dict())
        assert len(envelope["sig"]["nonce"]) == 16

    def test_sign_includes_signer_and_alg(self) -> None:
        signer, _ = _make_signer(["agent_a"])
        envelope = signer.sign(Message(sender="agent_a").to_dict())
        assert envelope["sig"]["signer"] == "agent_a"
        assert envelope["sig"]["alg"] == "ed25519"

    def test_verify_valid_signature_returns_msg_dict(self) -> None:
        signer, _ = _make_signer(["agent_a"])
        msg = Message(sender="agent_a", recipient="agent_b", payload={"k": "v"})
        envelope = signer.sign(msg.to_dict())
        recovered = signer.verify(envelope)
        assert recovered["payload"] == {"k": "v"}
        assert recovered["sender"] == "agent_a"

    def test_verify_tampered_payload_raises(self) -> None:
        signer, _ = _make_signer(["agent_a"])
        envelope = signer.sign(Message(sender="agent_a").to_dict())
        # Tamper with msg after signing
        envelope["msg"]["payload"] = {"injected": True}
        with pytest.raises(SignatureError):
            signer.verify(envelope)

    def test_verify_tampered_sender_raises(self) -> None:
        signer, _ = _make_signer(["agent_a"])
        envelope = signer.sign(Message(sender="agent_a").to_dict())
        # Tamper with routing field
        envelope["msg"]["sender"] = "agent_evil"
        with pytest.raises(SignatureError):
            signer.verify(envelope)

    def test_verify_replayed_nonce_raises(self) -> None:
        signer, _ = _make_signer(["agent_a"])
        envelope = signer.sign(Message(sender="agent_a").to_dict())
        signer.verify(envelope)
        with pytest.raises(SignatureError, match="Replayed"):
            signer.verify(envelope)

    def test_verify_unknown_signer_strict_raises(self) -> None:
        signer, _ = _make_signer(["agent_a"])
        envelope = signer.sign(Message(sender="agent_a").to_dict())
        # Replace signer name with unknown agent
        envelope["sig"]["signer"] = "ghost"
        with pytest.raises(SignatureError, match="Unknown signer"):
            signer.verify(envelope)

    def test_verify_unknown_signer_allow_unsigned_passes(self) -> None:
        signer, _ = _make_signer(["agent_a"], allow_unsigned=True)
        envelope = signer.sign(Message(sender="agent_a").to_dict())
        envelope["sig"]["signer"] = "ghost"
        result = signer.verify(envelope)
        assert isinstance(result, dict)

    def test_cross_agent_signing(self) -> None:
        """Agent A signs; agent B's signer verifies using the shared registry."""
        identities_a = {"agent_a": AgentIdentity.generate("agent_a")}
        identities_b = {"agent_b": AgentIdentity.generate("agent_b")}

        # Shared registry with both public keys
        registry = KeyRegistry()
        registry.register("agent_a", identities_a["agent_a"].verify_key)
        registry.register("agent_b", identities_b["agent_b"].verify_key)

        config = SigningConfig(enabled=True, require_verification=True)
        signer_a = MessageSigner(identities_a, registry, config)
        signer_b = MessageSigner(identities_b, registry, config)

        msg = Message(sender="agent_a", recipient="agent_b", payload={"data": 42})
        envelope = signer_a.sign(msg.to_dict())
        recovered = signer_b.verify(envelope)
        assert recovered["payload"] == {"data": 42}

    def test_sign_unknown_local_sender_strict_raises(self) -> None:
        signer, _ = _make_signer(["agent_a"])
        with pytest.raises(SignatureError):
            signer.sign(Message(sender="ghost").to_dict())

    def test_sign_unknown_local_sender_allow_unsigned_passes(self) -> None:
        signer, _ = _make_signer(["agent_a"], allow_unsigned=True)
        envelope = signer.sign(Message(sender="ghost").to_dict())
        assert envelope["v"] == 2
        assert envelope["sig"]["value"] == b""

    def test_verify_known_signer_empty_signature_strict_raises(self) -> None:
        signer, _ = _make_signer(["agent_a"])
        # Construct a v=2 envelope with a known signer but deliberately empty signature
        nonce = os.urandom(16)
        envelope = {
            "v": 2,
            "msg": Message(sender="agent_a").to_dict(),
            "sig": {"signer": "agent_a", "alg": "ed25519", "value": b"", "nonce": nonce},
        }
        with pytest.raises(SignatureError, match="Missing signature"):
            signer.verify(envelope)

    def test_verify_known_signer_empty_signature_allow_unsigned_passes(self) -> None:
        signer, _ = _make_signer(["agent_a"], allow_unsigned=True)
        nonce = os.urandom(16)
        envelope = {
            "v": 2,
            "msg": Message(sender="agent_a").to_dict(),
            "sig": {"signer": "agent_a", "alg": "ed25519", "value": b"", "nonce": nonce},
        }
        result = signer.verify(envelope)
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# SigningSerializer
# ---------------------------------------------------------------------------


def _make_signing_serializer(
    agent_names: list[str],
    require_verification: bool = True,
    allow_unsigned: bool = False,
) -> SigningSerializer:
    signer, _ = _make_signer(agent_names, require_verification, allow_unsigned)
    config = SigningConfig(
        enabled=True,
        require_verification=require_verification,
        allow_unsigned=allow_unsigned,
    )
    return SigningSerializer(signer, config)


class TestSigningSerializer:
    def test_serialize_deserialize_roundtrip(self) -> None:
        ser = _make_signing_serializer(["agent_a"])
        msg = Message(sender="agent_a", recipient="agent_b", payload={"x": 99})
        recovered = ser.deserialize(ser.serialize(msg))
        assert recovered.sender == "agent_a"
        assert recovered.payload == {"x": 99}

    def test_serialized_bytes_are_v2_envelope(self) -> None:
        import msgpack

        ser = _make_signing_serializer(["agent_a"])
        msg = Message(sender="agent_a")
        data = ser.serialize(msg)
        envelope = msgpack.unpackb(data, raw=False)
        assert envelope["v"] == 2
        assert "sig" in envelope

    def test_deserialize_tampered_raises_signature_error(self) -> None:
        import msgpack

        ser = _make_signing_serializer(["agent_a"])
        data = ser.serialize(Message(sender="agent_a", payload={"a": 1}))
        envelope = msgpack.unpackb(data, raw=False)
        envelope["msg"]["payload"] = {"injected": True}
        tampered = msgpack.packb(envelope, use_bin_type=True)
        with pytest.raises(SignatureError):
            ser.deserialize(tampered)

    def test_deserialize_unsigned_v1_strict_raises(self) -> None:
        from civitas.serializer import MsgpackSerializer

        plain_ser = MsgpackSerializer()
        msg = Message(sender="agent_a")
        v1_bytes = plain_ser.serialize(msg)

        ser = _make_signing_serializer(["agent_a"], require_verification=True)
        with pytest.raises(SignatureError, match="Unsigned message"):
            ser.deserialize(v1_bytes)

    def test_deserialize_unsigned_v1_allow_unsigned_passes(self) -> None:
        from civitas.serializer import MsgpackSerializer

        plain_ser = MsgpackSerializer()
        msg = Message(sender="agent_a")
        v1_bytes = plain_ser.serialize(msg)

        ser = _make_signing_serializer(["agent_a"], require_verification=True, allow_unsigned=True)
        recovered = ser.deserialize(v1_bytes)
        assert recovered.sender == "agent_a"

    def test_deserialize_corrupt_bytes_raises_deserialization_error(self) -> None:
        ser = _make_signing_serializer(["agent_a"])
        with pytest.raises(DeserializationError):
            ser.deserialize(b"\xff\xfe garbage")

    def test_deserialize_replayed_message_raises(self) -> None:
        ser = _make_signing_serializer(["agent_a"])
        msg = Message(sender="agent_a")
        data = ser.serialize(msg)
        ser.deserialize(data)
        with pytest.raises(SignatureError, match="Replayed"):
            ser.deserialize(data)

    def test_deserialize_v2_no_sig_block_strict_raises(self) -> None:
        import msgpack

        ser = _make_signing_serializer(["agent_a"])
        envelope = {"v": 2, "msg": Message(sender="agent_a").to_dict()}  # no "sig" key
        data = msgpack.packb(envelope, use_bin_type=True)
        with pytest.raises(SignatureError, match="no signature block"):
            ser.deserialize(data)

    def test_deserialize_v2_no_sig_block_allow_unsigned_passes(self) -> None:
        import msgpack

        ser = _make_signing_serializer(["agent_a"], allow_unsigned=True)
        envelope = {"v": 2, "msg": Message(sender="agent_a").to_dict()}
        data = msgpack.packb(envelope, use_bin_type=True)
        result = ser.deserialize(data)
        assert result.sender == "agent_a"

    def test_deserialize_msg_dict_not_reconstructable_raises(self) -> None:
        import msgpack

        ser = _make_signing_serializer(["agent_a"], allow_unsigned=True)
        # v=1 unsigned (allow_unsigned=True) but payload contains bytes — not JSON-serialisable,
        # so Message.__post_init__ raises ValueError, caught as DeserializationError.
        bad = {"schema_version": 1, "payload": b"binary not json"}
        data = msgpack.packb(bad, use_bin_type=True)
        with pytest.raises(DeserializationError):
            ser.deserialize(data)


# ---------------------------------------------------------------------------
# Runtime integration — _extract_public_keys helper
# ---------------------------------------------------------------------------


class TestExtractPublicKeys:
    def test_extracts_agent_block_public_key(self) -> None:
        from civitas.runtime import _extract_public_keys

        config = {
            "supervision": {
                "name": "root",
                "children": [
                    {"agent": {"name": "agent_a", "type": "MyAgent", "public_key": "abc123"}},
                    {"agent": {"name": "agent_b", "type": "MyAgent"}},
                ],
            }
        }
        keys = _extract_public_keys(config)
        assert keys == {"agent_a": "abc123"}

    def test_extracts_nested_supervisor_public_keys(self) -> None:
        from civitas.runtime import _extract_public_keys

        config = {
            "supervision": {
                "name": "root",
                "children": [
                    {
                        "supervisor": {
                            "name": "inner",
                            "children": [
                                {
                                    "agent": {
                                        "name": "agent_a",
                                        "type": "X",
                                        "public_key": "key_a",
                                    }
                                },
                            ],
                        }
                    }
                ],
            }
        }
        keys = _extract_public_keys(config)
        assert keys == {"agent_a": "key_a"}

    def test_returns_empty_when_no_public_keys(self) -> None:
        from civitas.runtime import _extract_public_keys

        config = {
            "supervision": {
                "name": "root",
                "children": [{"agent": {"name": "agent_a", "type": "X"}}],
            }
        }
        assert _extract_public_keys(config) == {}

    def test_handles_missing_supervision(self) -> None:
        from civitas.runtime import _extract_public_keys

        assert _extract_public_keys({}) == {}


# ---------------------------------------------------------------------------
# SignatureError is a CivitasError
# ---------------------------------------------------------------------------


class TestSignatureError:
    def test_is_civitas_error(self) -> None:
        from civitas.errors import CivitasError

        err = SignatureError("bad sig")
        assert isinstance(err, CivitasError)

    def test_message(self) -> None:
        err = SignatureError("test message")
        assert str(err) == "test message"

    def test_exported_from_civitas(self) -> None:
        import civitas

        assert hasattr(civitas, "SignatureError")
        assert civitas.SignatureError is SignatureError
