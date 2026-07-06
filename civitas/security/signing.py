"""Message signing, verification, nonce replay protection, and SigningSerializer."""

from __future__ import annotations

import os
from collections import OrderedDict
from typing import Any

import msgpack

from civitas.errors import DeserializationError, SignatureError
from civitas.messages import Message
from civitas.security.config import SigningConfig
from civitas.security.identity import AgentIdentity
from civitas.security.registry import KeyRegistry


class NonceCache:
    """Bounded LRU cache of seen nonces — replay protection for signed messages.

    Caps at ``maxsize`` entries (default 10,000 ≈ 320KB). Evicts the oldest
    entry when full. Check-and-add is O(1).
    """

    def __init__(self, maxsize: int = 10_000) -> None:
        self._cache: OrderedDict[bytes, None] = OrderedDict()
        self._maxsize = maxsize

    def check_and_add(self, nonce: bytes) -> bool:
        """Return True if nonce is fresh (unseen). False means replay — reject the message."""
        if nonce in self._cache:
            return False
        self._cache[nonce] = None
        if len(self._cache) > self._maxsize:
            self._cache.popitem(last=False)
        return True


class MessageSigner:
    """Signs outgoing messages and verifies incoming signatures.

    Holds private keys for agents hosted in this process and the public key
    registry for all reachable agents (local + remote).
    """

    def __init__(
        self,
        identities: dict[str, AgentIdentity],
        registry: KeyRegistry,
        config: SigningConfig,
    ) -> None:
        self._identities = identities
        self._registry = registry
        self._config = config
        self._nonce_cache = NonceCache()

    def add_identity(self, identity: AgentIdentity) -> None:
        """Register a private signing identity for a locally-hosted agent.

        Used to provision a per-incarnation key for a dynamically spawned child
        (R6 · D11) so the child can sign its own outgoing messages.
        """
        self._identities[identity.name] = identity

    def trust(self, name: str, verify_key: Any) -> None:
        """Register a peer's public verify key so its signatures can be checked."""
        self._registry.register(name, verify_key)

    def sign(self, msg_dict: dict[str, Any]) -> dict[str, Any]:
        """Wrap ``msg_dict`` in a v=2 signed envelope.

        If the sender has no local identity (system messages, unknown sender),
        the signature value is empty and the receiver handles it per config.
        """
        sender = str(msg_dict.get("sender", ""))
        identity = self._identities.get(sender)
        nonce = os.urandom(16)

        signed_bytes = msgpack.packb(
            {"v": 2, "msg": msg_dict, "signer": sender, "nonce": nonce},
            use_bin_type=True,
        )

        if identity is not None:
            signature = identity.sign(signed_bytes)
        else:
            signature = b""

        if not signature and self._config.require_verification and not self._config.allow_unsigned:
            raise SignatureError(
                f"No signing key for sender '{sender}' — cannot produce a valid signature"
            )

        return {
            "v": 2,
            "msg": msg_dict,
            "sig": {
                "signer": sender,
                "alg": "ed25519",
                "value": signature,
                "nonce": nonce,
            },
        }

    def verify(self, envelope: dict[str, Any]) -> dict[str, Any]:
        """Verify a v=2 signed envelope, return the inner message dict.

        Raises:
            SignatureError: on replay, unknown signer (when strict), or bad signature.
        """
        sig = envelope.get("sig", {})
        signer = str(sig.get("signer", ""))
        nonce: bytes = sig.get("nonce", b"")
        signature: bytes = sig.get("value", b"")
        msg_dict: dict[str, Any] = envelope.get("msg", {})

        if not self._nonce_cache.check_and_add(nonce):
            raise SignatureError(f"Replayed nonce in message from '{signer}'")

        verify_key = self._registry.get(signer)
        if verify_key is None:
            if self._config.require_verification and not self._config.allow_unsigned:
                raise SignatureError(
                    f"Unknown signer '{signer}': no public key registered. "
                    f"Add their public key to the topology or KeyRegistry."
                )
            return msg_dict

        if not signature:
            if self._config.require_verification and not self._config.allow_unsigned:
                raise SignatureError(f"Missing signature from known agent '{signer}'")
            return msg_dict

        signed_bytes = msgpack.packb(
            {"v": 2, "msg": msg_dict, "signer": signer, "nonce": nonce},
            use_bin_type=True,
        )

        try:
            verify_key.verify(signed_bytes, signature)
        except Exception as exc:
            raise SignatureError(f"Signature verification failed for '{signer}': {exc}") from exc

        return msg_dict


class SigningSerializer:
    """Msgpack serializer with Ed25519 envelope signing (wire format v=2).

    Drop-in replacement for MsgpackSerializer when security is enabled.
    Signs on serialize, verifies on deserialize. InProcess transport never
    creates one of these — the standard MsgpackSerializer is used instead.
    """

    def __init__(self, signer: MessageSigner, config: SigningConfig) -> None:
        self._signer = signer
        self._config = config

    @property
    def signer(self) -> MessageSigner:
        """The underlying :class:`MessageSigner` (identity + key management)."""
        return self._signer

    def serialize(self, message: Message) -> bytes:
        msg_dict = message.to_dict()
        envelope = self._signer.sign(msg_dict)
        result: bytes = msgpack.packb(envelope, use_bin_type=True)
        return result

    def deserialize(self, data: bytes) -> Message:
        try:
            raw: dict[str, Any] = msgpack.unpackb(data, raw=False)
        except Exception as exc:
            raise DeserializationError(f"Failed to deserialize msgpack data: {exc}") from exc

        if raw.get("v") == 2:
            if "sig" in raw:
                msg_dict = self._signer.verify(raw)
            else:
                # v=2 envelope missing sig block — treat as unsigned
                if self._config.require_verification and not self._config.allow_unsigned:
                    raise SignatureError("Received v=2 envelope with no signature block")
                msg_dict = raw.get("msg", raw)
        else:
            # v=1 — legacy unsigned message
            if self._config.require_verification and not self._config.allow_unsigned:
                raise SignatureError(
                    "Unsigned message rejected (require_verification=true). "
                    "Set signing.allow_unsigned: true for rolling upgrades."
                )
            msg_dict = raw

        try:
            return Message.from_dict(msg_dict)
        except Exception as exc:
            raise DeserializationError(f"Failed to reconstruct Message: {exc}") from exc
