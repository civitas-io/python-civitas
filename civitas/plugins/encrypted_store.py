"""EncryptingStateStore — backend-agnostic encryption of agent state at rest.

Wraps any :class:`~civitas.plugins.state.StateStore` and encrypts persisted
*values* with ChaCha20-Poly1305 (AEAD). Agent *names* pass through unchanged so
``list_agents()`` and indexing keep working. See
``docs/design/encrypted-statestore.md``.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from typing import TYPE_CHECKING, Any

from civitas.errors import ConfigurationError, StateDecryptionError

if TYPE_CHECKING:
    from civitas.plugins.state import StateStore

logger = logging.getLogger(__name__)

ENVELOPE_KEY = "__civitas_enc__"
"""Reserved top-level key holding the base64 encryption envelope."""

_ENVELOPE_VERSION = 1
_NONCE_SIZE = 12


def _load_chacha() -> Any:
    """Import ChaCha20Poly1305, raising ConfigurationError if unavailable."""
    try:
        from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
    except ImportError as exc:
        raise ConfigurationError(
            "Encrypted state store requires the 'cryptography' package. "
            "Install it with: pip install 'civitas[encryption]'"
        ) from exc
    return ChaCha20Poly1305


class EncryptingStateStore:
    """StateStore wrapper that encrypts values with ChaCha20-Poly1305 + AAD.

    Args:
        inner: The backing store whose values are encrypted.
        keys: Key ring mapping ``key_id`` → 32-byte key. Reads select the key
            by the envelope's ``key_id``; writes always use ``current_key_id``.
        current_key_id: Key id used to encrypt new writes; must be in ``keys``.
        allow_plaintext_read: When True, ``get`` returns unencrypted (legacy)
            values verbatim instead of raising, enabling gradual migration.
            The next ``set`` re-encrypts them.

    The agent name is bound as AEAD associated data, so an envelope written for
    one agent fails to decrypt under another agent's name.
    """

    def __init__(
        self,
        inner: StateStore,
        *,
        keys: dict[int, bytes],
        current_key_id: int,
        allow_plaintext_read: bool = False,
    ) -> None:
        if current_key_id not in keys:
            raise ConfigurationError(
                f"current_key_id {current_key_id} is not present in the key ring."
            )
        self._inner = inner
        self._keys = keys
        self._current_key_id = current_key_id
        self._allow_plaintext_read = allow_plaintext_read
        self._chacha = _load_chacha()

    async def get(self, agent_name: str) -> dict[str, Any] | None:
        """Retrieve and decrypt the persisted state for an agent."""
        raw = await self._inner.get(agent_name)
        if raw is None:
            return None
        if ENVELOPE_KEY not in raw:
            if self._allow_plaintext_read:
                return raw
            raise StateDecryptionError(
                f"Unencrypted state found for agent '{agent_name}'. Run "
                "`civitas state migrate` to re-encrypt, or set "
                "allow_plaintext_read=true for gradual migration."
            )
        return self._decrypt(agent_name, raw[ENVELOPE_KEY])

    async def set(self, agent_name: str, state: dict[str, Any]) -> None:
        """Encrypt and persist state for an agent."""
        plaintext = json.dumps(state).encode()
        nonce = os.urandom(_NONCE_SIZE)
        cipher = self._chacha(self._keys[self._current_key_id])
        ciphertext = cipher.encrypt(nonce, plaintext, agent_name.encode())
        envelope = bytes([_ENVELOPE_VERSION, self._current_key_id]) + nonce + ciphertext
        await self._inner.set(agent_name, {ENVELOPE_KEY: base64.b64encode(envelope).decode()})

    async def delete(self, agent_name: str) -> None:
        """Remove persisted state for an agent."""
        await self._inner.delete(agent_name)

    async def list_agents(self) -> list[str]:
        """Return all agent names with persisted state."""
        return await self._inner.list_agents()

    async def close(self) -> None:
        """Release resources held by the inner store."""
        await self._inner.close()

    def _decrypt(self, agent_name: str, encoded: str) -> dict[str, Any]:
        """Decode and decrypt a base64 envelope into the original state dict."""
        envelope = base64.b64decode(encoded)
        version = envelope[0]
        key_id = envelope[1]
        if version != _ENVELOPE_VERSION:
            raise StateDecryptionError(
                f"Unsupported envelope version {version} for agent '{agent_name}'."
            )
        if key_id not in self._keys:
            raise StateDecryptionError(
                f"Unknown key_id {key_id} for agent '{agent_name}'. The key is "
                "not in the configured key ring."
            )
        nonce = envelope[2 : 2 + _NONCE_SIZE]
        ciphertext = envelope[2 + _NONCE_SIZE :]
        cipher = self._chacha(self._keys[key_id])
        # InvalidTag on tamper/wrong key — never leak plaintext or key material.
        try:
            plaintext = cipher.decrypt(nonce, ciphertext, agent_name.encode())
        except Exception as exc:
            raise StateDecryptionError(
                f"Failed to decrypt state for agent '{agent_name}' "
                f"(key_id {key_id}): authentication failed."
            ) from exc
        decoded: dict[str, Any] = json.loads(plaintext)
        return decoded
