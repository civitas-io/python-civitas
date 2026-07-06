"""Public key registry — maps agent names to their Ed25519 verify keys."""

from __future__ import annotations

import base64
from typing import Any


class KeyRegistry:
    """Thread-safe (GIL-protected) mapping of agent names to verify keys."""

    def __init__(self) -> None:
        self._keys: dict[str, Any] = {}  # agent_name → nacl.signing.VerifyKey

    def register(self, name: str, verify_key: Any) -> None:
        """Register a verify key obtained from an AgentIdentity."""
        self._keys[name] = verify_key

    def register_b64(self, name: str, public_key_b64: str) -> None:
        """Register a verify key from a base64-encoded string (e.g., from topology YAML)."""
        import nacl.signing

        key_bytes = base64.b64decode(public_key_b64)
        self._keys[name] = nacl.signing.VerifyKey(key_bytes)

    def get(self, name: str) -> Any | None:
        """Return the verify key for ``name``, or None if not registered."""
        return self._keys.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._keys

    def __len__(self) -> int:
        return len(self._keys)
