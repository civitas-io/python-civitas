"""Agent Ed25519 identity — keypair generation, persistence, and signing."""

from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Any

from civitas.errors import ConfigurationError


def _require_nacl() -> None:
    try:
        import nacl.signing  # noqa: F401
    except ImportError as exc:
        raise ConfigurationError(
            "pynacl is required for message signing. "
            "Install it with: pip install 'civitas[security]'"
        ) from exc


class AgentIdentity:
    """Ed25519 signing keypair for a single agent.

    Private key stays local. Public key is registered in KeyRegistry so peers
    can verify signatures without holding any secret material.
    """

    def __init__(self, name: str, signing_key: Any) -> None:
        self.name = name
        self._signing_key = signing_key

    @property
    def verify_key(self) -> Any:
        return self._signing_key.verify_key

    def public_key_b64(self) -> str:
        """Base64-encoded 32-byte verify key — safe to embed in topology YAML."""
        return base64.b64encode(bytes(self.verify_key)).decode()

    def sign(self, data: bytes) -> bytes:
        """Return the 64-byte Ed25519 signature over ``data``."""
        signed: Any = self._signing_key.sign(data)
        return bytes(signed.signature)

    @classmethod
    def generate(cls, name: str) -> AgentIdentity:
        """Generate a fresh Ed25519 keypair."""
        _require_nacl()
        import nacl.signing

        return cls(name, nacl.signing.SigningKey.generate())

    @classmethod
    def load(cls, name: str, key_dir: Path) -> AgentIdentity:
        """Load from ``{key_dir}/{name}/id_ed25519`` (base64 seed, mode: provisioned)."""
        _require_nacl()
        import nacl.signing

        key_file = key_dir / name / "id_ed25519"
        if not key_file.exists():
            raise FileNotFoundError(
                f"Signing key not found: {key_file}. Generate with: civitas security init"
            )
        seed = base64.b64decode(key_file.read_text().strip())
        return cls(name, nacl.signing.SigningKey(seed))

    @classmethod
    def load_or_generate(cls, name: str, key_dir: Path) -> AgentIdentity:
        """Load existing keypair or generate and persist a new one (mode: auto)."""
        key_file = key_dir / name / "id_ed25519"
        if key_file.exists():
            return cls.load(name, key_dir)
        identity = cls.generate(name)
        identity.save(key_dir)
        return identity

    def save(self, key_dir: Path) -> None:
        """Persist keypair to ``{key_dir}/{name}/`` in OpenSSH-style layout.

        Private key written mode 0600, public key mode 0644.
        """
        agent_dir = key_dir / self.name
        agent_dir.mkdir(parents=True, exist_ok=True)

        priv_file = agent_dir / "id_ed25519"
        pub_file = agent_dir / "id_ed25519.pub"

        seed_b64 = base64.b64encode(bytes(self._signing_key)).decode()
        priv_file.write_text(seed_b64)
        os.chmod(priv_file, 0o600)

        pub_file.write_text(self.public_key_b64())
        os.chmod(pub_file, 0o644)
