"""SecretsProvider protocol and built-in implementations."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol, runtime_checkable

from civitas.errors import ConfigurationError


@runtime_checkable
class SecretsProvider(Protocol):
    """Protocol for secret resolution.

    Implementations read from environment variables, files, Vault, etc.
    The default provider is ``EnvSecretsProvider`` (reads from ``os.environ``).
    """

    def get(self, key: str) -> str | None:
        """Return the secret value for ``key``, or None if not found."""
        ...

    def require(self, key: str) -> str:
        """Return the secret value for ``key``, raising ``ConfigurationError`` if missing."""
        ...


class EnvSecretsProvider:
    """Reads secrets from environment variables.

    This is the default provider used when no explicit ``SecretsProvider``
    is configured. Suitable for any deployment that surfaces secrets as env
    vars (Kubernetes Secrets, Docker secrets, Vault sidecar, etc.).
    """

    def get(self, key: str) -> str | None:
        return os.environ.get(key)

    def require(self, key: str) -> str:
        value = self.get(key)
        if value is None:
            raise ConfigurationError(f"Required secret '{key}' is not set in the environment.")
        return value


class FileSecretsProvider:
    """Reads secrets from a key=value flat file (one secret per line).

    Suitable for Docker secrets mounted at ``/run/secrets/`` or any
    simple file-based secrets workflow. Lines starting with ``#`` and
    blank lines are ignored. Secrets are loaded once at instantiation.

    Example file::

        ANTHROPIC_API_KEY=sk-ant-...
        OPENAI_API_KEY=sk-...
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._secrets: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            raise ConfigurationError(f"Secrets file not found: {self._path}")
        for line in self._path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            self._secrets[key.strip()] = value.strip()

    def get(self, key: str) -> str | None:
        return self._secrets.get(key)

    def require(self, key: str) -> str:
        value = self.get(key)
        if value is None:
            raise ConfigurationError(f"Required secret '{key}' not found in {self._path}.")
        return value
