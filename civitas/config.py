"""Centralized configuration — single source for all environment variables.

Usage:
    from civitas.config import settings
    serializer_name = settings.serializer

Values are frozen at instantiation time (module import). Tests can construct
a fresh ``Settings(env={...})`` to inject overrides without touching os.environ.
"""

from __future__ import annotations

import os

from civitas.errors import ConfigurationError

_VALID_SERIALIZERS = frozenset({"msgpack", "json"})


class SecretStr:
    """A string that masks its value in repr/str to prevent accidental log exposure."""

    def __init__(self, value: str | None) -> None:
        self._value = value

    def get(self) -> str | None:
        """Return the raw secret value."""
        return self._value

    def __repr__(self) -> str:
        return "SecretStr('**********')" if self._value else "SecretStr(None)"

    def __str__(self) -> str:
        return "**********" if self._value else ""

    def __bool__(self) -> bool:
        return bool(self._value)


class Settings:
    """Frozen configuration snapshot read from environment variables.

    All environment variable reads are centralized here. Application code
    should never call ``os.environ`` directly — use ``settings.<attr>``
    instead.

    Attributes:
        serializer:         Serializer format: ``'msgpack'`` (default) or ``'json'``.
        otel_endpoint:      OTEL collector gRPC endpoint, or ``None`` for console export.
        anthropic_api_key:  Anthropic API key (masked in logs).
        openai_api_key:     OpenAI API key (masked in logs).
        gemini_api_key:     Google Gemini API key (masked in logs).
        fiddler_api_key:    Fiddler API key (masked in logs).
        gateway_api_key:    Shared secret the gateway's require_api_key middleware
                            requires from clients in the X-API-Key header (masked).
        nats_url:           NATS server URL for distributed transport.
    """

    def __init__(self, env: dict[str, str] | None = None) -> None:
        _env: dict[str, str] | os._Environ[str] = env if env is not None else os.environ

        # Validated enum-style settings
        raw_serializer = _env.get("AGENCY_SERIALIZER", "msgpack")
        if raw_serializer not in _VALID_SERIALIZERS:
            raise ConfigurationError(
                f"AGENCY_SERIALIZER={raw_serializer!r} is not valid. "
                f"Choose from: {sorted(_VALID_SERIALIZERS)}"
            )
        self.serializer: str = raw_serializer

        # Plain string settings
        self.otel_endpoint: str | None = _env.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        self.nats_url: str = _env.get("NATS_URL", "nats://localhost:4222")

        # Secret strings — masked in repr/str
        self.anthropic_api_key = SecretStr(_env.get("ANTHROPIC_API_KEY"))
        self.openai_api_key = SecretStr(_env.get("OPENAI_API_KEY"))
        self.gemini_api_key = SecretStr(_env.get("GEMINI_API_KEY"))
        self.fiddler_api_key = SecretStr(_env.get("FIDDLER_API_KEY"))
        self.gateway_api_key = SecretStr(_env.get("CIVITAS_GATEWAY_API_KEY"))


settings = Settings()
