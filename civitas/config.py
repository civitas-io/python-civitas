"""Centralized configuration — single source for all environment variables.

Usage:
    from civitas.config import settings
    serializer_name = settings.serializer

Values are frozen at instantiation time (module import). Tests can construct
a fresh ``Settings(env={...})`` to inject overrides without touching os.environ.
"""

from __future__ import annotations

import base64
import binascii
import os

from civitas.errors import ConfigurationError

_VALID_SERIALIZERS = frozenset({"msgpack", "json"})
_STATE_KEY_BYTES = 32


def decode_state_key(value: str) -> bytes:
    """Decode a base64 state key into raw bytes, validating its length.

    Args:
        value: Base64-encoded 32-byte key (e.g. from ``CIVITAS_STATE_KEY``).

    Raises:
        ConfigurationError: If the value is not valid base64 or does not decode
            to exactly 32 bytes.
    """
    try:
        raw = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ConfigurationError("State encryption key is not valid base64.") from exc
    if len(raw) != _STATE_KEY_BYTES:
        raise ConfigurationError(
            f"State encryption key must decode to {_STATE_KEY_BYTES} bytes, got {len(raw)}."
        )
    return raw


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

        # Gateway JWT bearer auth (civitas[jwt]) — consumed by JwtVerifier.
        self.jwt_jwks_url: str | None = _env.get("CIVITAS_JWT_JWKS_URL")
        self.jwt_audience: str | None = _env.get("CIVITAS_JWT_AUDIENCE")
        self.jwt_issuer: str | None = _env.get("CIVITAS_JWT_ISSUER")
        raw_algorithms = _env.get("CIVITAS_JWT_ALGORITHMS", "")
        self.jwt_algorithms: tuple[str, ...] = tuple(
            a.strip() for a in raw_algorithms.split(",") if a.strip()
        )
        self.jwt_public_key = SecretStr(_env.get("CIVITAS_JWT_PUBLIC_KEY"))
        self.jwt_secret = SecretStr(_env.get("CIVITAS_JWT_SECRET"))

        # Encrypted state store key (civitas[encryption]) — base64 32-byte key.
        self.state_key = SecretStr(_env.get("CIVITAS_STATE_KEY"))

        # Gateway mTLS: exact-match allowlist of full client-cert subject DNs.
        # Semicolon-separated (a DN itself contains commas, e.g. "CN=svc,O=Acme").
        raw_dns = _env.get("CIVITAS_GATEWAY_MTLS_ALLOWED_DNS", "")
        self.gateway_mtls_allowed_dns: frozenset[str] = frozenset(
            d.strip() for d in raw_dns.split(";") if d.strip()
        )


settings = Settings()


def load_state_key(env_var: str = "CIVITAS_STATE_KEY") -> bytes:
    """Load and decode a state encryption key from the environment.

    Read at call time (not from the frozen ``settings`` snapshot) so multi-process
    workers and rotation keys named by the operator resolve correctly. Env access
    is confined to this config module.

    Args:
        env_var: Name of the environment variable holding the base64 key.

    Raises:
        ConfigurationError: If the variable is unset or not a base64 32-byte key.
    """
    raw = settings.state_key.get() if env_var == "CIVITAS_STATE_KEY" else os.environ.get(env_var)
    if not raw:
        raise ConfigurationError(
            f"Encrypted state store requires the {env_var} environment variable "
            "(base64-encoded 32-byte key)."
        )
    return decode_state_key(raw)
