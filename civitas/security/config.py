"""Security configuration dataclasses parsed from the topology YAML `security:` block."""

from __future__ import annotations

import ssl
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class IdentityConfig:
    mode: str = "auto"  # "auto" | "provisioned"
    key_dir: Path = field(default_factory=lambda: Path("./civitas-keys"))


@dataclass
class SigningConfig:
    enabled: bool = False
    algorithm: str = "ed25519"
    require_verification: bool = True
    allow_unsigned: bool = False


@dataclass
class ZmqCurveConfig:
    """ZeroMQ CURVE security configuration.

    server_public_key / server_secret_key: Z85-encoded keypair for the proxy.
    client_public_key / client_secret_key: Z85-encoded keypair for connecting Workers.
    """

    enabled: bool = False
    server_public_key: str = ""
    server_secret_key: str = ""
    client_public_key: str = ""
    client_secret_key: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ZmqCurveConfig:
        return cls(
            enabled=data.get("enabled", False),
            server_public_key=data.get("server_public_key", ""),
            server_secret_key=data.get("server_secret_key", ""),
            client_public_key=data.get("client_public_key", ""),
            client_secret_key=data.get("client_secret_key", ""),
        )


@dataclass
class NatsTlsConfig:
    """NATS TLS + nkeys configuration.

    cert / key / ca: paths to PEM-format files for mutual TLS.
    nkey_seed: NKey seed string for Ed25519-based subject auth (requires civitas[nkeys]).
    """

    enabled: bool = False
    cert: Path | None = None
    key: Path | None = None
    ca: Path | None = None
    nkey_seed: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NatsTlsConfig:
        tls_data = data.get("tls", {})
        cert_raw = tls_data.get("cert")
        key_raw = tls_data.get("key")
        ca_raw = tls_data.get("ca")
        return cls(
            enabled=tls_data.get("enabled", bool(cert_raw or key_raw or ca_raw)),
            cert=Path(cert_raw) if cert_raw else None,
            key=Path(key_raw) if key_raw else None,
            ca=Path(ca_raw) if ca_raw else None,
            nkey_seed=data.get("nkey_seed", ""),
        )

    def build_ssl_context(self) -> ssl.SSLContext:
        """Build an SSLContext from the configured cert/key/ca paths."""
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        if self.ca:
            ctx.load_verify_locations(cafile=str(self.ca))
        if self.cert and self.key:
            ctx.load_cert_chain(certfile=str(self.cert), keyfile=str(self.key))
        return ctx


@dataclass
class GatewayAuthConfig:
    """HTTP gateway auth config parsed from the topology gateway ``auth:`` block.

    JWT secrets and URLs come from ``CIVITAS_JWT_*`` env vars (see
    :mod:`civitas.config`); the mTLS DN allowlist comes from
    ``CIVITAS_GATEWAY_MTLS_ALLOWED_DNS``. This block carries the non-secret mTLS
    trust settings that are applied to :class:`~civitas.gateway.core.GatewayConfig`.
    """

    tls_ca_cert: str | None = None
    client_cert_mode: str = "none"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GatewayAuthConfig:
        mtls_data = data.get("mtls", {})
        return cls(
            tls_ca_cert=mtls_data.get("ca_cert") or mtls_data.get("tls_ca_cert"),
            client_cert_mode=mtls_data.get("client_cert_mode", "none"),
        )


@dataclass
class TransportSecurityConfig:
    """Per-transport security configuration (ZMQ CURVE + NATS TLS/nkeys)."""

    zmq: ZmqCurveConfig = field(default_factory=ZmqCurveConfig)
    nats: NatsTlsConfig = field(default_factory=NatsTlsConfig)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TransportSecurityConfig:
        zmq_data = data.get("zmq", {})
        nats_data = data.get("nats", {})
        zmq_cfg = ZmqCurveConfig.from_dict(zmq_data.get("curve", zmq_data))
        nats_cfg = NatsTlsConfig.from_dict(nats_data)
        return cls(zmq=zmq_cfg, nats=nats_cfg)


@dataclass
class SecurityConfig:
    identity: IdentityConfig = field(default_factory=IdentityConfig)
    signing: SigningConfig = field(default_factory=SigningConfig)
    transport: TransportSecurityConfig = field(default_factory=TransportSecurityConfig)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SecurityConfig:
        identity_data = data.get("identity", {})
        signing_data = data.get("signing", {})
        transport_data = data.get("transport", {})

        key_dir_raw = identity_data.get("key_dir", "./civitas-keys")
        identity = IdentityConfig(
            mode=identity_data.get("mode", "auto"),
            key_dir=Path(key_dir_raw),
        )
        signing = SigningConfig(
            enabled=signing_data.get("enabled", False),
            algorithm=signing_data.get("algorithm", "ed25519"),
            require_verification=signing_data.get("require_verification", True),
            allow_unsigned=signing_data.get("allow_unsigned", False),
        )
        transport = TransportSecurityConfig.from_dict(transport_data)
        return cls(identity=identity, signing=signing, transport=transport)
