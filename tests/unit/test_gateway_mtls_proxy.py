"""Tests for HTTP mTLS via a trusted reverse proxy (#25, RFC 9440 Client-Cert).

Implements the plan's §4 test matrix (T1-T18). Reuses the JWT/ASGI-driver patterns
from ``test_gateway_auth.py`` (``rsa_keys``, ``_drive``, allowlist monkeypatch) and the
CA + leaf cert-generation pattern from ``test_gateway_ws_grpc_auth.py`` (``_make_ca`` /
``_make_leaf``), DER-encoding the leaf for the ``Client-Cert`` header since RFC 9440
carries base64 DER, not PEM.

T4 (``proxy_headers=False`` on the constructed ``uvicorn.Config``) and T15 (an
``X-Forwarded-For`` spoof from an untrusted peer is still rejected) are the two
regression gates for the Oracle B1 blocker — they prove the CIDR trust check keys on the
true TCP peer, not a client-forgeable forwarded address.
"""

from __future__ import annotations

import asyncio
import base64
import datetime
import sys
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from civitas.config import Settings
from civitas.errors import ConfigurationError
from civitas.gateway.asgi import GatewayASGI
from civitas.gateway.core import GatewayConfig, HTTPGateway
from civitas.gateway.mtls import _client_cert_from_headers, _dn_from_der, _dn_from_pem
from civitas.gateway.router import RouteTable
from civitas.messages import Message
from civitas.runtime import Runtime

_MTLS_PATH = "civitas.gateway.mtls.require_client_cert"
_CIDRS = frozenset({"10.0.0.0/8"})
_ALLOWED_PEER = ("10.0.0.5", 5000)
_UNTRUSTED_PEER = ("203.0.113.9", 5000)

# ---------------------------------------------------------------------------
# Certificate helpers (self-signed CA + leaf; mirrors test_gateway_ws_grpc_auth.py)
# ---------------------------------------------------------------------------


def _make_ca() -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Civitas Test CA")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _make_leaf(
    ca_key: rsa.RSAPrivateKey, ca_cert: x509.Certificate, common_name: str
) -> x509.Certificate:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Acme"),
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        ]
    )
    now = datetime.datetime.now(datetime.UTC)
    return (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .sign(ca_key, hashes.SHA256())
    )


def _client_cert_header(der: bytes) -> bytes:
    """Encode DER bytes as an RFC 9440 ``Client-Cert`` value: ``:<base64-DER>:``."""
    return b":" + base64.b64encode(der) + b":"


@pytest.fixture(scope="module")
def proxy_certs() -> SimpleNamespace:
    """A CA-signed leaf exposed as PEM, DER, its DN, and a ready RFC 9440 header value."""
    ca_key, ca_cert = _make_ca()
    leaf = _make_leaf(ca_key, ca_cert, "service")
    leaf_pem = leaf.public_bytes(serialization.Encoding.PEM)
    leaf_der = leaf.public_bytes(serialization.Encoding.DER)
    return SimpleNamespace(
        leaf_pem=leaf_pem,
        leaf_der=leaf_der,
        dn=_dn_from_der(leaf_der),
        header=_client_cert_header(leaf_der),
    )


# ---------------------------------------------------------------------------
# ASGI / gateway harness (mirrors test_gateway_auth.py)
# ---------------------------------------------------------------------------


def _set_mtls_allowlist(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    env = {"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS": value} if value else {}
    monkeypatch.setattr("civitas.gateway.mtls.settings", Settings(env=env))


def _proxy_config() -> GatewayConfig:
    return GatewayConfig(
        mtls_source="proxy_header", trusted_proxy_cidrs=_CIDRS, middleware=[_MTLS_PATH]
    )


def _mkasgi(config: GatewayConfig) -> GatewayASGI:
    gateway = MagicMock(spec=HTTPGateway)
    gateway.name = "api"
    gateway.ask = AsyncMock(return_value=MagicMock(spec=Message, payload={"ok": True}))
    return GatewayASGI(gateway=gateway, route_table=RouteTable.from_config([]), config=config)


async def _drive(
    asgi: GatewayASGI,
    *,
    raw_headers: list[tuple[bytes, bytes]],
    client: tuple[str, int] | None = None,
    extensions: dict[str, Any] | None = None,
    method: str = "POST",
    path: str = "/agents/foo",
) -> int:
    """Drive one HTTP request through the ASGI app and return the response status."""
    scope: dict[str, Any] = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": raw_headers,
        "query_string": b"",
    }
    if client is not None:
        scope["client"] = client
    if extensions is not None:
        scope["extensions"] = extensions

    async def receive() -> dict[str, Any]:
        return {"body": b"{}", "more_body": False}

    sent: list[dict[str, Any]] = []

    async def send(msg: dict[str, Any]) -> None:
        sent.append(msg)

    await asgi(scope, receive, send)
    return next(e for e in sent if e["type"] == "http.response.start")["status"]


def _patch_uvicorn_capture(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Fake ``uvicorn.Server`` (no socket bind) while keeping the real ``uvicorn.Config``.

    Keeping the real ``Config`` is what lets T4 assert ``proxy_headers`` on the actual
    constructed object. The returned dict captures the config once ``Server`` is built.
    """
    import uvicorn

    captured: dict[str, Any] = {}

    class _FakeServer:
        def __init__(self, cfg: Any) -> None:
            captured["config"] = cfg
            self._exit = asyncio.Event()

        @property
        def should_exit(self) -> bool:
            return self._exit.is_set()

        @should_exit.setter
        def should_exit(self, value: bool) -> None:
            if value:
                self._exit.set()

        async def serve(self) -> None:
            await self._exit.wait()

    monkeypatch.setattr(uvicorn, "Server", _FakeServer)
    return captured


# ---------------------------------------------------------------------------
# T1-T3, T5 — GatewayConfig validation (D1/D2/D3/S4)
# ---------------------------------------------------------------------------


class TestConfigValidation:
    def test_t1_invalid_mtls_source_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="mtls_source"):
            GatewayConfig(mtls_source="proxy-header")

    def test_t2_proxy_header_empty_cidrs_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="trusted_proxy_cidrs"):
            GatewayConfig(mtls_source="proxy_header")

    def test_t3_proxy_header_invalid_cidr_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="invalid"):
            GatewayConfig(mtls_source="proxy_header", trusted_proxy_cidrs=frozenset({"not-a-cidr"}))

    def test_t5_proxy_header_requires_client_cert_mode_none(self) -> None:
        with pytest.raises(ConfigurationError) as exc:
            GatewayConfig(
                mtls_source="proxy_header",
                trusted_proxy_cidrs=_CIDRS,
                client_cert_mode="required",
                tls_ca_cert="ca.pem",
                tls_cert="c.pem",
                tls_key="k.pem",
            )
        message = str(exc.value)
        assert "client_cert_mode must be 'none'" in message
        assert "grpc_enabled" in message
        assert "separate HTTPGateway" in message

    def test_default_source_is_direct(self) -> None:
        cfg = GatewayConfig()
        assert cfg.mtls_source == "direct"
        assert cfg.trusted_proxy_cidrs == frozenset()

    def test_valid_proxy_header_config(self) -> None:
        cfg = GatewayConfig(mtls_source="proxy_header", trusted_proxy_cidrs=_CIDRS)
        assert cfg.mtls_source == "proxy_header"
        assert cfg.client_cert_mode == "none"


# ---------------------------------------------------------------------------
# T4, T6, T7 — on_start wiring (D8/D9/B1)
# ---------------------------------------------------------------------------


class TestOnStart:
    @pytest.mark.asyncio
    async def test_t4_proxy_header_sets_proxy_headers_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # B1 regression gate: uvicorn's ProxyHeadersMiddleware must be off so
        # scope["client"] cannot be rewritten from a client X-Forwarded-For.
        captured = _patch_uvicorn_capture(monkeypatch)
        gw = HTTPGateway(
            "api",
            GatewayConfig(
                port=18411,
                mtls_source="proxy_header",
                trusted_proxy_cidrs=_CIDRS,
                middleware=[_MTLS_PATH],
            ),
        )
        gw._bus = None
        await gw.on_start()
        try:
            assert captured["config"].proxy_headers is False
        finally:
            await gw.on_stop()

    @pytest.mark.asyncio
    async def test_t4_direct_mode_keeps_uvicorn_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # B1 must be scoped: direct mode's uvicorn config is left at its default.
        captured = _patch_uvicorn_capture(monkeypatch)
        gw = HTTPGateway("api", GatewayConfig(port=18412))
        gw._bus = None
        await gw.on_start()
        try:
            assert captured["config"].proxy_headers is True
        finally:
            await gw.on_stop()

    @pytest.mark.asyncio
    async def test_t6_cryptography_unimportable_fails_startup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_uvicorn_capture(monkeypatch)
        monkeypatch.setitem(sys.modules, "cryptography", None)
        gw = HTTPGateway(
            "api",
            GatewayConfig(
                port=18413,
                mtls_source="proxy_header",
                trusted_proxy_cidrs=_CIDRS,
                middleware=[_MTLS_PATH],
            ),
        )
        gw._bus = None
        with pytest.raises(ConfigurationError):
            await gw.on_start()
        assert captured == {}  # crashed before uvicorn.Server was ever constructed

    @pytest.mark.asyncio
    async def test_t7_missing_require_client_cert_fails_startup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured = _patch_uvicorn_capture(monkeypatch)
        gw = HTTPGateway(
            "api",
            GatewayConfig(port=18414, mtls_source="proxy_header", trusted_proxy_cidrs=_CIDRS),
        )
        gw._bus = None
        with pytest.raises(ConfigurationError, match="require_client_cert"):
            await gw.on_start()
        assert captured == {}


# ---------------------------------------------------------------------------
# Extractor unit tests (P2.1 — localized, hand-built scopes)
# ---------------------------------------------------------------------------


class TestExtractor:
    def test_valid_allowed_returns_dn(self, proxy_certs: SimpleNamespace) -> None:
        scope = {"client": _ALLOWED_PEER, "headers": [(b"client-cert", proxy_certs.header)]}
        assert _client_cert_from_headers(scope, _CIDRS) == {"dn": proxy_certs.dn}

    def test_untrusted_cidr_returns_none(self, proxy_certs: SimpleNamespace) -> None:
        scope = {"client": _UNTRUSTED_PEER, "headers": [(b"client-cert", proxy_certs.header)]}
        assert _client_cert_from_headers(scope, _CIDRS) is None

    def test_missing_client_returns_none(self, proxy_certs: SimpleNamespace) -> None:
        scope = {"headers": [(b"client-cert", proxy_certs.header)]}
        assert _client_cert_from_headers(scope, _CIDRS) is None

    def test_zero_headers_returns_none(self) -> None:
        assert _client_cert_from_headers({"client": _ALLOWED_PEER, "headers": []}, _CIDRS) is None

    def test_two_headers_mixed_case_returns_none(self, proxy_certs: SimpleNamespace) -> None:
        scope = {
            "client": _ALLOWED_PEER,
            "headers": [(b"Client-Cert", proxy_certs.header), (b"client-cert", proxy_certs.header)],
        }
        assert _client_cert_from_headers(scope, _CIDRS) is None

    def test_bad_base64_returns_none(self) -> None:
        scope = {"client": _ALLOWED_PEER, "headers": [(b"client-cert", b":not!!base64:")]}
        assert _client_cert_from_headers(scope, _CIDRS) is None

    def test_valid_base64_non_der_returns_none(self) -> None:
        junk = b":" + base64.b64encode(b"not a certificate") + b":"
        scope = {"client": _ALLOWED_PEER, "headers": [(b"client-cert", junk)]}
        assert _client_cert_from_headers(scope, _CIDRS) is None

    def test_missing_colon_delimiters_returns_none(self, proxy_certs: SimpleNamespace) -> None:
        raw = base64.b64encode(proxy_certs.leaf_der)  # no surrounding colons
        scope = {"client": _ALLOWED_PEER, "headers": [(b"client-cert", raw)]}
        assert _client_cert_from_headers(scope, _CIDRS) is None


# ---------------------------------------------------------------------------
# T8-T15 — full ASGI round trips (D2/D4/D5/B1/B2)
# ---------------------------------------------------------------------------


class TestRoundTrip:
    @pytest.mark.asyncio
    async def test_t8_valid_allowlisted_dn_200(
        self, monkeypatch: pytest.MonkeyPatch, proxy_certs: SimpleNamespace
    ) -> None:
        _set_mtls_allowlist(monkeypatch, proxy_certs.dn)
        status = await _drive(
            _mkasgi(_proxy_config()),
            client=_ALLOWED_PEER,
            raw_headers=[(b"client-cert", proxy_certs.header)],
        )
        assert status == 200

    @pytest.mark.asyncio
    async def test_t9_valid_unlisted_dn_403(
        self, monkeypatch: pytest.MonkeyPatch, proxy_certs: SimpleNamespace
    ) -> None:
        _set_mtls_allowlist(monkeypatch, "CN=not-the-client,O=Acme,C=US")
        status = await _drive(
            _mkasgi(_proxy_config()),
            client=_ALLOWED_PEER,
            raw_headers=[(b"client-cert", proxy_certs.header)],
        )
        assert status == 403

    @pytest.mark.asyncio
    async def test_t10_disallowed_cidr_401(
        self, monkeypatch: pytest.MonkeyPatch, proxy_certs: SimpleNamespace
    ) -> None:
        _set_mtls_allowlist(monkeypatch, proxy_certs.dn)
        status = await _drive(
            _mkasgi(_proxy_config()),
            client=_UNTRUSTED_PEER,
            raw_headers=[(b"client-cert", proxy_certs.header)],
        )
        assert status == 401

    @pytest.mark.asyncio
    async def test_t11_zero_headers_401(
        self, monkeypatch: pytest.MonkeyPatch, proxy_certs: SimpleNamespace
    ) -> None:
        _set_mtls_allowlist(monkeypatch, proxy_certs.dn)
        status = await _drive(_mkasgi(_proxy_config()), client=_ALLOWED_PEER, raw_headers=[])
        assert status == 401

    @pytest.mark.asyncio
    async def test_t12_two_headers_singleton_violation_401(
        self, monkeypatch: pytest.MonkeyPatch, proxy_certs: SimpleNamespace
    ) -> None:
        _set_mtls_allowlist(monkeypatch, proxy_certs.dn)
        status = await _drive(
            _mkasgi(_proxy_config()),
            client=_ALLOWED_PEER,
            raw_headers=[
                (b"Client-Cert", proxy_certs.header),
                (b"client-cert", proxy_certs.header),
            ],
        )
        assert status == 401

    @pytest.mark.asyncio
    async def test_t13_malformed_base64_401_not_500(
        self, monkeypatch: pytest.MonkeyPatch, proxy_certs: SimpleNamespace
    ) -> None:
        _set_mtls_allowlist(monkeypatch, proxy_certs.dn)
        status = await _drive(
            _mkasgi(_proxy_config()),
            client=_ALLOWED_PEER,
            raw_headers=[(b"client-cert", b":not!!valid!!base64:")],
        )
        assert status == 401

    @pytest.mark.asyncio
    async def test_t14_valid_base64_non_der_401_not_500(
        self, monkeypatch: pytest.MonkeyPatch, proxy_certs: SimpleNamespace
    ) -> None:
        _set_mtls_allowlist(monkeypatch, proxy_certs.dn)
        junk = b":" + base64.b64encode(b"not a certificate") + b":"
        status = await _drive(
            _mkasgi(_proxy_config()), client=_ALLOWED_PEER, raw_headers=[(b"client-cert", junk)]
        )
        assert status == 401

    @pytest.mark.asyncio
    async def test_t15_xff_spoof_from_untrusted_peer_401(
        self, monkeypatch: pytest.MonkeyPatch, proxy_certs: SimpleNamespace
    ) -> None:
        # B1 regression gate: an attacker at an untrusted peer IP sets X-Forwarded-For to
        # an in-CIDR address and forwards a valid cert. The extractor keys on the true TCP
        # peer (scope["client"]), never the forgeable XFF header, so the cert is ignored.
        _set_mtls_allowlist(monkeypatch, proxy_certs.dn)
        status = await _drive(
            _mkasgi(_proxy_config()),
            client=_UNTRUSTED_PEER,
            raw_headers=[
                (b"x-forwarded-for", b"10.0.0.5"),
                (b"client-cert", proxy_certs.header),
            ],
        )
        assert status == 401


# ---------------------------------------------------------------------------
# T16 — shared DN formatter round-trip (D4)
# ---------------------------------------------------------------------------


class TestDnRoundTrip:
    def test_t16_pem_and_der_yield_identical_dn(self, proxy_certs: SimpleNamespace) -> None:
        assert _dn_from_pem(proxy_certs.leaf_pem) == _dn_from_der(proxy_certs.leaf_der)
        assert _dn_from_pem(proxy_certs.leaf_pem) == proxy_certs.dn


# ---------------------------------------------------------------------------
# T17 — direct mode unchanged (Constraint 5 regression)
# ---------------------------------------------------------------------------


class TestDirectModeRegression:
    @pytest.mark.asyncio
    async def test_t17_direct_mode_ignores_client_cert_header(
        self, monkeypatch: pytest.MonkeyPatch, proxy_certs: SimpleNamespace
    ) -> None:
        # Default (direct) mode reads the ASGI TLS extension, never the RFC 9440 header —
        # a Client-Cert header from any peer is ignored, falling through to 401.
        _set_mtls_allowlist(monkeypatch, proxy_certs.dn)
        status = await _drive(
            _mkasgi(GatewayConfig(middleware=[_MTLS_PATH])),
            client=_ALLOWED_PEER,
            raw_headers=[(b"client-cert", proxy_certs.header)],
        )
        assert status == 401

    @pytest.mark.asyncio
    async def test_t17_direct_mode_scope_extension_still_authorizes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        dn = "CN=service,O=Acme,C=US"
        _set_mtls_allowlist(monkeypatch, dn)
        status = await _drive(
            _mkasgi(GatewayConfig(middleware=[_MTLS_PATH])),
            raw_headers=[],
            extensions={"tls": {"client_cert_name": dn, "client_cert_chain": ["LEAF-PEM"]}},
        )
        assert status == 200


# ---------------------------------------------------------------------------
# T18 — YAML topology plumbing (P3)
# ---------------------------------------------------------------------------


class TestYamlPlumbing:
    def test_t18_topology_wires_mtls_source_and_cidrs(self) -> None:
        config = {
            "supervision": {
                "name": "root",
                "children": [
                    {
                        "name": "api",
                        "type": "http_gateway",
                        "config": {
                            "port": 18499,
                            "middleware": ["civitas.gateway.mtls.require_client_cert"],
                            "auth": {
                                "mtls": {
                                    "mtls_source": "proxy_header",
                                    "trusted_proxy_cidrs": ["10.0.0.0/8"],
                                }
                            },
                        },
                    }
                ],
            }
        }
        rt = Runtime.from_config_dict(config)
        gateways = [a for a in rt.all_agents() if isinstance(a, HTTPGateway)]
        assert len(gateways) == 1
        assert gateways[0]._gw_config.mtls_source == "proxy_header"
        assert gateways[0]._gw_config.trusted_proxy_cidrs == frozenset({"10.0.0.0/8"})
