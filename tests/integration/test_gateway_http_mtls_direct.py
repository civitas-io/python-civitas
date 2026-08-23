"""Real end-to-end proof that GH #25's `direct`-mode half is fixed
(docs/design/gateway-http-mtls-direct.md): a real self-signed CA, a real
server leaf, and real client leaves, against an actual running
`HTTPGateway` + uvicorn + `TlsAwareHttpToolsProtocol` -- not mocks, and
not just config assembly.

Surfaced 2026-08-23 from civitas-io/presidium's M7 work, which needs
`mtls_source="direct"` to actually work for a genuinely self-hostable,
single-process server. Mirrors the four real handshake scenarios
Presidium's own packages/presidium-contrib/tests/integration/
test_presidium_server_mtls.py already proved the gap with, run here
directly against this repo's own `HTTPGateway` to close the gap at its
actual source rather than only downstream.
"""

from __future__ import annotations

import asyncio
import datetime
import ipaddress
import socket
import ssl
from types import SimpleNamespace

import pytest

pytest.importorskip("uvicorn")  # civitas[http]
pytest.importorskip("cryptography")  # civitas[jwt] / DN extraction

import httpx  # noqa: E402
from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: E402
from cryptography.x509.oid import NameOID  # noqa: E402

from civitas import Runtime, Supervisor  # noqa: E402
from civitas.config import Settings  # noqa: E402
from civitas.gateway import GatewayConfig, HTTPGateway  # noqa: E402
from tests.conftest import EchoAgent  # noqa: E402

# ---------------------------------------------------------------------------
# Certificate helpers -- a real self-signed CA + real leaves, no mocking.
# Mirrors civitas-io/presidium's own test_presidium_server_mtls.py, which
# found (and fixed) the same two real cert-generation gotchas modern
# OpenSSL enforces: a SubjectKeyIdentifier/AuthorityKeyIdentifier pair, and
# a KeyUsage extension asserting keyCertSign/cRLSign on the CA.
# ---------------------------------------------------------------------------


def _rsa_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _key_pem(key: rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )


def _make_ca(common_name: str) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = _rsa_key()
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime.now(datetime.UTC)
    ski = x509.SubjectKeyIdentifier.from_public_key(key.public_key())
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(ski, critical=False)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _make_leaf(
    ca_key: rsa.RSAPrivateKey,
    ca_cert: x509.Certificate,
    common_name: str,
    *,
    san: str | None = None,
) -> tuple[bytes, bytes]:
    key = _rsa_key()
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Acme"),
        ]
    )
    now = datetime.datetime.now(datetime.UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))
    )
    if san is not None:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName(san), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
    aki = x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key())
    builder = builder.add_extension(aki, critical=False)
    cert = builder.sign(ca_key, hashes.SHA256())
    return _key_pem(key), cert.public_bytes(serialization.Encoding.PEM)


def _dn(pem: bytes) -> str:
    return x509.load_pem_x509_certificate(pem).subject.rfc4514_string()


def _client_ssl_context(
    ca_path: str, cert_path: str | None = None, key_path: str | None = None
) -> ssl.SSLContext:
    """A real, fully-configured client SSLContext -- httpx's deprecated
    cert=(cert, key) + verify=<str path> combination has a real bug/
    incompatibility in this httpx version (found running this test for the
    first time: a fully valid, trusted, correctly-loaded client cert still
    produced a bare httpx.ReadError with zero server-side signal). The
    modern, recommended API -- one fully-configured ssl.SSLContext passed
    as verify=... -- works correctly, confirmed empirically."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(ca_path)
    if cert_path is not None:
        ctx.load_cert_chain(cert_path, key_path)
    return ctx


@pytest.fixture(scope="module")
def tls_certs(tmp_path_factory: pytest.TempPathFactory) -> SimpleNamespace:
    ca_key, ca_cert = _make_ca("Civitas Test CA")
    ca_pem = ca_cert.public_bytes(serialization.Encoding.PEM)
    server_key, server_cert = _make_leaf(ca_key, ca_cert, "localhost", san="localhost")
    client_key, client_cert = _make_leaf(ca_key, ca_cert, "trusted-service")
    intruder_key, intruder_cert = _make_leaf(ca_key, ca_cert, "intruder-service")

    other_ca_key, other_ca_cert = _make_ca("A Completely Different CA")
    outsider_key, outsider_cert = _make_leaf(other_ca_key, other_ca_cert, "outsider-service")

    directory = tmp_path_factory.mktemp("mtls-direct")

    def _write(name: str, data: bytes) -> str:
        path = directory / name
        path.write_bytes(data)
        return str(path)

    return SimpleNamespace(
        ca_path=_write("ca.pem", ca_pem),
        server_cert_path=_write("server.pem", server_cert),
        server_key_path=_write("server.key", server_key),
        client_cert_path=_write("client.pem", client_cert),
        client_key_path=_write("client.key", client_key),
        client_dn=_dn(client_cert),
        intruder_cert_path=_write("intruder.pem", intruder_cert),
        intruder_key_path=_write("intruder.key", intruder_key),
        outsider_cert_path=_write("outsider.pem", outsider_cert),
        outsider_key_path=_write("outsider.key", outsider_key),
    )


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


async def _wait_for_port_open(host: str, port: int, timeout_seconds: float = 5.0) -> None:
    async with asyncio.timeout(timeout_seconds):
        while True:
            try:
                _, writer = await asyncio.open_connection(host, port)
                writer.close()
                await writer.wait_closed()
                return
            except OSError:
                await asyncio.sleep(0.02)


class TestDirectModeMtlsRealHandshake:
    """Proves the actual TLS socket + TlsAwareHttpToolsProtocol enforce
    what the design claims -- against this repo's own real HTTPGateway,
    directly, not just downstream in a consumer."""

    async def test_trusted_client_cert_allowlisted_dn_reaches_the_app(
        self, monkeypatch: pytest.MonkeyPatch, tls_certs: SimpleNamespace
    ) -> None:
        monkeypatch.setattr(
            "civitas.gateway.mtls.settings",
            Settings(env={"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS": tls_certs.client_dn}),
        )
        port = _free_port()
        worker = EchoAgent("worker")
        gw = HTTPGateway(
            "api",
            GatewayConfig(
                host="127.0.0.1",
                port=port,
                tls_cert=tls_certs.server_cert_path,
                tls_key=tls_certs.server_key_path,
                tls_ca_cert=tls_certs.ca_path,
                client_cert_mode="required",
                middleware=["civitas.gateway.mtls.require_client_cert"],
                routes=[{"method": "POST", "path": "/echo", "agent": "worker", "mode": "call"}],
            ),
        )
        rt = Runtime(supervisor=Supervisor("root", children=[gw, worker]))
        await rt.start()
        try:
            await _wait_for_port_open("127.0.0.1", port)
            await asyncio.sleep(0.05)
            async with httpx.AsyncClient(
                verify=_client_ssl_context(
                    tls_certs.ca_path, tls_certs.client_cert_path, tls_certs.client_key_path
                ),
            ) as client:
                resp = await client.post(
                    f"https://127.0.0.1:{port}/echo", json={"hello": "world"}, timeout=5.0
                )
            assert resp.status_code == 200
            assert resp.json()["echo"] == {"hello": "world"}
        finally:
            await rt.stop()

    async def test_same_ca_but_dn_not_allowlisted_gets_403(
        self, monkeypatch: pytest.MonkeyPatch, tls_certs: SimpleNamespace
    ) -> None:
        """The TLS handshake itself succeeds (signed by the trusted CA) --
        the app-layer DN allowlist is what rejects this, proving the two
        layers are both real and distinct."""
        monkeypatch.setattr(
            "civitas.gateway.mtls.settings",
            Settings(env={"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS": tls_certs.client_dn}),
        )
        port = _free_port()
        worker = EchoAgent("worker")
        gw = HTTPGateway(
            "api",
            GatewayConfig(
                host="127.0.0.1",
                port=port,
                tls_cert=tls_certs.server_cert_path,
                tls_key=tls_certs.server_key_path,
                tls_ca_cert=tls_certs.ca_path,
                client_cert_mode="required",
                middleware=["civitas.gateway.mtls.require_client_cert"],
                routes=[{"method": "POST", "path": "/echo", "agent": "worker", "mode": "call"}],
            ),
        )
        rt = Runtime(supervisor=Supervisor("root", children=[gw, worker]))
        await rt.start()
        try:
            await _wait_for_port_open("127.0.0.1", port)
            await asyncio.sleep(0.05)
            async with httpx.AsyncClient(
                verify=_client_ssl_context(
                    tls_certs.ca_path, tls_certs.intruder_cert_path, tls_certs.intruder_key_path
                ),
            ) as client:
                resp = await client.post(
                    f"https://127.0.0.1:{port}/echo", json={"hello": "world"}, timeout=5.0
                )
            assert resp.status_code == 403
        finally:
            await rt.stop()

    async def test_no_client_cert_fails_the_tls_handshake(
        self, monkeypatch: pytest.MonkeyPatch, tls_certs: SimpleNamespace
    ) -> None:
        monkeypatch.setattr(
            "civitas.gateway.mtls.settings",
            Settings(env={"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS": tls_certs.client_dn}),
        )
        port = _free_port()
        worker = EchoAgent("worker")
        gw = HTTPGateway(
            "api",
            GatewayConfig(
                host="127.0.0.1",
                port=port,
                tls_cert=tls_certs.server_cert_path,
                tls_key=tls_certs.server_key_path,
                tls_ca_cert=tls_certs.ca_path,
                client_cert_mode="required",
                middleware=["civitas.gateway.mtls.require_client_cert"],
                routes=[{"method": "POST", "path": "/echo", "agent": "worker", "mode": "call"}],
            ),
        )
        rt = Runtime(supervisor=Supervisor("root", children=[gw, worker]))
        await rt.start()
        try:
            await _wait_for_port_open("127.0.0.1", port)
            await asyncio.sleep(0.05)
            async with httpx.AsyncClient(verify=_client_ssl_context(tls_certs.ca_path)) as client:
                with pytest.raises((httpx.ConnectError, httpx.ReadError, ssl.SSLError)):
                    await client.get(f"https://127.0.0.1:{port}/echo", timeout=5.0)
        finally:
            await rt.stop()

    async def test_cert_from_an_untrusted_ca_fails_the_tls_handshake(
        self, monkeypatch: pytest.MonkeyPatch, tls_certs: SimpleNamespace
    ) -> None:
        monkeypatch.setattr(
            "civitas.gateway.mtls.settings",
            Settings(env={"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS": tls_certs.client_dn}),
        )
        port = _free_port()
        worker = EchoAgent("worker")
        gw = HTTPGateway(
            "api",
            GatewayConfig(
                host="127.0.0.1",
                port=port,
                tls_cert=tls_certs.server_cert_path,
                tls_key=tls_certs.server_key_path,
                tls_ca_cert=tls_certs.ca_path,
                client_cert_mode="required",
                middleware=["civitas.gateway.mtls.require_client_cert"],
                routes=[{"method": "POST", "path": "/echo", "agent": "worker", "mode": "call"}],
            ),
        )
        rt = Runtime(supervisor=Supervisor("root", children=[gw, worker]))
        await rt.start()
        try:
            await _wait_for_port_open("127.0.0.1", port)
            await asyncio.sleep(0.05)
            async with httpx.AsyncClient(
                verify=_client_ssl_context(
                    tls_certs.ca_path, tls_certs.outsider_cert_path, tls_certs.outsider_key_path
                ),
            ) as client:
                with pytest.raises((httpx.ConnectError, httpx.ReadError, ssl.SSLError)):
                    await client.get(f"https://127.0.0.1:{port}/echo", timeout=5.0)
        finally:
            await rt.stop()


class TestDirectModeMtlsDoesNotAffectOtherModes:
    """D2 regression coverage: a plaintext gateway is completely unaffected
    by TlsAwareHttpToolsProtocol existing -- it's only wired in for
    client_cert_mode != "none" and mtls_source == "direct"."""

    async def test_plaintext_gateway_unaffected(self) -> None:
        port = _free_port()
        worker = EchoAgent("worker")
        gw = HTTPGateway(
            "api",
            GatewayConfig(
                host="127.0.0.1",
                port=port,
                routes=[{"method": "POST", "path": "/echo", "agent": "worker", "mode": "call"}],
            ),
        )
        rt = Runtime(supervisor=Supervisor("root", children=[gw, worker]))
        await rt.start()
        try:
            await _wait_for_port_open("127.0.0.1", port)
            await asyncio.sleep(0.05)
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"http://127.0.0.1:{port}/echo", json={"hello": "world"}, timeout=5.0
                )
            assert resp.status_code == 200
        finally:
            await rt.stop()
