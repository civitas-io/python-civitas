"""HTTP/3 loopback smoke — a real QUIC GET through H3Server (V6, #43).

The proof #43 demanded: HTTP/3 was advertised since M4.x but no test anywhere
had ever driven a request through it — and the event handler contained an
ImportError (`StreamReset` is a QUIC event, not an H3 event) that fired on
first use with every aioquic in the declared >=1.0 range.
"""

from __future__ import annotations

import asyncio
import datetime
import ipaddress
import socket
import ssl

import pytest

pytest.importorskip("aioquic", reason="aioquic not installed")

from aioquic.asyncio import connect  # noqa: E402
from aioquic.asyncio.protocol import QuicConnectionProtocol  # noqa: E402
from aioquic.h3.connection import H3_ALPN, H3Connection  # noqa: E402
from aioquic.h3.events import DataReceived, HeadersReceived  # noqa: E402
from aioquic.quic.configuration import QuicConfiguration  # noqa: E402

from civitas.gateway.h3 import H3Server  # noqa: E402


def _self_signed(tmp_path):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    certfile, keyfile = tmp_path / "cert.pem", tmp_path / "key.pem"
    certfile.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    keyfile.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return str(certfile), str(keyfile)


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _H3Client(QuicConnectionProtocol):
    """Minimal HTTP/3 client: one GET, collect status + body."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http: H3Connection | None = None
        self.status: int | None = None
        self.body = b""
        self.done: asyncio.Future[bool] = asyncio.get_event_loop().create_future()

    def quic_event_received(self, event):
        if self.http is None:
            return
        for ev in self.http.handle_event(event):
            if isinstance(ev, HeadersReceived):
                self.status = int(dict(ev.headers)[b":status"])
            elif isinstance(ev, DataReceived):
                self.body += ev.data
                if ev.stream_ended and not self.done.done():
                    self.done.set_result(True)


async def test_h3_get_roundtrip(tmp_path):
    """QUIC handshake → HTTP/3 GET → ASGI app → HTTP/3 response, end to end."""
    certfile, keyfile = _self_signed(tmp_path)
    port = _free_udp_port()
    seen_scopes: list[dict] = []

    async def mini_app(scope, receive, send):
        seen_scopes.append(scope)
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"pong-over-quic"})

    server = H3Server(
        asgi_app=mini_app,  # type: ignore[arg-type]  # duck-typed ASGI callable
        host="127.0.0.1",
        port=port,
        certfile=certfile,
        keyfile=keyfile,
    )
    await server.start()
    try:
        config = QuicConfiguration(alpn_protocols=H3_ALPN, is_client=True)
        config.verify_mode = ssl.CERT_NONE  # self-signed

        async with connect(
            "127.0.0.1", port, configuration=config, create_protocol=_H3Client
        ) as client:
            assert isinstance(client, _H3Client)
            client.http = H3Connection(client._quic)
            stream_id = client._quic.get_next_available_stream_id()
            client.http.send_headers(
                stream_id=stream_id,
                headers=[
                    (b":method", b"GET"),
                    (b":scheme", b"https"),
                    (b":authority", b"localhost"),
                    (b":path", b"/ping?q=1"),
                ],
                end_stream=True,
            )
            client.transmit()
            async with asyncio.timeout(5.0):
                await client.done

            assert client.status == 200
            assert client.body == b"pong-over-quic"

        # The server adapted the H3 stream into a faithful ASGI scope.
        scope = seen_scopes[0]
        assert scope["http_version"] == "3"
        assert scope["method"] == "GET"
        assert scope["path"] == "/ping"
        assert scope["query_string"] == b"q=1"
    finally:
        await server.stop()
