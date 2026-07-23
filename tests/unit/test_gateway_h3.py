"""Unit tests for the HTTP/3 / QUIC gateway server (V6, #43).

h3.py shipped untested since M4.x ("aioquic not installed in the dev
environment") — aioquic is now a dev dependency. The stream-to-ASGI adapter
and server lifecycle are unit-tested here; the real-QUIC loopback proof lives
in tests/integration/test_h3_gateway.py.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("aioquic", reason="aioquic not installed")

from aioquic.h3.events import DataReceived  # noqa: E402
from aioquic.quic.events import StreamReset  # noqa: E402

from civitas.gateway.h3 import H3Server, _H3RequestHandler, _make_protocol_factory  # noqa: E402

# ---------------------------------------------------------------------------
# _H3RequestHandler — the HTTP/3-stream ↔ ASGI adapter
# ---------------------------------------------------------------------------


def _handler(**overrides: Any) -> tuple[_H3RequestHandler, MagicMock, MagicMock]:
    connection = MagicMock()
    transmit = MagicMock()
    handler = _H3RequestHandler(
        connection=connection,
        stream_id=overrides.get("stream_id", 4),
        scope={"type": "http", "method": "GET", "path": "/"},
        transmit=transmit,
    )
    return handler, connection, transmit


async def test_data_received_becomes_http_request_event():
    handler, _, _ = _handler()
    handler.h3_event_received(DataReceived(data=b"chunk", stream_id=4, stream_ended=False))
    handler.h3_event_received(DataReceived(data=b"end", stream_id=4, stream_ended=True))

    first = await handler.receive()
    assert first == {"type": "http.request", "body": b"chunk", "more_body": True}
    second = await handler.receive()
    assert second == {"type": "http.request", "body": b"end", "more_body": False}


async def test_stream_reset_becomes_disconnect():
    handler, _, _ = _handler()
    handler.h3_event_received(StreamReset(error_code=0, stream_id=4))
    assert await handler.receive() == {"type": "http.disconnect"}


async def test_send_response_start_maps_status_and_headers():
    handler, connection, transmit = _handler()
    await handler.send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    connection.send_headers.assert_called_once()
    kwargs = connection.send_headers.call_args.kwargs
    assert kwargs["stream_id"] == 4
    assert (b":status", b"200") in kwargs["headers"]
    assert (b"content-type", b"application/json") in kwargs["headers"]
    transmit.assert_called_once()


async def test_send_response_body_end_stream_semantics():
    handler, connection, transmit = _handler()
    await handler.send({"type": "http.response.body", "body": b"partial", "more_body": True})
    await handler.send({"type": "http.response.body", "body": b"final"})

    calls = connection.send_data.call_args_list
    assert calls[0].kwargs == {"stream_id": 4, "data": b"partial", "end_stream": False}
    assert calls[1].kwargs == {"stream_id": 4, "data": b"final", "end_stream": True}
    assert transmit.call_count == 2


async def test_run_contains_app_exceptions():
    """A crashing ASGI app must not propagate out of the stream handler."""
    handler, _, _ = _handler()

    async def exploding_app(scope: Any, receive: Any, send: Any) -> None:
        raise RuntimeError("app blew up")

    await handler.run(exploding_app)  # type: ignore[arg-type]  # must not raise


# ---------------------------------------------------------------------------
# H3Server lifecycle
# ---------------------------------------------------------------------------


async def test_server_start_configures_and_serves(tls_cert_pair):
    certfile, keyfile = tls_cert_pair
    served: dict[str, Any] = {}

    async def fake_serve(host: str, port: int, *, configuration: Any, create_protocol: Any):
        served.update(host=host, port=port, configuration=configuration)
        return MagicMock()

    app = MagicMock()
    server = H3Server(asgi_app=app, host="127.0.0.1", port=4433, certfile=certfile, keyfile=keyfile)
    with patch("aioquic.asyncio.server.serve", side_effect=fake_serve):
        await server.start()

    assert served["host"] == "127.0.0.1" and served["port"] == 4433
    assert "h3" in served["configuration"].alpn_protocols
    assert served["configuration"].is_client is False
    assert served["configuration"].certificate is not None  # cert chain actually loaded

    await server.stop()
    assert server._server is None


async def test_server_stop_without_start_is_noop():
    server = H3Server(asgi_app=MagicMock(), host="127.0.0.1", port=4433, certfile="x", keyfile="y")
    await server.stop()  # must not raise


def test_protocol_factory_returns_quic_protocol_subclass():
    from aioquic.asyncio.protocol import QuicConnectionProtocol

    factory = _make_protocol_factory(MagicMock())
    assert issubclass(factory, QuicConnectionProtocol)


# ---------------------------------------------------------------------------
# Shared fixture: throwaway self-signed cert
# ---------------------------------------------------------------------------


@pytest.fixture
def tls_cert_pair(tmp_path):
    """Self-signed localhost cert for QUIC configuration tests."""
    import datetime
    import ipaddress

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
    certfile = tmp_path / "cert.pem"
    keyfile = tmp_path / "key.pem"
    certfile.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    keyfile.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return str(certfile), str(keyfile)


# Quiet the unused-import style for asyncio (kept for parity with module under test)
_ = asyncio
