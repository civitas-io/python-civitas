"""Tests for WS/gRPC gateway auth (#17): WS JWT, gRPC JWT + mTLS, and D9-D11 guards.

Reuses the JWT fixture pattern from ``test_gateway_auth.py`` (``rsa_keys`` + claim
helpers), the real-server WS pattern from ``test_gateway_streaming.py``, and the
real-server gRPC pattern from ``test_grpc_gateway.py`` (extended with a TLS/mTLS
variant via ``grpc.aio.secure_channel`` and client credentials).
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import json
import logging
import socket
import time
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import grpc
import jwt as pyjwt
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

from civitas import AgentProcess, Runtime, Supervisor
from civitas.config import Settings
from civitas.errors import ConfigurationError
from civitas.gateway.asgi import GatewayASGI
from civitas.gateway.core import GatewayConfig, HTTPGateway
from civitas.gateway.dispatch import GatewayDispatcher, StreamSink
from civitas.gateway.grpc_server import GrpcServer, _dict_to_struct, _struct_to_dict
from civitas.gateway.jwt_auth import _JWT_MIDDLEWARE_PATH, JwtVerifier
from civitas.gateway.mtls import (
    _check_dn,
    _dn_from_pem,
    _Forbidden,
    _MtlsMisconfigured,
    _NoCertificate,
    require_client_cert,
)
from civitas.gateway.proto import civitas_pb2, civitas_pb2_grpc
from civitas.gateway.router import RouteTable
from civitas.gateway.types import GatewayRequest, GatewayResponse
from civitas.messages import Message

_AUD = "civitas-api"
_ISS = "https://idp.example"
_WS_ROUTES = [{"path": "/ws/echo", "agent": "wsecho"}]

# ---------------------------------------------------------------------------
# JWT helpers (mirrors test_gateway_auth.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rsa_keys() -> tuple[str, str]:
    """A single RSA keypair (private_pem, public_pem) shared across the module."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    pub = (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    return priv, pub


def _claims(**over: Any) -> dict[str, Any]:
    now = int(time.time())
    base: dict[str, Any] = {
        "sub": "user-1",
        "aud": _AUD,
        "iss": _ISS,
        "iat": now,
        "exp": now + 3600,
    }
    base.update(over)
    return {k: v for k, v in base.items() if v is not None}


def _static_verifier(pub: str) -> JwtVerifier:
    return JwtVerifier(jwks_url=None, public_key=pub, secret=None, audience=_AUD, issuer=_ISS)


def _token(priv: str, **over: Any) -> str:
    return pyjwt.encode(_claims(**over), priv, algorithm="RS256")


def _jwt_settings(pub: str) -> Settings:
    return Settings(
        env={
            "CIVITAS_JWT_PUBLIC_KEY": pub,
            "CIVITAS_JWT_AUDIENCE": _AUD,
            "CIVITAS_JWT_ISSUER": _ISS,
        }
    )


# ---------------------------------------------------------------------------
# Certificate helpers (self-signed CA + leaves for the mTLS test cases)
# ---------------------------------------------------------------------------


def _rsa_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _key_pem(key: rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )


def _make_ca() -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = _rsa_key()
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
    ca_key: rsa.RSAPrivateKey,
    ca_cert: x509.Certificate,
    common_name: str,
    *,
    san: str | None = "localhost",
) -> tuple[bytes, bytes]:
    key = _rsa_key()
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Acme"),
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
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
            x509.SubjectAlternativeName([x509.DNSName(san)]), critical=False
        )
    cert = builder.sign(ca_key, hashes.SHA256())
    return _key_pem(key), cert.public_bytes(serialization.Encoding.PEM)


@pytest.fixture(scope="module")
def tls_certs(tmp_path_factory: pytest.TempPathFactory) -> SimpleNamespace:
    """A CA plus server/client leaves; server PEMs are written to files for GrpcServer."""
    ca_key, ca_cert = _make_ca()
    ca_pem = ca_cert.public_bytes(serialization.Encoding.PEM)
    server_key, server_cert = _make_leaf(ca_key, ca_cert, "localhost", san="localhost")
    client_key, client_cert = _make_leaf(ca_key, ca_cert, "service", san=None)
    intruder_key, intruder_cert = _make_leaf(ca_key, ca_cert, "intruder", san=None)

    directory = tmp_path_factory.mktemp("tls")
    ca_path = directory / "ca.pem"
    ca_path.write_bytes(ca_pem)
    cert_path = directory / "server.pem"
    cert_path.write_bytes(server_cert)
    key_path = directory / "server.key"
    key_path.write_bytes(server_key)

    return SimpleNamespace(
        ca_pem=ca_pem,
        ca_path=str(ca_path),
        cert_path=str(cert_path),
        key_path=str(key_path),
        client_key=client_key,
        client_cert=client_cert,
        client_dn=_dn_from_pem(client_cert),
        intruder_key=intruder_key,
        intruder_cert=intruder_cert,
    )


# ---------------------------------------------------------------------------
# gRPC helpers (real GrpcServer + real channel, extended for TLS/mTLS)
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _grpc_gateway() -> MagicMock:
    gateway = MagicMock(spec=HTTPGateway)
    gateway.name = "api"
    return gateway


def _agent_request(payload: dict[str, Any] | None = None) -> civitas_pb2.AgentRequest:
    return civitas_pb2.AgentRequest(
        recipient="echo", type="test", payload=_dict_to_struct(payload or {})
    )


def _reply(payload: dict[str, Any]) -> MagicMock:
    reply = MagicMock(spec=Message)
    reply.payload = payload
    return reply


@contextlib.asynccontextmanager
async def _running_grpc(gateway: Any, **kwargs: Any) -> AsyncIterator[int]:
    port = _free_port()
    server = GrpcServer(
        GatewayDispatcher(gateway, request_timeout=5.0), "127.0.0.1", port, **kwargs
    )
    await server.start()
    try:
        yield port
    finally:
        await server.stop()


def _mtls_channel(port: int, certs: SimpleNamespace, key: bytes, cert: bytes) -> grpc.aio.Channel:
    creds = grpc.ssl_channel_credentials(
        root_certificates=certs.ca_pem, private_key=key, certificate_chain=cert
    )
    return grpc.aio.secure_channel(
        f"127.0.0.1:{port}", creds, options=(("grpc.ssl_target_name_override", "localhost"),)
    )


# ---------------------------------------------------------------------------
# WebSocket helpers (ASGI-level driver + real echo agent)
# ---------------------------------------------------------------------------


def _ws_asgi(verifier: JwtVerifier | None) -> GatewayASGI:
    gateway = MagicMock(spec=HTTPGateway)
    gateway.name = "api"
    gateway._jwt_verifier = verifier
    gateway._open_stream = MagicMock(return_value=StreamSink(8))
    gateway._close_stream = MagicMock()
    gateway._send_stream_request = AsyncMock()
    config = GatewayConfig(ws_routes=list(_WS_ROUTES))
    return GatewayASGI(gateway=gateway, route_table=RouteTable.from_config([]), config=config)


async def _drive_ws(
    asgi: GatewayASGI,
    *,
    subprotocols: list[str],
    path: str = "/ws/echo",
    disconnect_after: bool = False,
) -> list[dict[str, Any]]:
    scope: dict[str, Any] = {"type": "websocket", "path": path, "subprotocols": subprotocols}
    events: list[dict[str, Any]] = [{"type": "websocket.connect"}]
    if disconnect_after:
        events.append({"type": "websocket.disconnect"})
    incoming = iter(events)

    async def receive() -> dict[str, Any]:
        try:
            return next(incoming)
        except StopIteration:
            return {"type": "websocket.disconnect"}

    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await asgi(scope, receive, send)
    return sent


def _patch_uvicorn(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace uvicorn.Config/Server so on_start never binds a real socket."""
    import uvicorn

    def _fake_config(**kw: Any) -> dict[str, Any]:
        return kw

    class _FakeServer:
        def __init__(self, cfg: Any) -> None:
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

    monkeypatch.setattr(uvicorn, "Config", _fake_config)
    monkeypatch.setattr(uvicorn, "Server", _FakeServer)


class _WsEchoAgent(AgentProcess):
    async def handle(self, message: Message) -> None:
        if message.type == "ws.close":
            return
        await self.emit({"echo": message.payload.get("text", "")})


# ---------------------------------------------------------------------------
# T1-T5 — WebSocket JWT enforcement (D1/D2/D6/D11)
# ---------------------------------------------------------------------------


class TestWebSocketAuth:
    @pytest.mark.asyncio
    async def test_t1_valid_token_accepts_and_echoes_subprotocol(
        self, monkeypatch: pytest.MonkeyPatch, rsa_keys: tuple[str, str]
    ) -> None:
        websockets = pytest.importorskip("websockets")
        priv, pub = rsa_keys
        monkeypatch.setattr("civitas.gateway.core.settings", _jwt_settings(pub))
        port = _free_port()
        subprotocol = f"civitas.bearer.{_token(priv)}"
        config = GatewayConfig(
            port=port, middleware=[_JWT_MIDDLEWARE_PATH], ws_routes=list(_WS_ROUTES)
        )
        runtime = Runtime(
            supervisor=Supervisor(
                "root", children=[HTTPGateway("api", config), _WsEchoAgent("wsecho")]
            )
        )
        await runtime.start()
        await asyncio.sleep(0.2)
        try:
            async with websockets.connect(
                f"ws://127.0.0.1:{port}/ws/echo", subprotocols=[subprotocol]
            ) as ws:
                assert ws.subprotocol == subprotocol
                await ws.send(json.dumps({"text": "hello"}))
                reply = await asyncio.wait_for(ws.recv(), timeout=5.0)
            assert json.loads(reply) == {"echo": "hello"}
        finally:
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_t2_invalid_token_closes_4401(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        asgi = _ws_asgi(_static_verifier(pub))
        expired = _token(priv, exp=int(time.time()) - 3600)
        sent = await _drive_ws(asgi, subprotocols=[f"civitas.bearer.{expired}"])
        assert sent == [{"type": "websocket.close", "code": 4401}]

    @pytest.mark.asyncio
    async def test_t3_no_subprotocol_closes_4401(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        asgi = _ws_asgi(_static_verifier(pub))
        sent = await _drive_ws(asgi, subprotocols=[])
        assert sent == [{"type": "websocket.close", "code": 4401}]

    @pytest.mark.asyncio
    async def test_t4_no_verifier_stays_open(self) -> None:
        asgi = _ws_asgi(None)
        sent = await _drive_ws(asgi, subprotocols=[], disconnect_after=True)
        assert sent[0] == {"type": "websocket.accept"}
        assert all(message.get("type") != "websocket.close" for message in sent)

    @pytest.mark.asyncio
    async def test_t5_mtls_only_ws_routes_warns_and_stays_open(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        _patch_uvicorn(monkeypatch)
        gateway = HTTPGateway(
            "api",
            GatewayConfig(
                port=_free_port(),
                client_cert_mode="required",
                tls_ca_cert="ca.pem",
                tls_cert="c.pem",
                tls_key="k.pem",
                ws_routes=list(_WS_ROUTES),
            ),
        )
        gateway._bus = None
        with caplog.at_level(logging.WARNING):
            await gateway.on_start()
        try:
            assert any("no JWT verifier is configured" in r.getMessage() for r in caplog.records)
            sent = await _drive_ws(_ws_asgi(None), subprotocols=[], disconnect_after=True)
            assert sent[0] == {"type": "websocket.accept"}
        finally:
            await gateway.on_stop()


# ---------------------------------------------------------------------------
# T6-T10 — gRPC interceptor: JWT metadata + mTLS transport (D3/D4/D8, F4/F6)
# ---------------------------------------------------------------------------


class TestGrpcAuth:
    @pytest.mark.asyncio
    async def test_t6_jwt_metadata_enforced(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        gateway = _grpc_gateway()
        gateway.ask = AsyncMock(return_value=_reply({"answer": "ok"}))
        async with _running_grpc(gateway, jwt_verifier=_static_verifier(pub)) as port:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = civitas_pb2_grpc.AgentStub(channel)
                with pytest.raises(grpc.aio.AioRpcError) as missing:
                    await stub.Invoke(_agent_request())
                assert missing.value.code() == grpc.StatusCode.UNAUTHENTICATED
                with pytest.raises(grpc.aio.AioRpcError) as invalid:
                    await stub.Invoke(
                        _agent_request(), metadata=(("authorization", "Bearer not.a.jwt"),)
                    )
                assert invalid.value.code() == grpc.StatusCode.UNAUTHENTICATED
                reply = await stub.Invoke(
                    _agent_request(), metadata=(("authorization", f"Bearer {_token(priv)}"),)
                )
                assert _struct_to_dict(reply.payload) == {"answer": "ok"}

    @pytest.mark.asyncio
    async def test_t7_health_reflection_exempt_when_jwt_mtls_on(
        self,
        monkeypatch: pytest.MonkeyPatch,
        rsa_keys: tuple[str, str],
        tls_certs: SimpleNamespace,
    ) -> None:
        _, pub = rsa_keys
        # Allowlist deliberately excludes the client DN: health/reflection must still
        # work (they skip both the JWT and the mTLS-DN check).
        monkeypatch.setattr(
            "civitas.gateway.grpc_server.settings",
            Settings(env={"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS": "CN=not-the-client"}),
        )
        gateway = _grpc_gateway()
        gateway.ask = AsyncMock(return_value=_reply({"answer": "ok"}))
        async with _running_grpc(
            gateway,
            jwt_verifier=_static_verifier(pub),
            tls_ca_cert=tls_certs.ca_path,
            tls_cert=tls_certs.cert_path,
            tls_key=tls_certs.key_path,
            client_cert_mode="required",
        ) as port:
            async with _mtls_channel(
                port, tls_certs, tls_certs.client_key, tls_certs.client_cert
            ) as channel:
                health = await health_pb2_grpc.HealthStub(channel).Check(
                    health_pb2.HealthCheckRequest()
                )
                assert health.status == health_pb2.HealthCheckResponse.SERVING

                reflection_stub = reflection_pb2_grpc.ServerReflectionStub(channel)

                async def _requests() -> AsyncIterator[reflection_pb2.ServerReflectionRequest]:
                    yield reflection_pb2.ServerReflectionRequest(list_services="*")

                services: list[Any] = []
                async for resp in reflection_stub.ServerReflectionInfo(_requests()):
                    services = list(resp.list_services_response.service)
                    break
                assert services

                with pytest.raises(grpc.aio.AioRpcError) as exc:
                    await civitas_pb2_grpc.AgentStub(channel).Invoke(_agent_request())
                assert exc.value.code() == grpc.StatusCode.UNAUTHENTICATED

    @pytest.mark.asyncio
    async def test_t8_mtls_dn_allowlist_enforced(
        self, monkeypatch: pytest.MonkeyPatch, tls_certs: SimpleNamespace
    ) -> None:
        monkeypatch.setattr(
            "civitas.gateway.grpc_server.settings",
            Settings(env={"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS": tls_certs.client_dn}),
        )
        gateway = _grpc_gateway()
        gateway.ask = AsyncMock(return_value=_reply({"answer": "ok"}))
        stream_sink = StreamSink(8)
        stream_sink.push({"v": "x"})
        stream_sink.end()
        gateway._open_stream = MagicMock(return_value=stream_sink)
        gateway._send_stream_request = AsyncMock()
        gateway._close_stream = MagicMock()
        async with _running_grpc(
            gateway,
            tls_ca_cert=tls_certs.ca_path,
            tls_cert=tls_certs.cert_path,
            tls_key=tls_certs.key_path,
            client_cert_mode="required",
        ) as port:
            async with _mtls_channel(
                port, tls_certs, tls_certs.client_key, tls_certs.client_cert
            ) as channel:
                stub = civitas_pb2_grpc.AgentStub(channel)
                reply = await stub.Invoke(_agent_request())
                assert _struct_to_dict(reply.payload) == {"answer": "ok"}
                chunks = [_struct_to_dict(r.payload) async for r in stub.Stream(_agent_request())]
                assert chunks == [{"v": "x"}]
            async with _mtls_channel(
                port, tls_certs, tls_certs.intruder_key, tls_certs.intruder_cert
            ) as channel:
                stub = civitas_pb2_grpc.AgentStub(channel)
                with pytest.raises(grpc.aio.AioRpcError) as exc:
                    await stub.Invoke(_agent_request())
                assert exc.value.code() == grpc.StatusCode.PERMISSION_DENIED

    @pytest.mark.asyncio
    async def test_t9_mtls_no_client_cert_never_completes(
        self, monkeypatch: pytest.MonkeyPatch, tls_certs: SimpleNamespace
    ) -> None:
        monkeypatch.setattr(
            "civitas.gateway.grpc_server.settings",
            Settings(env={"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS": tls_certs.client_dn}),
        )
        gateway = _grpc_gateway()
        gateway.ask = AsyncMock(return_value=_reply({"answer": "ok"}))
        async with _running_grpc(
            gateway,
            tls_ca_cert=tls_certs.ca_path,
            tls_cert=tls_certs.cert_path,
            tls_key=tls_certs.key_path,
            client_cert_mode="required",
        ) as port:
            # A certless client under require_client_auth=True fails at the TLS layer:
            # assert non-completion (Constraint 4), not a specific gRPC status.
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = civitas_pb2_grpc.AgentStub(channel)
                with pytest.raises(grpc.aio.AioRpcError):
                    await stub.Invoke(_agent_request(), timeout=3.0)

    @pytest.mark.asyncio
    async def test_t10_mtls_empty_allowlist_internal(
        self, monkeypatch: pytest.MonkeyPatch, tls_certs: SimpleNamespace
    ) -> None:
        monkeypatch.setattr("civitas.gateway.grpc_server.settings", Settings(env={}))
        gateway = _grpc_gateway()
        gateway.ask = AsyncMock(return_value=_reply({"answer": "ok"}))
        async with _running_grpc(
            gateway,
            tls_ca_cert=tls_certs.ca_path,
            tls_cert=tls_certs.cert_path,
            tls_key=tls_certs.key_path,
            client_cert_mode="required",
        ) as port:
            async with _mtls_channel(
                port, tls_certs, tls_certs.client_key, tls_certs.client_cert
            ) as channel:
                stub = civitas_pb2_grpc.AgentStub(channel)
                with pytest.raises(grpc.aio.AioRpcError) as exc:
                    await stub.Invoke(_agent_request())
                assert exc.value.code() == grpc.StatusCode.INTERNAL


# ---------------------------------------------------------------------------
# T11-T12 — startup validations (D9 config-time, D10 on_start)
# ---------------------------------------------------------------------------


class TestStartupValidations:
    def test_t11_optional_mode_with_grpc_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="grpc_enabled"):
            GatewayConfig(
                client_cert_mode="optional",
                tls_ca_cert="ca.pem",
                tls_cert="c.pem",
                tls_key="k.pem",
                grpc_enabled=True,
                grpc_port=50051,
            )
        # HTTP-only optional mTLS is unaffected.
        cfg = GatewayConfig(
            client_cert_mode="optional", tls_ca_cert="ca.pem", tls_cert="c.pem", tls_key="k.pem"
        )
        assert cfg.client_cert_mode == "optional"

    @pytest.mark.asyncio
    async def test_t12_jwt_over_insecure_grpc_rejected(
        self, monkeypatch: pytest.MonkeyPatch, rsa_keys: tuple[str, str]
    ) -> None:
        _, pub = rsa_keys
        _patch_uvicorn(monkeypatch)
        monkeypatch.setattr("civitas.gateway.core.settings", _jwt_settings(pub))
        gateway = HTTPGateway(
            "api",
            GatewayConfig(
                port=_free_port(),
                middleware=[_JWT_MIDDLEWARE_PATH],
                grpc_enabled=True,
                grpc_port=_free_port(),
            ),
        )
        gateway._bus = None
        with pytest.raises(ConfigurationError, match="insecure"):
            await gateway.on_start()


# ---------------------------------------------------------------------------
# T13-T14 — shared mTLS primitives (F2 / D5)
# ---------------------------------------------------------------------------


class TestMtlsPrimitives:
    def test_t13_dn_from_pem_is_stable_across_call_sites(self, tls_certs: SimpleNamespace) -> None:
        first = _dn_from_pem(tls_certs.client_cert)
        second = _dn_from_pem(tls_certs.client_cert)
        assert first == second
        assert first == tls_certs.client_dn
        expected = x509.load_pem_x509_certificate(tls_certs.client_cert).subject.rfc4514_string()
        assert first == expected

    def test_t14_check_dn_outcomes(self) -> None:
        allowed = frozenset({"CN=service,O=Acme,C=US"})
        with pytest.raises(_MtlsMisconfigured):
            _check_dn("CN=service,O=Acme,C=US", frozenset())
        with pytest.raises(_NoCertificate):
            _check_dn(None, allowed)
        with pytest.raises(_Forbidden) as exc:
            _check_dn("CN=other", allowed)
        assert exc.value.dn == "CN=other"
        assert _check_dn("CN=service,O=Acme,C=US", allowed) is None


# ---------------------------------------------------------------------------
# T15-T16 — regression guards (Constraint 7, D6)
# ---------------------------------------------------------------------------


class TestRegressionGuards:
    @pytest.mark.asyncio
    async def test_t15_require_client_cert_behavior_preserved(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def call_next(_: GatewayRequest) -> GatewayResponse:
            return GatewayResponse(200, {"ok": True})

        dn = "CN=service,O=Acme,C=US"
        monkeypatch.setattr("civitas.gateway.mtls.settings", Settings(env={}))
        resp = await require_client_cert(
            GatewayRequest(method="GET", path="/x", client_cert={"dn": dn}), call_next
        )
        assert resp.status == 500

        monkeypatch.setattr(
            "civitas.gateway.mtls.settings",
            Settings(env={"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS": dn}),
        )
        resp = await require_client_cert(
            GatewayRequest(method="GET", path="/x", client_cert=None), call_next
        )
        assert resp.status == 401
        resp = await require_client_cert(
            GatewayRequest(method="GET", path="/x", client_cert={"dn": "CN=evil"}), call_next
        )
        assert resp.status == 403
        resp = await require_client_cert(
            GatewayRequest(method="GET", path="/x", client_cert={"dn": dn}), call_next
        )
        assert resp.status == 200

    @pytest.mark.asyncio
    async def test_t16_no_auth_grpc_and_ws_unchanged(self) -> None:
        gateway = _grpc_gateway()
        gateway.ask = AsyncMock(return_value=_reply({"answer": "ok"}))
        async with _running_grpc(gateway) as port:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                reply = await civitas_pb2_grpc.AgentStub(channel).Invoke(_agent_request())
                assert _struct_to_dict(reply.payload) == {"answer": "ok"}

        sent = await _drive_ws(_ws_asgi(None), subprotocols=[], disconnect_after=True)
        assert sent[0] == {"type": "websocket.accept"}
