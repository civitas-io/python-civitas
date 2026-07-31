"""Tests for gateway auth (v0.7.0 R3): M1 fatal load, JWT, mTLS, docs gating, compose."""

from __future__ import annotations

import asyncio
import json
import sys
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from civitas.config import Settings
from civitas.errors import ConfigurationError
from civitas.gateway.asgi import GatewayASGI, _client_cert_from_scope
from civitas.gateway.core import _CERT_REQS, GatewayConfig, HTTPGateway
from civitas.gateway.jwt_auth import _JWT_MIDDLEWARE_PATH, JwtVerifier, require_jwt
from civitas.gateway.mtls import require_client_cert
from civitas.gateway.router import RouteTable
from civitas.gateway.types import GatewayRequest, GatewayResponse
from civitas.messages import Message
from civitas.runtime import Runtime
from civitas.security.config import GatewayAuthConfig

_AUD = "civitas-api"
_ISS = "https://idp.example"
_JWKS_URL = "https://idp.example/.well-known/jwks.json"
_DN = "CN=service,O=Acme,C=US"
_MTLS_PATH = "civitas.gateway.mtls.require_client_cert"

# ---------------------------------------------------------------------------
# Shared fixtures / helpers
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


def _static_verifier(pub: str, **over: Any) -> JwtVerifier:
    kwargs: dict[str, Any] = {
        "jwks_url": None,
        "public_key": pub,
        "secret": None,
        "audience": _AUD,
        "issuer": _ISS,
        "algorithms": (),
    }
    kwargs.update(over)
    return JwtVerifier(**kwargs)


def _jwt_request(header: str | None, verifier: Any, *, auth: dict | None = None) -> GatewayRequest:
    headers = {} if header is None else {"authorization": header}
    return GatewayRequest(
        method="POST",
        path="/x",
        headers=headers,
        gateway=SimpleNamespace(_jwt_verifier=verifier),
        auth=auth,
    )


async def _run_middleware(middleware: Any, request: GatewayRequest) -> tuple[GatewayResponse, bool]:
    reached = False

    async def call_next(req: GatewayRequest) -> GatewayResponse:
        nonlocal reached
        reached = True
        return GatewayResponse(200, {"ok": True})

    resp = await middleware(request, call_next)
    return resp, reached


async def _drive(
    asgi: GatewayASGI,
    *,
    method: str = "POST",
    path: str = "/agents/foo",
    headers: dict[str, str] | None = None,
    body: dict | None = None,
    extensions: dict | None = None,
    query_string: bytes = b"",
) -> tuple[int, dict]:
    raw_headers = [(k.encode(), v.encode()) for k, v in (headers or {}).items()]
    scope: dict[str, Any] = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": raw_headers,
        "query_string": query_string,
    }
    if extensions is not None:
        scope["extensions"] = extensions
    body_bytes = json.dumps(body or {}).encode()

    async def receive() -> dict:
        return {"body": body_bytes, "more_body": False}

    sent: list[dict] = []

    async def send(msg: dict) -> None:
        sent.append(msg)

    await asgi(scope, receive, send)
    start = next(e for e in sent if e["type"] == "http.response.start")
    body_evt = next(e for e in sent if e["type"] == "http.response.body")
    try:
        parsed = json.loads(body_evt["body"])
    except (json.JSONDecodeError, ValueError):
        parsed = {}
    return start["status"], parsed


def _patch_uvicorn(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Replace uvicorn.Config/Server so on_start never binds a real socket."""
    import uvicorn

    captured: dict[str, Any] = {}

    def _fake_config(**kw: Any) -> dict[str, Any]:
        captured.update(kw)
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
    return captured


# A module-level capturing middleware, wired by dotted path, to observe the
# request the ASGI edge builds (client_cert / auth) after middleware runs.
_captured_request: dict[str, Any] = {}


async def _capture_mw(request: GatewayRequest, call_next: Any) -> GatewayResponse:
    _captured_request["client_cert"] = request.client_cert
    _captured_request["auth"] = request.auth
    return await call_next(request)


# ---------------------------------------------------------------------------
# Task 1 — M1: middleware-load failures are fatal at startup
# ---------------------------------------------------------------------------


class TestMiddlewareLoadFatal:
    def test_bad_global_middleware_raises_at_construction(self) -> None:
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        config = GatewayConfig(middleware=["nonexistent.module.mw"])
        with pytest.raises(ConfigurationError):
            GatewayASGI(gateway=gateway, route_table=RouteTable.from_config([]), config=config)

    def test_bad_route_middleware_raises_at_construction(self) -> None:
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        config = GatewayConfig(
            routes=[
                {
                    "method": "POST",
                    "path": "/x",
                    "agent": "a",
                    "middleware": ["nonexistent.module.mw"],
                }
            ]
        )
        with pytest.raises(ConfigurationError):
            GatewayASGI(
                gateway=gateway, route_table=RouteTable.from_config(config.routes), config=config
            )

    @pytest.mark.asyncio
    async def test_on_start_crashes_on_bad_middleware(self) -> None:
        # The QA scenario: a middleware that can't load must crash on_start (the
        # supervised gateway dies) rather than serve unauthenticated.
        gw = HTTPGateway("api", GatewayConfig(port=18098, middleware=["nonexistent.module.mw"]))
        gw._bus = None
        with pytest.raises(ConfigurationError):
            await gw.on_start()
        assert gw._uvicorn_server is None  # never reached server startup


# ---------------------------------------------------------------------------
# Task 3 — JWT verifier config validation
# ---------------------------------------------------------------------------


class TestJwtVerifierConfig:
    def test_requires_exactly_one_source_zero(self) -> None:
        with pytest.raises(ConfigurationError, match="exactly one key source"):
            JwtVerifier(jwks_url=None, public_key=None, secret=None, audience=_AUD, issuer=_ISS)

    def test_requires_exactly_one_source_two(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        with pytest.raises(ConfigurationError, match="exactly one key source"):
            JwtVerifier(jwks_url=_JWKS_URL, public_key=pub, secret=None, audience=_AUD, issuer=_ISS)

    def test_requires_audience(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        with pytest.raises(ConfigurationError, match="AUDIENCE"):
            JwtVerifier(jwks_url=None, public_key=pub, secret=None, audience=None, issuer=_ISS)

    def test_requires_issuer(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        with pytest.raises(ConfigurationError, match="ISSUER"):
            JwtVerifier(jwks_url=None, public_key=pub, secret=None, audience=_AUD, issuer=None)

    def test_rejects_rs_hs_mixing(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        with pytest.raises(ConfigurationError, match="mix"):
            JwtVerifier(
                jwks_url=None,
                public_key=pub,
                secret=None,
                audience=_AUD,
                issuer=_ISS,
                algorithms=("RS256", "HS256"),
            )

    def test_secret_requires_hmac(self) -> None:
        with pytest.raises(ConfigurationError, match="HMAC"):
            JwtVerifier(
                jwks_url=None,
                public_key=None,
                secret="x" * 32,
                audience=_AUD,
                issuer=_ISS,
                algorithms=("RS256",),
            )

    def test_public_key_rejects_hmac(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        with pytest.raises(ConfigurationError, match="HMAC"):
            JwtVerifier(
                jwks_url=None,
                public_key=pub,
                secret=None,
                audience=_AUD,
                issuer=_ISS,
                algorithms=("HS256",),
            )

    def test_jwks_must_be_https(self) -> None:
        with pytest.raises(ConfigurationError, match="https"):
            JwtVerifier(
                jwks_url="http://idp.example/jwks",
                public_key=None,
                secret=None,
                audience=_AUD,
                issuer=_ISS,
            )

    def test_pyjwt_absent_raises_configuration_error(self) -> None:
        saved = sys.modules.get("jwt", ...)
        sys.modules["jwt"] = None
        try:
            with pytest.raises(ConfigurationError, match="PyJWT"):
                JwtVerifier(jwks_url=None, public_key="x", secret=None, audience=_AUD, issuer=_ISS)
        finally:
            if saved is ...:
                sys.modules.pop("jwt", None)
            else:
                sys.modules["jwt"] = saved


class TestJwtVerifierFromSettings:
    def test_static_public_key(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        cfg = Settings(
            env={
                "CIVITAS_JWT_PUBLIC_KEY": pub,
                "CIVITAS_JWT_AUDIENCE": _AUD,
                "CIVITAS_JWT_ISSUER": _ISS,
            }
        )
        verifier = JwtVerifier.from_settings(cfg)
        assert verifier._jwk_client is None

    def test_jwks_source(self) -> None:
        cfg = Settings(
            env={
                "CIVITAS_JWT_JWKS_URL": _JWKS_URL,
                "CIVITAS_JWT_AUDIENCE": _AUD,
                "CIVITAS_JWT_ISSUER": _ISS,
            }
        )
        verifier = JwtVerifier.from_settings(cfg)
        assert verifier._jwk_client is not None

    def test_algorithms_parsed(self) -> None:
        cfg = Settings(
            env={
                "CIVITAS_JWT_SECRET": "x" * 32,
                "CIVITAS_JWT_AUDIENCE": _AUD,
                "CIVITAS_JWT_ISSUER": _ISS,
                "CIVITAS_JWT_ALGORITHMS": "HS256, HS384",
            }
        )
        verifier = JwtVerifier.from_settings(cfg)
        assert verifier._algorithms == ["HS256", "HS384"]


# ---------------------------------------------------------------------------
# Task 3 — require_jwt behaviour (status matrix + threat model)
# ---------------------------------------------------------------------------


class TestRequireJwt:
    @pytest.mark.asyncio
    async def test_valid_static_rs256_sets_auth(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        req = _jwt_request(
            f"Bearer {jwt.encode(_claims(), priv, algorithm='RS256')}", _static_verifier(pub)
        )
        resp, reached = await _run_middleware(require_jwt, req)
        assert resp.status == 200
        assert reached
        assert req.auth is not None
        assert req.auth["claims"]["sub"] == "user-1"
        # v0.9.6 (control-plane-writes.md D1): require_jwt also exposes the
        # standard principal dict (id = JWT sub) so control-plane write actions
        # can record an honest actor.
        assert req.auth["principal"] == {"id": "user-1", "method": "jwt"}

    @pytest.mark.asyncio
    async def test_valid_jwks_rs256(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        verifier = JwtVerifier(
            jwks_url=_JWKS_URL, public_key=None, secret=None, audience=_AUD, issuer=_ISS
        )
        verifier._jwk_client = SimpleNamespace(
            get_signing_key_from_jwt=lambda tok: SimpleNamespace(key=pub)
        )
        token = jwt.encode(_claims(), priv, algorithm="RS256", headers={"kid": "k1"})
        req = _jwt_request(f"Bearer {token}", verifier)
        resp, reached = await _run_middleware(require_jwt, req)
        assert resp.status == 200
        assert reached

    @pytest.mark.asyncio
    async def test_valid_hs256_static(self) -> None:
        secret = "supersecret-supersecret-supersecret"
        verifier = JwtVerifier(
            jwks_url=None,
            public_key=None,
            secret=secret,
            audience=_AUD,
            issuer=_ISS,
            algorithms=("HS256",),
        )
        req = _jwt_request(f"Bearer {jwt.encode(_claims(), secret, algorithm='HS256')}", verifier)
        resp, reached = await _run_middleware(require_jwt, req)
        assert resp.status == 200
        assert reached

    @pytest.mark.asyncio
    async def test_bearer_scheme_case_insensitive(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        token = jwt.encode(_claims(), priv, algorithm="RS256")
        req = _jwt_request(f"bEaReR {token}", _static_verifier(pub))
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 200

    @pytest.mark.asyncio
    async def test_expired_token_401(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        # Well beyond the 60s leeway so it is unambiguously expired.
        token = jwt.encode(_claims(exp=int(time.time()) - 3600), priv, algorithm="RS256")
        req = _jwt_request(f"Bearer {token}", _static_verifier(pub))
        resp, reached = await _run_middleware(require_jwt, req)
        assert resp.status == 401
        assert not reached
        assert 'error="invalid_token"' in resp.headers["WWW-Authenticate"]

    @pytest.mark.asyncio
    async def test_missing_exp_401(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        token = jwt.encode(_claims(exp=None), priv, algorithm="RS256")
        req = _jwt_request(f"Bearer {token}", _static_verifier(pub))
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_nbf_in_future_401(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        token = jwt.encode(_claims(nbf=int(time.time()) + 3600), priv, algorithm="RS256")
        req = _jwt_request(f"Bearer {token}", _static_verifier(pub))
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_bad_audience_401(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        token = jwt.encode(_claims(aud="other-api"), priv, algorithm="RS256")
        req = _jwt_request(f"Bearer {token}", _static_verifier(pub))
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_bad_issuer_401(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        token = jwt.encode(_claims(iss="https://evil.example"), priv, algorithm="RS256")
        req = _jwt_request(f"Bearer {token}", _static_verifier(pub))
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_alg_none_rejected_401(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        token = jwt.encode(_claims(), key="", algorithm="none")
        req = _jwt_request(f"Bearer {token}", _static_verifier(pub))
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_rs_hs_confusion_rejected_401(self, rsa_keys: tuple[str, str]) -> None:
        # Algorithm-confusion defense: an HS256 token is rejected outright because
        # the verifier's explicit algorithms allowlist is ["RS256"] only.
        _, pub = rsa_keys
        token = jwt.encode(_claims(), key="attacker-hmac-secret-32bytes-long!", algorithm="HS256")
        req = _jwt_request(f"Bearer {token}", _static_verifier(pub))
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_forged_jku_x5c_header_ignored_401(self, rsa_keys: tuple[str, str]) -> None:
        # PyJWT never fetches keys from jku/x5c/kid headers; an unknown kid just
        # fails the JWKS lookup against our own trusted client.
        priv, pub = rsa_keys
        verifier = JwtVerifier(
            jwks_url=_JWKS_URL, public_key=None, secret=None, audience=_AUD, issuer=_ISS
        )

        def _raise(tok: str) -> Any:
            raise jwt.PyJWKClientError("no matching kid")

        verifier._jwk_client = SimpleNamespace(get_signing_key_from_jwt=_raise)
        token = jwt.encode(
            _claims(),
            priv,
            algorithm="RS256",
            headers={"kid": "attacker", "jku": "https://evil.example/jwks", "x5c": ["forged"]},
        )
        req = _jwt_request(f"Bearer {token}", verifier)
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_oversized_token_401(self, rsa_keys: tuple[str, str]) -> None:
        priv, pub = rsa_keys
        token = jwt.encode(_claims(pad="x" * 9000), priv, algorithm="RS256")
        req = _jwt_request(f"Bearer {token}", _static_verifier(pub))
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_missing_header_401_with_challenge(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        req = _jwt_request(None, _static_verifier(pub))
        resp, reached = await _run_middleware(require_jwt, req)
        assert resp.status == 401
        assert not reached
        assert resp.headers["WWW-Authenticate"] == "Bearer"

    @pytest.mark.asyncio
    async def test_wrong_scheme_401(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        req = _jwt_request("Token abc.def.ghi", _static_verifier(pub))
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_bearer_without_token_401(self, rsa_keys: tuple[str, str]) -> None:
        _, pub = rsa_keys
        req = _jwt_request("Bearer    ", _static_verifier(pub))
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_unconfigured_verifier_500(self) -> None:
        req = _jwt_request("Bearer a.b.c", None)
        resp, reached = await _run_middleware(require_jwt, req)
        assert resp.status == 500
        assert not reached

    @pytest.mark.asyncio
    async def test_no_gateway_500(self) -> None:
        req = GatewayRequest(
            method="POST", path="/x", headers={"authorization": "Bearer a.b.c"}, gateway=None
        )
        resp, _ = await _run_middleware(require_jwt, req)
        assert resp.status == 500


# ---------------------------------------------------------------------------
# Task 4 — require_client_cert (mTLS authorization)
# ---------------------------------------------------------------------------


def _set_mtls_allowlist(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    env = {"CIVITAS_GATEWAY_MTLS_ALLOWED_DNS": value} if value else {}
    monkeypatch.setattr("civitas.gateway.mtls.settings", Settings(env=env))


class TestRequireClientCert:
    @pytest.mark.asyncio
    async def test_allowlisted_dn_continues(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_mtls_allowlist(monkeypatch, _DN)
        req = GatewayRequest(method="GET", path="/x", client_cert={"dn": _DN, "leaf_pem": "PEM"})
        resp, reached = await _run_middleware(require_client_cert, req)
        assert resp.status == 200
        assert reached
        assert req.auth == {"client_cert": {"dn": _DN}}

    @pytest.mark.asyncio
    async def test_unlisted_dn_403(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_mtls_allowlist(monkeypatch, _DN)
        req = GatewayRequest(method="GET", path="/x", client_cert={"dn": "CN=evil"})
        resp, reached = await _run_middleware(require_client_cert, req)
        assert resp.status == 403
        assert not reached

    @pytest.mark.asyncio
    async def test_absent_cert_401(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_mtls_allowlist(monkeypatch, _DN)
        req = GatewayRequest(method="GET", path="/x", client_cert=None)
        resp, reached = await _run_middleware(require_client_cert, req)
        assert resp.status == 401
        assert not reached

    @pytest.mark.asyncio
    async def test_empty_allowlist_500(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_mtls_allowlist(monkeypatch, "")
        req = GatewayRequest(method="GET", path="/x", client_cert={"dn": _DN})
        resp, _ = await _run_middleware(require_client_cert, req)
        assert resp.status == 500

    @pytest.mark.asyncio
    async def test_multi_rdn_dn_preserved(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # DNs contain commas; the semicolon-separated allowlist keeps them intact.
        dn2 = "CN=admin,O=Acme,C=US"
        _set_mtls_allowlist(monkeypatch, f"{_DN};{dn2}")
        req = GatewayRequest(method="GET", path="/x", client_cert={"dn": dn2})
        resp, reached = await _run_middleware(require_client_cert, req)
        assert resp.status == 200
        assert reached


# ---------------------------------------------------------------------------
# Task 4 — GatewayConfig mTLS preconditions + uvicorn ssl wiring
# ---------------------------------------------------------------------------


class TestGatewayConfigMtls:
    def test_unknown_mode_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="client_cert_mode"):
            GatewayConfig(client_cert_mode="bogus")

    def test_mode_requires_ca_cert_key(self) -> None:
        with pytest.raises(ConfigurationError, match="tls_ca_cert"):
            GatewayConfig(client_cert_mode="required")

    def test_mode_requires_all_three(self) -> None:
        with pytest.raises(ConfigurationError, match="tls_ca_cert"):
            GatewayConfig(client_cert_mode="optional", tls_ca_cert="ca.pem")

    def test_mode_incompatible_with_http3(self) -> None:
        with pytest.raises(ConfigurationError, match="HTTP/3"):
            GatewayConfig(
                client_cert_mode="required",
                tls_ca_cert="ca.pem",
                tls_cert="c.pem",
                tls_key="k.pem",
                enable_http3=True,
                port_quic=8443,
            )

    def test_valid_required_mode(self) -> None:
        cfg = GatewayConfig(
            client_cert_mode="required", tls_ca_cert="ca.pem", tls_cert="c.pem", tls_key="k.pem"
        )
        assert cfg.client_cert_mode == "required"
        assert cfg.tls_ca_cert == "ca.pem"

    def test_default_mode_none(self) -> None:
        assert GatewayConfig().client_cert_mode == "none"

    def test_cert_reqs_mapping(self) -> None:
        import ssl

        assert _CERT_REQS == {
            "none": ssl.CERT_NONE,
            "optional": ssl.CERT_OPTIONAL,
            "required": ssl.CERT_REQUIRED,
        }

    @pytest.mark.asyncio
    async def test_on_start_passes_ssl_cert_reqs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import ssl

        captured = _patch_uvicorn(monkeypatch)
        gw = HTTPGateway(
            "api",
            GatewayConfig(
                port=18091,
                client_cert_mode="required",
                tls_ca_cert="ca.pem",
                tls_cert="c.pem",
                tls_key="k.pem",
            ),
        )
        gw._bus = None
        await gw.on_start()
        try:
            assert captured["ssl_cert_reqs"] == ssl.CERT_REQUIRED
            assert captured["ssl_ca_certs"] == "ca.pem"
        finally:
            await gw.on_stop()


# ---------------------------------------------------------------------------
# Task 3/M2 — eager verifier build in on_start
# ---------------------------------------------------------------------------


class TestEagerVerifierBuild:
    @pytest.mark.asyncio
    async def test_on_start_builds_verifier(
        self, monkeypatch: pytest.MonkeyPatch, rsa_keys: tuple[str, str]
    ) -> None:
        _, pub = rsa_keys
        _patch_uvicorn(monkeypatch)
        monkeypatch.setattr(
            "civitas.gateway.core.settings",
            Settings(
                env={
                    "CIVITAS_JWT_PUBLIC_KEY": pub,
                    "CIVITAS_JWT_AUDIENCE": _AUD,
                    "CIVITAS_JWT_ISSUER": _ISS,
                }
            ),
        )
        gw = HTTPGateway("api", GatewayConfig(port=18092, middleware=[_JWT_MIDDLEWARE_PATH]))
        gw._bus = None
        await gw.on_start()
        try:
            assert gw._jwt_verifier is not None
        finally:
            await gw.on_stop()

    @pytest.mark.asyncio
    async def test_on_start_crashes_when_jwt_unconfigured(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_uvicorn(monkeypatch)
        monkeypatch.setattr("civitas.gateway.core.settings", Settings(env={}))
        gw = HTTPGateway("api", GatewayConfig(port=18093, middleware=[_JWT_MIDDLEWARE_PATH]))
        gw._bus = None
        with pytest.raises(ConfigurationError):
            await gw.on_start()

    @pytest.mark.asyncio
    async def test_no_verifier_when_jwt_not_configured(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_uvicorn(monkeypatch)
        gw = HTTPGateway("api", GatewayConfig(port=18094))
        gw._bus = None
        await gw.on_start()
        try:
            assert gw._jwt_verifier is None
        finally:
            await gw.on_stop()


# ---------------------------------------------------------------------------
# Task 2 — client_cert population from the TLS scope
# ---------------------------------------------------------------------------


class TestClientCertPopulation:
    def test_from_scope_with_cert(self) -> None:
        scope = {
            "extensions": {"tls": {"client_cert_name": _DN, "client_cert_chain": ["LEAF-PEM"]}}
        }
        assert _client_cert_from_scope(scope) == {"dn": _DN, "leaf_pem": "LEAF-PEM"}

    def test_from_scope_no_tls(self) -> None:
        assert _client_cert_from_scope({}) is None

    def test_from_scope_tls_without_chain(self) -> None:
        assert _client_cert_from_scope({"extensions": {"tls": {"client_cert_name": _DN}}}) is None

    @pytest.mark.asyncio
    async def test_asgi_populates_client_cert(self) -> None:
        _captured_request.clear()
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        gateway.ask = AsyncMock(return_value=MagicMock(spec=Message, payload={"ok": True}))
        config = GatewayConfig(middleware=["tests.unit.test_gateway_auth._capture_mw"])
        asgi = GatewayASGI(gateway=gateway, route_table=RouteTable.from_config([]), config=config)
        status, _ = await _drive(
            asgi,
            method="POST",
            path="/agents/foo",
            extensions={"tls": {"client_cert_name": _DN, "client_cert_chain": ["LEAF-PEM"]}},
        )
        assert status == 200
        assert _captured_request["client_cert"] == {"dn": _DN, "leaf_pem": "LEAF-PEM"}

    @pytest.mark.asyncio
    async def test_asgi_client_cert_none_without_tls(self) -> None:
        _captured_request.clear()
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        gateway.ask = AsyncMock(return_value=MagicMock(spec=Message, payload={"ok": True}))
        config = GatewayConfig(middleware=["tests.unit.test_gateway_auth._capture_mw"])
        asgi = GatewayASGI(gateway=gateway, route_table=RouteTable.from_config([]), config=config)
        status, _ = await _drive(asgi, method="POST", path="/agents/foo")
        assert status == 200
        assert _captured_request["client_cert"] is None


# ---------------------------------------------------------------------------
# Task 5 — docs gating
# ---------------------------------------------------------------------------


class TestDocsGating:
    def test_no_auth_docs_on_by_default(self) -> None:
        assert GatewayConfig().docs_enabled is True

    def test_api_key_middleware_docs_off(self) -> None:
        assert (
            GatewayConfig(middleware=["civitas.gateway.auth.require_api_key"]).docs_enabled is False
        )

    def test_jwt_middleware_docs_off(self) -> None:
        assert GatewayConfig(middleware=[_JWT_MIDDLEWARE_PATH]).docs_enabled is False

    def test_mtls_middleware_docs_off(self) -> None:
        assert GatewayConfig(middleware=[_MTLS_PATH]).docs_enabled is False

    def test_client_cert_mode_docs_off(self) -> None:
        cfg = GatewayConfig(
            client_cert_mode="optional", tls_ca_cert="ca.pem", tls_cert="c.pem", tls_key="k.pem"
        )
        assert cfg.docs_enabled is False

    def test_explicit_true_overrides(self) -> None:
        cfg = GatewayConfig(middleware=["civitas.gateway.auth.require_api_key"], docs_enabled=True)
        assert cfg.docs_enabled is True

    @pytest.mark.asyncio
    async def test_docs_gated_returns_404(self) -> None:
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        gateway.ask = AsyncMock(side_effect=TimeoutError())
        config = GatewayConfig(
            client_cert_mode="optional", tls_ca_cert="ca.pem", tls_cert="c.pem", tls_key="k.pem"
        )
        asgi = GatewayASGI(gateway=gateway, route_table=RouteTable.from_config([]), config=config)
        status, _ = await _drive(asgi, method="GET", path="/docs")
        assert status == 404

    @pytest.mark.asyncio
    async def test_docs_served_when_explicitly_enabled(self) -> None:
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        config = GatewayConfig(
            client_cert_mode="optional",
            tls_ca_cert="ca.pem",
            tls_cert="c.pem",
            tls_key="k.pem",
            docs_enabled=True,
        )
        asgi = GatewayASGI(gateway=gateway, route_table=RouteTable.from_config([]), config=config)
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/docs",
            "headers": [],
            "query_string": b"",
        }
        sent: list[dict] = []

        async def receive() -> dict:
            return {"body": b"", "more_body": False}

        async def send(msg: dict) -> None:
            sent.append(msg)

        await asgi(scope, receive, send)
        start = next(e for e in sent if e["type"] == "http.response.start")
        assert start["status"] == 200


# ---------------------------------------------------------------------------
# Task 5 — GatewayAuthConfig.from_dict + Runtime wiring
# ---------------------------------------------------------------------------


class TestGatewayAuthConfig:
    def test_from_dict_empty(self) -> None:
        cfg = GatewayAuthConfig.from_dict({})
        assert cfg.client_cert_mode == "none"
        assert cfg.tls_ca_cert is None

    def test_from_dict_mtls_block(self) -> None:
        cfg = GatewayAuthConfig.from_dict(
            {"mtls": {"ca_cert": "ca.pem", "client_cert_mode": "required"}}
        )
        assert cfg.tls_ca_cert == "ca.pem"
        assert cfg.client_cert_mode == "required"

    def test_from_dict_tls_ca_cert_alias(self) -> None:
        cfg = GatewayAuthConfig.from_dict({"mtls": {"tls_ca_cert": "ca2.pem"}})
        assert cfg.tls_ca_cert == "ca2.pem"


class TestRuntimeGatewayAuthWiring:
    def test_from_config_dict_wires_mtls(self) -> None:
        config = {
            "supervision": {
                "name": "root",
                "children": [
                    {
                        "name": "api",
                        "type": "http_gateway",
                        "config": {
                            "port": 18099,
                            "tls_cert": "c.pem",
                            "tls_key": "k.pem",
                            "auth": {"mtls": {"ca_cert": "ca.pem", "client_cert_mode": "required"}},
                        },
                    }
                ],
            }
        }
        rt = Runtime.from_config_dict(config)
        gateways = [a for a in rt.all_agents() if isinstance(a, HTTPGateway)]
        assert len(gateways) == 1
        assert gateways[0]._gw_config.client_cert_mode == "required"
        assert gateways[0]._gw_config.tls_ca_cert == "ca.pem"
        assert gateways[0]._gw_config.docs_enabled is False


# ---------------------------------------------------------------------------
# Compose: recommended order rate_limit -> mTLS -> JWT -> api-key
# ---------------------------------------------------------------------------


class TestCompose:
    @pytest.mark.asyncio
    async def test_mtls_runs_before_jwt(
        self, monkeypatch: pytest.MonkeyPatch, rsa_keys: tuple[str, str]
    ) -> None:
        _, pub = rsa_keys
        _set_mtls_allowlist(monkeypatch, _DN)
        verifier = MagicMock()
        verifier.verify = AsyncMock(side_effect=AssertionError("JWT ran before mTLS"))
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        gateway._jwt_verifier = verifier
        gateway.ask = AsyncMock(return_value=MagicMock(spec=Message, payload={"ok": True}))
        config = GatewayConfig(middleware=[_MTLS_PATH, _JWT_MIDDLEWARE_PATH])
        asgi = GatewayASGI(gateway=gateway, route_table=RouteTable.from_config([]), config=config)
        # No client cert -> mTLS short-circuits with 401 before JWT is consulted.
        status, _ = await _drive(asgi, method="POST", path="/agents/foo")
        assert status == 401
        verifier.verify.assert_not_called()

    @pytest.mark.asyncio
    async def test_full_stack_composes_and_populates_auth(
        self, monkeypatch: pytest.MonkeyPatch, rsa_keys: tuple[str, str]
    ) -> None:
        priv, pub = rsa_keys
        _set_mtls_allowlist(monkeypatch, _DN)
        monkeypatch.setattr(
            "civitas.gateway.auth.settings",
            Settings(env={"CIVITAS_GATEWAY_API_KEY": "topsecret"}),
        )
        _captured_request.clear()
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        gateway._jwt_verifier = _static_verifier(pub)
        gateway.call = AsyncMock(return_value={"allowed": True})
        gateway.ask = AsyncMock(return_value=MagicMock(spec=Message, payload={"ok": True}))
        config = GatewayConfig(
            middleware=[
                "civitas.gateway.ratelimit.rate_limit",
                _MTLS_PATH,
                _JWT_MIDDLEWARE_PATH,
                "civitas.gateway.auth.require_api_key",
                "tests.unit.test_gateway_auth._capture_mw",
            ]
        )
        asgi = GatewayASGI(gateway=gateway, route_table=RouteTable.from_config([]), config=config)
        token = jwt.encode(_claims(), priv, algorithm="RS256")
        status, _ = await _drive(
            asgi,
            method="POST",
            path="/agents/foo",
            headers={"authorization": f"Bearer {token}", "x-api-key": "topsecret"},
            extensions={"tls": {"client_cert_name": _DN, "client_cert_chain": ["LEAF-PEM"]}},
        )
        assert status == 200
        assert _captured_request["auth"]["client_cert"]["dn"] == _DN
        assert _captured_request["auth"]["claims"]["sub"] == "user-1"
