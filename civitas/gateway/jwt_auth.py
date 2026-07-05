"""First-party JWT bearer verification middleware (v0.7.0 R3, opt-in ``civitas[jwt]``).

:func:`require_jwt` verifies an ``Authorization: Bearer <token>`` header against a
:class:`JwtVerifier` built once, eagerly, at gateway ``on_start`` (so a
misconfiguration or a missing PyJWT crashes startup rather than failing on the
first request). Wire it from topology YAML::

    middleware: [civitas.gateway.jwt_auth.require_jwt]

and configure it via ``CIVITAS_JWT_*`` environment variables (see
:mod:`civitas.config`). It is secure-by-default: explicit ``algorithms`` (default
``["RS256"]``), ``exp``/``iss``/``aud`` are *required* (a token without ``exp``
never expires otherwise), audience and issuer are verified, ``alg=none`` and
RS/HS algorithm confusion are rejected, tokens are size-capped, and the blocking
JWKS lookup is offloaded to a thread so it never stalls the event loop.

Security boundary: this covers HTTP request routes only — WebSocket and gRPC
surfaces bypass the middleware chain and are not protected (tracked as a
follow-up). Client identity is never read from request headers (spoofable).
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from civitas.errors import ConfigurationError
from civitas.gateway.types import GatewayRequest, GatewayResponse, NextMiddleware

if TYPE_CHECKING:
    from civitas.config import Settings

logger = logging.getLogger(__name__)

_JWT_MIDDLEWARE_PATH = "civitas.gateway.jwt_auth.require_jwt"
_DEFAULT_ALGORITHMS = ("RS256",)
# Reject tokens larger than this before any parsing — bounds the work an
# unauthenticated caller can force and blunts oversized-token DoS.
_MAX_TOKEN_BYTES = 8192
# Bounded clock-skew tolerance; capped well under the design's 120s ceiling.
_LEEWAY_SECONDS = 60


class _InvalidToken(Exception):
    """Internal marker: a token failed verification and maps to HTTP 401."""


class JwtVerifier:
    """Validated, reusable JWT verifier holding one key source (JWKS xor static).

    Built once at startup from :class:`civitas.config.Settings`. The constructor
    validates the whole configuration and raises :class:`ConfigurationError` on any
    problem, so ``on_start`` fails loudly instead of the gateway serving 500s.
    """

    def __init__(
        self,
        *,
        jwks_url: str | None,
        public_key: str | None,
        secret: str | None,
        audience: str | None,
        issuer: str | None,
        algorithms: tuple[str, ...] = (),
    ) -> None:
        try:
            import jwt
        except ImportError as exc:
            raise ConfigurationError(
                "JWT gateway auth requires PyJWT. Install with: pip install 'civitas[jwt]'"
            ) from exc

        sources = [bool(jwks_url), bool(public_key), bool(secret)]
        if sum(sources) != 1:
            raise ConfigurationError(
                "JWT auth requires exactly one key source: set one of "
                "CIVITAS_JWT_JWKS_URL, CIVITAS_JWT_PUBLIC_KEY, or CIVITAS_JWT_SECRET"
            )
        if not audience:
            raise ConfigurationError("JWT auth requires CIVITAS_JWT_AUDIENCE")
        if not issuer:
            raise ConfigurationError("JWT auth requires CIVITAS_JWT_ISSUER")

        algs = list(algorithms) if algorithms else list(_DEFAULT_ALGORITHMS)
        is_hmac = [a.upper().startswith("HS") for a in algs]
        if any(is_hmac) and not all(is_hmac):
            raise ConfigurationError(
                "JWT algorithms must not mix HMAC (HS*) and asymmetric algorithms"
            )
        uses_hmac = bool(algs) and all(is_hmac)
        if secret and not uses_hmac:
            raise ConfigurationError("CIVITAS_JWT_SECRET requires an HMAC algorithm (e.g. HS256)")
        if (jwks_url or public_key) and uses_hmac:
            raise ConfigurationError(
                "HMAC algorithms cannot be used with CIVITAS_JWT_JWKS_URL or CIVITAS_JWT_PUBLIC_KEY"
            )
        if jwks_url and not jwks_url.lower().startswith("https://"):
            raise ConfigurationError("CIVITAS_JWT_JWKS_URL must use https://")

        self._audience = audience
        self._issuer = issuer
        self._algorithms = algs
        self._static_key: str = public_key or secret or ""
        self._decode = jwt.decode
        self._jwt_errors: tuple[type[BaseException], ...] = (jwt.PyJWTError,)
        self._jwk_client = jwt.PyJWKClient(jwks_url) if jwks_url else None

    @classmethod
    def from_settings(cls, cfg: Settings) -> JwtVerifier:
        """Build a verifier from ``CIVITAS_JWT_*`` settings."""
        return cls(
            jwks_url=cfg.jwt_jwks_url,
            public_key=cfg.jwt_public_key.get(),
            secret=cfg.jwt_secret.get(),
            audience=cfg.jwt_audience,
            issuer=cfg.jwt_issuer,
            algorithms=cfg.jwt_algorithms,
        )

    async def verify(self, token: str) -> dict[str, Any]:
        """Return verified claims, or raise :class:`_InvalidToken` (maps to 401)."""
        if len(token) > _MAX_TOKEN_BYTES:
            raise _InvalidToken("token exceeds maximum size")
        try:
            if self._jwk_client is not None:
                # PyJWKClient.get_signing_key_from_jwt does blocking network I/O;
                # offload it so a JWKS refetch never stalls the event loop.
                signing_key = await asyncio.to_thread(
                    self._jwk_client.get_signing_key_from_jwt, token
                )
                key = signing_key.key
            else:
                key = self._static_key
            claims: dict[str, Any] = self._decode(
                token,
                key,
                algorithms=self._algorithms,
                audience=self._audience,
                issuer=self._issuer,
                leeway=_LEEWAY_SECONDS,
                options={"require": ["exp", "iss", "aud"], "verify_aud": True, "verify_iss": True},
            )
        except self._jwt_errors as exc:
            raise _InvalidToken(str(exc)) from exc
        return claims


async def require_jwt(request: GatewayRequest, call_next: NextMiddleware) -> GatewayResponse:
    """Require a valid ``Authorization: Bearer`` JWT; fail-closed otherwise.

    On success, verified claims are attached at ``request.auth["claims"]`` and the
    request continues down the chain. A missing/malformed header or an invalid
    token yields 401 with a ``WWW-Authenticate: Bearer`` challenge (RFC 6750); an
    unconfigured verifier yields 500 (loud misconfig, never allow-all).
    """
    gateway = request.gateway
    verifier = getattr(gateway, "_jwt_verifier", None) if gateway is not None else None
    if verifier is None:
        logger.error("require_jwt is enabled but no JWT verifier is configured; denying request")
        return GatewayResponse(status=500, body={"error": "server auth is not configured"})

    scheme, _, raw_token = request.headers.get("authorization", "").partition(" ")
    token = raw_token.strip()
    if scheme.lower() != "bearer" or not token:
        return GatewayResponse(
            status=401,
            body={"error": "missing bearer token"},
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        claims = await verifier.verify(token)
    except _InvalidToken:
        return GatewayResponse(
            status=401,
            body={"error": "invalid token"},
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )
    request.auth = {**(request.auth or {}), "claims": claims}
    return await call_next(request)
