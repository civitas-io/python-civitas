"""First-party mTLS client-certificate authorization middleware (v0.7.0 R3).

:func:`require_client_cert` authorizes a request on the client's TLS certificate,
which the ASGI edge exposes on ``request.client_cert`` (see
:func:`civitas.gateway.asgi._client_cert_from_scope`). Wire it from topology
YAML::

    middleware: [civitas.gateway.mtls.require_client_cert]

and set the exact-match allowlist of full subject DNs via
``CIVITAS_GATEWAY_MTLS_ALLOWED_DNS`` (semicolon-separated, since a DN itself
contains commas). Authorization is on the **full subject DN, exact match** — not
a CN substring — to avoid CN spoofing.

Operator note: the trust anchor (``GatewayConfig.tls_ca_cert``) MUST be a dedicated
private CA. TLS proves "signed by a trusted CA", not "is this identity" — a public
or broad CA means any certificate it signed passes, leaving the DN allowlist as the
sole (and then spoofable) gate. mTLS is enforced on the uvicorn path only; HTTP/3
cannot enforce client certs and is rejected at config time.
"""

from __future__ import annotations

import base64
import binascii
import ipaddress
import logging
from typing import TYPE_CHECKING, Any

from civitas.config import settings
from civitas.errors import ConfigurationError
from civitas.gateway.types import GatewayRequest, GatewayResponse, NextMiddleware

if TYPE_CHECKING:
    from cryptography import x509

logger = logging.getLogger(__name__)

_MTLS_MIDDLEWARE_PATH = "civitas.gateway.mtls.require_client_cert"


class _MtlsMisconfigured(Exception):
    """Internal marker: the DN allowlist is empty -> HTTP 500 / gRPC INTERNAL."""


class _NoCertificate(Exception):
    """Internal marker: no client certificate DN presented -> HTTP 401 / gRPC UNAUTHENTICATED."""


class _Forbidden(Exception):
    """Internal marker: the cert DN is not allowlisted -> HTTP 403 / gRPC PERMISSION_DENIED."""

    def __init__(self, dn: str) -> None:
        self.dn = dn
        super().__init__(f"client certificate DN not authorized: {dn}")


def _check_dn(dn: str | None, allowed: frozenset[str]) -> None:
    """Authorize a client-cert subject DN against the exact-match allowlist.

    The single, transport-agnostic authorization predicate: HTTP, gRPC, and any
    future mTLS surface all raise the same three markers so their fail-closed
    semantics stay identical (D5).

    Args:
        dn: The client certificate's full subject DN, or ``None`` if none was
            presented.
        allowed: The exact-match allowlist of authorized subject DNs.

    Raises:
        _MtlsMisconfigured: The allowlist is empty (loud misconfig, never allow-all).
        _NoCertificate: No certificate DN was presented.
        _Forbidden: The DN is not in the allowlist.
    """
    if not allowed:
        raise _MtlsMisconfigured
    if dn is None:
        raise _NoCertificate
    if dn not in allowed:
        raise _Forbidden(dn)


def _load_x509() -> Any:
    """Return the ``cryptography.x509`` module, behind one guarded import.

    The single place ``cryptography`` is imported for DN extraction, so both the
    PEM and DER loaders share one ``ImportError`` guard (D4/N1). ``cryptography``
    stays a lazy, optional dependency — never a hard requirement of the ``grpc``
    extra.

    Raises:
        ConfigurationError: If ``cryptography`` is not installed.
    """
    try:
        from cryptography import x509
    except ImportError as exc:
        raise ConfigurationError(
            "gRPC/HTTP mTLS DN extraction requires cryptography. "
            "Install with: pip install 'civitas[jwt]' or 'cryptography>=41'"
        ) from exc
    return x509


def _dn_from_cert(cert: x509.Certificate) -> str:
    """Return a certificate's full subject DN (RFC 4514).

    The single DN formatter shared by :func:`_dn_from_pem` and
    :func:`_dn_from_der`, so every transport agrees on DN string format by
    construction (D4/F2).
    """
    return cert.subject.rfc4514_string()


def _dn_from_pem(pem: bytes) -> str:
    """Return a certificate's full subject DN (RFC 4514) from its PEM bytes.

    The PEM-encoded sibling of :func:`_dn_from_der` (gRPC's
    ``auth_context()["x509_pem_cert"]`` is PEM); both funnel through
    :func:`_dn_from_cert` so their output is byte-identical for the same cert.

    Raises:
        ConfigurationError: If ``cryptography`` is not installed.
    """
    return _dn_from_cert(_load_x509().load_pem_x509_certificate(pem))


def _dn_from_der(der: bytes) -> str:
    """Return a certificate's full subject DN (RFC 4514) from its DER bytes.

    The DER-encoded sibling of :func:`_dn_from_pem` (RFC 9440's ``Client-Cert``
    header carries base64 DER, not PEM); both funnel through :func:`_dn_from_cert`
    so their output is byte-identical for the same cert.

    Raises:
        ConfigurationError: If ``cryptography`` is not installed.
    """
    return _dn_from_cert(_load_x509().load_der_x509_certificate(der))


def _client_cert_from_headers(
    scope: dict[str, Any], trusted_cidrs: frozenset[str]
) -> dict[str, Any] | None:
    """Return the RFC 9440 ``Client-Cert`` leaf DN, or None if untrusted/absent.

    The ``mtls_source='proxy_header'`` counterpart to
    :func:`civitas.gateway.asgi._client_cert_from_scope`: a TLS-terminating reverse
    proxy forwards the verified client certificate as ``Client-Cert: :<base64-DER>:``
    (RFC 9440). Fail-closed to ``None`` at every stage, in cheap-check-first order:

    1. The peer IP (``scope["client"]`` — the true TCP peer, since uvicorn's
       ``proxy_headers`` is forced off in this mode) must fall inside
       ``trusted_cidrs``; otherwise the header is ignored entirely (D2), never
       trusting a spoofable ``X-Forwarded-For``.
    2. Exactly one ``Client-Cert`` header must be present (RFC 9440 §2.2 forbids
       multiples); the raw ``scope["headers"]`` list is scanned case-insensitively
       so duplicates stay visible instead of collapsing to a last-wins value.
    3. The ``:``-delimited, strict-base64 DER is decoded and its subject DN
       extracted; a malformed value returns ``None``, never raising to a 500.

    A missing ``cryptography`` is deliberately not swallowed here — it surfaces as a
    startup ``ConfigurationError`` (D8), not a per-request failure.

    Args:
        scope: The ASGI connection scope (mirrors ``_client_cert_from_scope``).
        trusted_cidrs: CIDRs the immediate peer must fall within to be trusted.
    """
    client = scope.get("client")
    if not client:
        return None
    try:
        peer_ip = ipaddress.ip_address(client[0])
    except ValueError:
        return None
    if not any(peer_ip in ipaddress.ip_network(cidr, strict=False) for cidr in trusted_cidrs):
        return None

    values = [value for name, value in scope.get("headers", []) if name.lower() == b"client-cert"]
    if len(values) != 1:
        return None
    raw = values[0]
    if len(raw) < 2 or not raw.startswith(b":") or not raw.endswith(b":"):
        return None

    try:
        der = base64.b64decode(raw[1:-1], validate=True)
        dn = _dn_from_der(der)
    except (binascii.Error, ValueError):
        return None
    return {"dn": dn}


async def require_client_cert(
    request: GatewayRequest, call_next: NextMiddleware
) -> GatewayResponse:
    """Authorize on the client certificate's full subject DN; fail-closed.

    - No allowlist configured -> 500 (loud misconfig, never allow-all).
    - No client certificate presented -> 401 (fail-closed under CERT_OPTIONAL).
    - Certificate subject DN not in the exact-match allowlist -> 403.
    - Otherwise the DN is attached at ``request.auth["client_cert"]`` and the
      request continues down the chain.
    """
    dn = request.client_cert.get("dn") if request.client_cert is not None else None
    try:
        _check_dn(dn, settings.gateway_mtls_allowed_dns)
    except _MtlsMisconfigured:
        logger.error(
            "require_client_cert is enabled but CIVITAS_GATEWAY_MTLS_ALLOWED_DNS is not set; "
            "denying request"
        )
        return GatewayResponse(status=500, body={"error": "server auth is not configured"})
    except _NoCertificate:
        return GatewayResponse(status=401, body={"error": "client certificate required"})
    except _Forbidden:
        return GatewayResponse(status=403, body={"error": "client certificate not authorized"})
    request.auth = {**(request.auth or {}), "client_cert": {"dn": dn}}
    return await call_next(request)
