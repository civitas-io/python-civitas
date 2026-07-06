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

import logging

from civitas.config import settings
from civitas.errors import ConfigurationError
from civitas.gateway.types import GatewayRequest, GatewayResponse, NextMiddleware

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


def _dn_from_pem(pem: bytes) -> str:
    """Return a certificate's full subject DN (RFC 4514) from its PEM bytes.

    The single DN-extraction path for every transport, so gRPC (and any future
    #25-unblocked HTTP/WS mTLS) agree on DN string format by construction (D4/F2).
    ``cryptography`` is imported lazily and guarded so it never becomes a hard
    dependency of the ``grpc`` extra.

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
    return x509.load_pem_x509_certificate(pem).subject.rfc4514_string()


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
