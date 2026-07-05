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
from civitas.gateway.types import GatewayRequest, GatewayResponse, NextMiddleware

logger = logging.getLogger(__name__)

_MTLS_MIDDLEWARE_PATH = "civitas.gateway.mtls.require_client_cert"


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
    allowed = settings.gateway_mtls_allowed_dns
    if not allowed:
        logger.error(
            "require_client_cert is enabled but CIVITAS_GATEWAY_MTLS_ALLOWED_DNS is not set; "
            "denying request"
        )
        return GatewayResponse(status=500, body={"error": "server auth is not configured"})
    if request.client_cert is None:
        return GatewayResponse(status=401, body={"error": "client certificate required"})
    dn = request.client_cert.get("dn")
    if dn not in allowed:
        return GatewayResponse(status=403, body={"error": "client certificate not authorized"})
    request.auth = {**(request.auth or {}), "client_cert": {"dn": dn}}
    return await call_next(request)
