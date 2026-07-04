"""First-party authentication middleware (G5).

:func:`require_api_key` checks a shared secret in the ``X-API-Key`` header against
``CIVITAS_GATEWAY_API_KEY`` (read via ``civitas.config.settings``). Wire it from
topology YAML::

    middleware: [civitas.gateway.auth.require_api_key]

It is fail-closed: if the header is missing/wrong the request gets 401, and if the
server secret is not configured at all the request gets 500 (a loud misconfig
rather than silently allowing everything).

JWT and mTLS remain integration points you implement as your own middleware —
first-party JWT would pull a JWT dependency, so it is tracked as a separate opt-in
extra rather than shipped in core.
"""

from __future__ import annotations

import hmac
import logging

from civitas.config import settings
from civitas.gateway.types import GatewayRequest, GatewayResponse, NextMiddleware

logger = logging.getLogger(__name__)


async def require_api_key(request: GatewayRequest, call_next: NextMiddleware) -> GatewayResponse:
    """Require a valid ``X-API-Key`` header; fail-closed otherwise."""
    expected = settings.gateway_api_key.get()
    if not expected:
        logger.error(
            "require_api_key is enabled but CIVITAS_GATEWAY_API_KEY is not set; denying request"
        )
        return GatewayResponse(status=500, body={"error": "server auth is not configured"})
    provided = request.headers.get("x-api-key", "")
    # Constant-time compare so a wrong key can't be recovered via timing.
    if not hmac.compare_digest(provided, expected):
        return GatewayResponse(status=401, body={"error": "invalid or missing API key"})
    return await call_next(request)
