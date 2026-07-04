"""First-party rate-limiting middleware (G4).

A :class:`RateLimiter` ``GenServer`` holds sliding-window counters; the
:func:`rate_limit` middleware consults it once per request and returns HTTP 429
when a client is over budget. Wire both from topology YAML::

    children:
      - name: rate_limiter          # the fixed name the middleware calls
        type: gen_server
        module: civitas.gateway.ratelimit
        class: RateLimiter
        config: {max_requests: 100, window_seconds: 60}

    # in the gateway's config:
    middleware: [civitas.gateway.ratelimit.rate_limit]

Keeping the counters in a GenServer (rather than middleware-local state) means
the limit is shared across every request the gateway serves and survives as one
supervised process.
"""

from __future__ import annotations

import time
from typing import Any

from civitas.gateway.types import GatewayRequest, GatewayResponse, NextMiddleware
from civitas.genserver import GenServer

_RATE_LIMITER_NAME = "rate_limiter"


class RateLimiter(GenServer):
    """Sliding-window limiter: ``max_requests`` per ``window_seconds`` per client."""

    def __init__(
        self,
        name: str,
        max_requests: int = 100,
        window_seconds: float = 60.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, **kwargs)
        self._max = max_requests
        self._window = window_seconds
        self._hits: dict[str, list[float]] = {}

    async def handle_call(self, payload: dict[str, Any], from_: str) -> dict[str, Any]:
        client_id = str(payload.get("client_id", ""))
        now = time.monotonic()
        cutoff = now - self._window
        hits = [t for t in self._hits.get(client_id, []) if t >= cutoff]
        allowed = len(hits) < self._max
        if allowed:
            hits.append(now)
        self._hits[client_id] = hits
        return {
            "allowed": allowed,
            "remaining": max(0, self._max - len(hits)),
            "retry_after": int(self._window),
        }


async def rate_limit(request: GatewayRequest, call_next: NextMiddleware) -> GatewayResponse:
    """Reject requests over the per-client rate budget with HTTP 429."""
    if request.gateway is None:
        return await call_next(request)
    result = await request.gateway.call(_RATE_LIMITER_NAME, {"client_id": request.client_ip})
    if not result.get("allowed", True):
        return GatewayResponse(
            status=429,
            headers={"Retry-After": str(result.get("retry_after", 60))},
            body={"error": "rate limit exceeded"},
        )
    return await call_next(request)
