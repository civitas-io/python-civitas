"""Gateway request/response types and middleware protocol."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from civitas.gateway.core import HTTPGateway


@dataclass
class GatewayRequest:
    """Thin HTTP request abstraction passed through the middleware chain."""

    method: str
    path: str
    path_params: dict[str, str] = field(default_factory=dict)
    query_params: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    body: dict[str, Any] = field(default_factory=dict)
    client_ip: str = ""
    gateway: HTTPGateway | None = None
    # mTLS leaf, set by the ASGI edge only when the client presents one:
    # ``{"dn": <full subject DN>, "leaf_pem": <PEM>}``, else None.
    client_cert: dict[str, Any] | None = None
    # Verified identity from auth middleware (authN feeding authZ); never merged
    # into the dispatched payload to avoid reserved-key collisions.
    auth: dict[str, Any] | None = None


@dataclass
class GatewayResponse:
    """Thin HTTP response produced by middleware or terminal dispatch handler.

    When ``stream`` is set, the body is delivered incrementally as Server-Sent
    Events (G3); ``status`` and ``headers`` still apply and ``body`` is ignored.
    """

    status: int = 200
    body: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    stream: AsyncIterator[dict[str, Any]] | None = field(default=None, repr=False)


# Middleware callable: (request, next) → response
NextMiddleware = Callable[[GatewayRequest], Awaitable[GatewayResponse]]
MiddlewareCallable = Callable[[GatewayRequest, NextMiddleware], Awaitable[GatewayResponse]]
