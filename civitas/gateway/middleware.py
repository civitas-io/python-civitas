"""Middleware chain builder and loader."""

from __future__ import annotations

import importlib
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from civitas.gateway.types import GatewayRequest, GatewayResponse, MiddlewareCallable

logger = logging.getLogger(__name__)


def load_middleware(dotted_path: str) -> MiddlewareCallable:
    """Import and return a middleware callable from a dotted module path."""
    module_path, _, name = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(
            f"Invalid middleware path {dotted_path!r}: must be a dotted path like "
            "'myapp.middleware.require_api_key'"
        )
    module = importlib.import_module(module_path)
    fn: MiddlewareCallable = getattr(module, name)
    return fn


def build_chain(
    middlewares: list[MiddlewareCallable],
    handler: Callable[[GatewayRequest], Awaitable[GatewayResponse]],
) -> Callable[[GatewayRequest], Awaitable[GatewayResponse]]:
    """Wrap *handler* with *middlewares* in order (first = outermost).

    Each middleware receives ``(request, next_fn)`` and must either call
    ``await next_fn(request)`` to continue the chain or return a
    ``GatewayResponse`` directly to short-circuit.
    """

    async def terminal(request: GatewayRequest) -> GatewayResponse:
        return await handler(request)

    chain: Callable[[GatewayRequest], Awaitable[GatewayResponse]] = terminal
    for mw in reversed(middlewares):
        _next = chain
        _mw: MiddlewareCallable = mw

        async def _wrapped(
            request: GatewayRequest,
            __next: Any = _next,
            __mw: Any = _mw,
        ) -> GatewayResponse:
            result: GatewayResponse = await __mw(request, __next)
            return result

        chain = _wrapped

    return chain
