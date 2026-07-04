"""ASGI callable — translates HTTP requests into Civitas messages and back."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from civitas.gateway.contracts import validate_request, validate_response
from civitas.gateway.dispatch import DispatchResult, DispatchStatus, GatewayDispatcher
from civitas.gateway.middleware import build_chain, load_middleware
from civitas.gateway.openapi import build_spec, swagger_html
from civitas.gateway.types import GatewayRequest, GatewayResponse, MiddlewareCallable

if TYPE_CHECKING:
    from civitas.gateway.core import GatewayConfig, HTTPGateway
    from civitas.gateway.router import RouteEntry, RouteTable

logger = logging.getLogger(__name__)

# ASGI type aliases
_Scope = dict[str, Any]
_Receive = Any
_Send = Any

_CONTENT_TYPE_JSON = (b"content-type", b"application/json")
_CONTENT_TYPE_HTML = (b"content-type", b"text/html; charset=utf-8")


def _parse_traceparent(value: str) -> tuple[str, str | None]:
    """Extract (trace_id, parent_span_id) from a W3C traceparent header.

    Format: 00-{32-hex trace_id}-{16-hex parent_span_id}-{2-hex flags}
    Returns ("", None) on malformed input.
    """
    parts = value.split("-")
    if len(parts) == 4 and len(parts[1]) == 32 and len(parts[2]) == 16:
        return parts[1], parts[2]
    return "", None


def _parse_query(query_string: bytes) -> dict[str, str]:
    result: dict[str, str] = {}
    if not query_string:
        return result
    for pair in query_string.decode(errors="replace").split("&"):
        if "=" in pair:
            k, _, v = pair.partition("=")
            result[k] = v
    return result


class GatewayASGI:
    """ASGI app served by uvicorn. Dispatches requests onto the Civitas bus."""

    def __init__(
        self,
        gateway: HTTPGateway,
        route_table: RouteTable,
        config: GatewayConfig,
        dispatcher: GatewayDispatcher | None = None,
    ) -> None:
        self._gateway = gateway
        self._route_table = route_table
        self._config = config
        self._dispatcher = dispatcher or GatewayDispatcher(gateway, config.request_timeout)

        # Load global middleware from config at construction time
        self._middlewares: list[MiddlewareCallable] = []
        for dotted_path in config.middleware:
            try:
                self._middlewares.append(load_middleware(dotted_path))
            except Exception:
                logger.exception("Failed to load middleware %r", dotted_path)

        # Route-scoped middleware is resolved lazily per RouteEntry (keyed by
        # object identity — entries live for the lifetime of the route table)
        # and cached so repeated requests don't re-import on every call.
        self._route_middleware_cache: dict[int, list[MiddlewareCallable]] = {}

        # Cached OpenAPI spec (built lazily)
        self._openapi_spec: dict[str, Any] | None = None

    def _route_middlewares(self, entry: RouteEntry) -> list[MiddlewareCallable]:
        """Resolve and cache *entry*'s own ``middleware:`` dotted paths."""
        cached = self._route_middleware_cache.get(id(entry))
        if cached is not None:
            return cached
        resolved: list[MiddlewareCallable] = []
        for dotted_path in entry.middleware:
            try:
                resolved.append(load_middleware(dotted_path))
            except Exception:
                logger.exception(
                    "Failed to load route middleware %r for %s %s",
                    dotted_path,
                    entry.method,
                    entry.path_pattern,
                )
        self._route_middleware_cache[id(entry)] = resolved
        return resolved

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        if scope["type"] == "lifespan":
            await self._handle_lifespan(receive, send)
        elif scope["type"] == "http":
            await self._handle_http(scope, receive, send)

    # ------------------------------------------------------------------
    # Lifespan
    # ------------------------------------------------------------------

    async def _handle_lifespan(self, receive: _Receive, send: _Send) -> None:
        while True:
            event = await receive()
            if event["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return

    # ------------------------------------------------------------------
    # HTTP request handling
    # ------------------------------------------------------------------

    async def _handle_http(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        method: str = scope["method"]
        path: str = scope["path"]
        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        query_params = _parse_query(scope.get("query_string", b""))
        client_ip = (scope.get("client") or ("", 0))[0]

        # Serve OpenAPI / docs before reading body
        if self._config.docs_enabled:
            docs_path = self._config.docs_path.rstrip("/")
            if method == "GET" and path == docs_path:
                await self._serve_swagger(send)
                return
            if method == "GET" and path in (docs_path + "/openapi.json", "/openapi.json"):
                await self._serve_openapi_json(send)
                return

        # Read full body
        body_bytes = b""
        while True:
            chunk = await receive()
            body_bytes += chunk.get("body", b"")
            if not chunk.get("more_body", False):
                break

        # Parse JSON body (empty body → empty dict)
        body: dict[str, Any] = {}
        if body_bytes:
            try:
                parsed = json.loads(body_bytes)
                if isinstance(parsed, dict):
                    body = parsed
                else:
                    await self._respond(
                        send, GatewayResponse(400, {"error": "request body must be a JSON object"})
                    )
                    return
            except (json.JSONDecodeError, ValueError):
                await self._respond(send, GatewayResponse(400, {"error": "invalid JSON body"}))
                return

        matched = self._route_table.match(method, path)
        route_middlewares = self._route_middlewares(matched[0]) if matched is not None else []

        request = GatewayRequest(
            method=method,
            path=path,
            path_params=matched[1] if matched is not None else {},
            query_params=query_params,
            headers=headers,
            body=body,
            client_ip=client_ip,
            gateway=self._gateway,
        )

        # Build and run middleware chain: global middleware, then this route's
        # own middleware, then contract validation + bus dispatch (terminal).
        chain = build_chain(self._middlewares + route_middlewares, self._dispatch_handler)
        response = await chain(request)

        # Attach trace context headers from original request
        trace_extra: dict[str, str] = {}
        if tp := headers.get("traceparent"):
            trace_extra["traceparent"] = tp

        await self._respond(send, response, extra_headers=trace_extra)

    async def _dispatch_handler(self, request: GatewayRequest) -> GatewayResponse:
        """Terminal middleware handler: route → contract validate → dispatch."""
        method = request.method
        path = request.path
        headers = request.headers
        body = request.body

        # Trace context
        trace_id, _parent_span_id = "", None
        if tp := headers.get("traceparent"):
            trace_id, _parent_span_id = _parse_traceparent(tp)

        # Message type override
        msg_type = headers.get("x-civitas-type", "http.request")

        # Custom route match
        matched = self._route_table.match(method, path)
        if matched is not None:
            entry, path_params = matched
            payload = {**body, **path_params}

            # Request contract validation
            if entry.request_schema is not None:
                valid, err = validate_request(entry.request_schema, payload)
                if not valid:
                    return GatewayResponse(422, err or {})

            result = await self._dispatcher.dispatch(
                recipient=entry.agent,
                msg_type=msg_type,
                payload=payload,
                mode=entry.mode,
                trace_id=trace_id,
            )
            return self._result_to_response(
                result, response_schema=entry.response_schema, method=method, path=path
            )

        # Default route fallback
        default = self._default_route(method, path, body)
        if default is not None:
            agent, mode, payload = default
            result = await self._dispatcher.dispatch(
                recipient=agent, msg_type=msg_type, payload=payload, mode=mode, trace_id=trace_id
            )
            return self._result_to_response(result)

        return GatewayResponse(404, {"error": f"no route for {method} {path}"})

    def _default_route(
        self, method: str, path: str, body: dict[str, Any]
    ) -> tuple[str, str, dict[str, Any]] | None:
        """Match default URL conventions. Returns (agent, mode, payload) or None."""
        parts = [p for p in path.strip("/").split("/") if p]
        n = len(parts)

        # POST /agents/{name}
        if method == "POST" and n == 2 and parts[0] == "agents":
            return parts[1], "call", body

        # POST /agents/{name}/cast
        if method == "POST" and n == 3 and parts[0] == "agents" and parts[2] == "cast":
            return parts[1], "cast", body

        # GET /agents/{name}/state
        if method == "GET" and n == 3 and parts[0] == "agents" and parts[2] == "state":
            return parts[1], "call", {"__op__": "state"}

        return None

    # ------------------------------------------------------------------
    # Dispatch helpers
    # ------------------------------------------------------------------

    def _result_to_response(
        self,
        result: DispatchResult,
        response_schema: Any | None = None,
        method: str = "",
        path: str = "",
    ) -> GatewayResponse:
        """Map a normalized DispatchResult onto an HTTP GatewayResponse."""
        if result.status is DispatchStatus.ACCEPTED:
            return GatewayResponse(202, {})
        if result.status is DispatchStatus.NOT_FOUND:
            return GatewayResponse(404, {"error": result.error})
        if result.status is DispatchStatus.TIMEOUT:
            return GatewayResponse(504, {"error": result.error})
        if result.status is DispatchStatus.INTERNAL:
            return GatewayResponse(500, {"error": result.error})

        # OK or AGENT_ERROR: validate the reply payload before mapping the error.
        if response_schema is not None:
            valid, err_msg = validate_response(response_schema, result.payload)
            if not valid:
                logger.error("Response validation failed for %s %s: %s", method, path, err_msg)
                return GatewayResponse(500, {"error": "response validation failed"})

        if result.status is DispatchStatus.AGENT_ERROR:
            return GatewayResponse(400, result.payload)
        return GatewayResponse(200, result.payload)

    # ------------------------------------------------------------------
    # OpenAPI / docs
    # ------------------------------------------------------------------

    def _get_openapi_spec(self) -> dict[str, Any]:
        if self._openapi_spec is None:
            self._openapi_spec = build_spec(self._route_table, self._config)
        return self._openapi_spec

    async def _serve_openapi_json(self, send: _Send) -> None:
        spec = self._get_openapi_spec()
        encoded = json.dumps(spec, indent=2).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    _CONTENT_TYPE_JSON,
                    (b"content-length", str(len(encoded)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": encoded})

    async def _serve_swagger(self, send: _Send) -> None:
        docs_path = self._config.docs_path.rstrip("/")
        html = swagger_html(docs_path + "/openapi.json").encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    _CONTENT_TYPE_HTML,
                    (b"content-length", str(len(html)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": html})

    # ------------------------------------------------------------------
    # Response helpers
    # ------------------------------------------------------------------

    async def _respond(
        self,
        send: _Send,
        response: GatewayResponse,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        encoded = json.dumps(response.body).encode()
        headers: list[tuple[bytes, bytes]] = [
            _CONTENT_TYPE_JSON,
            (b"content-length", str(len(encoded)).encode()),
        ]
        if self._config.enable_http3 and self._config.port_quic:
            headers.append((b"alt-svc", f'h3=":{self._config.port_quic}"'.encode()))
        for k, v in response.headers.items():
            headers.append((k.encode(), v.encode()))

        await send({"type": "http.response.start", "status": response.status, "headers": headers})
        await send({"type": "http.response.body", "body": encoded})
