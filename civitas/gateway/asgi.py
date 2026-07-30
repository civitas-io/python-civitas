"""ASGI callable — translates HTTP requests into Civitas messages and back."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
from email.parser import BytesParser
from email.policy import default as email_policy
from typing import TYPE_CHECKING, Any

from civitas.errors import ConfigurationError
from civitas.gateway.contracts import validate_request, validate_response
from civitas.gateway.dispatch import (
    DispatchResult,
    DispatchStatus,
    GatewayDispatcher,
    StreamSink,
    _StreamClosed,
)
from civitas.gateway.jwt_auth import _InvalidToken
from civitas.gateway.middleware import build_chain, load_middleware
from civitas.gateway.mtls import _client_cert_from_headers
from civitas.gateway.openapi import build_spec, swagger_html
from civitas.gateway.types import GatewayRequest, GatewayResponse, MiddlewareCallable
from civitas.messages import _uuid7

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

# WS bearer token transport: a single pinned Sec-WebSocket-Protocol subprotocol
# carrying the JWT as a suffix, e.g. "civitas.bearer.<jwt>" (D1).
_WS_BEARER_PREFIX = "civitas.bearer."


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


def _parse_multipart(body: bytes, content_type: str) -> dict[str, Any]:
    """Parse ``multipart/form-data`` into a JSON-serializable dict (G6).

    Text fields land under their field name; uploaded files land under
    ``__files__[name]`` as ``{filename, content_type, size, content_base64}`` so
    the resulting payload stays primitives-only (files are base64-encoded).
    """
    header = f"Content-Type: {content_type}\r\n\r\n".encode()
    parsed = BytesParser(policy=email_policy).parsebytes(header + body)
    result: dict[str, Any] = {}
    for part in parsed.iter_parts():
        name = part.get_param("name", header="content-disposition")
        if name is None:
            continue
        raw = part.get_payload(decode=True)
        content = raw if isinstance(raw, bytes) else b""
        filename = part.get_filename()
        if filename is not None:
            files: dict[str, Any] = result.setdefault("__files__", {})
            files[str(name)] = {
                "filename": filename,
                "content_type": part.get_content_type(),
                "size": len(content),
                "content_base64": base64.b64encode(content).decode(),
            }
        else:
            result[str(name)] = content.decode(errors="replace")
    return result


def _ws_parse(event: dict[str, Any]) -> dict[str, Any] | None:
    """Parse a ``websocket.receive`` event into a JSON object, or None to skip."""
    text = event.get("text")
    if text is None:
        raw = event.get("bytes")
        text = raw.decode(errors="replace") if raw else None
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else {"data": parsed}


def _extract_ws_token(subprotocols: list[str]) -> tuple[str, str] | None:
    """Return ``(subprotocol, token)`` for the first ``civitas.bearer.<jwt>`` offer.

    The token rides the ``Sec-WebSocket-Protocol`` handshake header as the suffix of
    a single pinned subprotocol string (D1). Returns ``None`` when no offered
    subprotocol carries a token.
    """
    for subprotocol in subprotocols:
        if subprotocol.startswith(_WS_BEARER_PREFIX):
            return subprotocol, subprotocol[len(_WS_BEARER_PREFIX) :]
    return None


def _client_cert_from_scope(scope: _Scope) -> dict[str, Any] | None:
    """Return the client's mTLS leaf from the ASGI TLS extension, or None.

    Exposes only the leaf (``dn`` = full subject DN, ``leaf_pem`` = leaf PEM);
    authorization is done on the full DN by ``require_client_cert``. Returns None
    when the connection is plaintext or the client presented no certificate.
    """
    tls = scope.get("extensions", {}).get("tls")
    if not tls:
        return None
    chain = tls.get("client_cert_chain")
    if not chain:
        return None
    return {"dn": tls.get("client_cert_name"), "leaf_pem": chain[0]}


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
        self._ws_routes: dict[str, str] = {r["path"]: r["agent"] for r in config.ws_routes}

        # Resolve all middleware eagerly at construction (called from the gateway's
        # on_start). A load failure now raises out of on_start and crashes the
        # supervised gateway rather than being logged-and-skipped (M1 — the old
        # behavior silently served unauthenticated when a security middleware
        # failed to import). Route-scoped middleware is resolved once per
        # RouteEntry (keyed by object identity — entries live for the lifetime of
        # the route table) and cached so repeated requests don't re-import.
        self._middlewares: list[MiddlewareCallable] = [
            self._load_or_fail(dotted_path) for dotted_path in config.middleware
        ]
        self._route_middleware_cache: dict[int, list[MiddlewareCallable]] = {
            id(entry): [self._load_or_fail(dotted_path, entry) for dotted_path in entry.middleware]
            for entry in route_table.entries()
        }

        # Cached OpenAPI spec (built lazily)
        self._openapi_spec: dict[str, Any] | None = None

    @staticmethod
    def _load_or_fail(dotted_path: str, entry: RouteEntry | None = None) -> MiddlewareCallable:
        """Import a middleware or raise ``ConfigurationError`` (fatal at startup)."""
        try:
            return load_middleware(dotted_path)
        except Exception as exc:
            if entry is not None:
                logger.error(
                    "Failed to load route middleware %r for %s %s",
                    dotted_path,
                    entry.method,
                    entry.path_pattern,
                )
            else:
                logger.error("Failed to load middleware %r", dotted_path)
            raise ConfigurationError(
                f"Failed to load gateway middleware {dotted_path!r}; refusing to start "
                "(a security middleware must never be silently skipped)"
            ) from exc

    def _route_middlewares(self, entry: RouteEntry) -> list[MiddlewareCallable]:
        """Return *entry*'s middleware, resolved eagerly at construction."""
        return self._route_middleware_cache.get(id(entry), [])

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        if scope["type"] == "lifespan":
            await self._handle_lifespan(receive, send)
        elif scope["type"] == "http":
            await self._handle_http(scope, receive, send)
        elif scope["type"] == "websocket":
            await self._handle_websocket(scope, receive, send)

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
    # WebSocket handling (G2)
    # ------------------------------------------------------------------

    async def _handle_websocket(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        """Bridge a WebSocket session to an agent: frames → cast, replies → frames.

        Each inbound frame is dispatched as a stream request keyed by a per-session
        correlation_id; the agent's emitted chunks stream back over the same socket
        via a pump task draining the session sink until the client disconnects.
        """
        agent = self._ws_routes.get(scope["path"])
        event = await receive()
        if event["type"] != "websocket.connect":
            return
        if agent is None:
            await send({"type": "websocket.close", "code": 4404})
            return

        # Auth resolves strictly before accept so no unauthenticated socket ever
        # opens (D2). JWT auto-inherits from HTTP config (D6): no verifier -> open
        # unchanged (regression guard). WS mTLS is out of scope (#25).
        if self._gateway._jwt_verifier is not None:
            extracted = _extract_ws_token(scope.get("subprotocols", []))
            if extracted is None:
                await send({"type": "websocket.close", "code": 4401})
                return
            subprotocol, token = extracted
            try:
                await self._gateway._jwt_verifier.verify(token)
            except _InvalidToken:
                await send({"type": "websocket.close", "code": 4401})
                return
            await send({"type": "websocket.accept", "subprotocol": subprotocol})
        else:
            await send({"type": "websocket.accept"})

        session_id = _uuid7()
        sink = self._gateway._open_stream(session_id)
        pump = asyncio.create_task(self._ws_pump(send, sink))
        try:
            while True:
                event = await receive()
                if event["type"] == "websocket.disconnect":
                    break
                if event["type"] == "websocket.receive":
                    payload = _ws_parse(event)
                    if payload is not None:
                        await self._gateway._send_stream_request(
                            recipient=agent,
                            payload=payload,
                            correlation_id=session_id,
                            msg_type="ws.message",
                        )
        finally:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await pump
            self._gateway._close_stream(session_id)
            with contextlib.suppress(Exception):
                await self._gateway._send_stream_request(
                    recipient=agent,
                    payload={"__session__": "closed"},
                    correlation_id=session_id,
                    msg_type="ws.close",
                )

    async def _ws_pump(self, send: _Send, sink: StreamSink) -> None:
        with contextlib.suppress(_StreamClosed):
            async for chunk in sink.drain():
                await send({"type": "websocket.send", "text": json.dumps(chunk)})

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

        # Parse body: multipart/form-data uploads (G6), else JSON (empty → {})
        content_type = headers.get("content-type", "")
        body: dict[str, Any] = {}
        if body_bytes and content_type.startswith("multipart/form-data"):
            try:
                body = _parse_multipart(body_bytes, content_type)
            except Exception:
                await self._respond(
                    send, GatewayResponse(400, {"error": "invalid multipart/form-data body"})
                )
                return
        elif body_bytes:
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

        # proxy_header mode reads the RFC 9440 Client-Cert header behind a trusted
        # proxy (D5); direct mode reads uvicorn's (never-populated) ASGI TLS extension
        # (#25). Both return the same {"dn": ...} shape into the unchanged authorizer.
        if self._config.mtls_source == "proxy_header":
            client_cert = _client_cert_from_headers(scope, self._config.trusted_proxy_cidrs)
        else:
            client_cert = _client_cert_from_scope(scope)

        request = GatewayRequest(
            method=method,
            path=path,
            path_params=matched[1] if matched is not None else {},
            query_params=query_params,
            headers=headers,
            body=body,
            client_ip=client_ip,
            gateway=self._gateway,
            client_cert=client_cert,
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

            if entry.mode == "stream":
                stream = self._dispatcher.stream(
                    recipient=entry.agent,
                    msg_type=msg_type,
                    payload=payload,
                    trace_id=trace_id,
                )
                return GatewayResponse(200, stream=stream)

            result = await self._dispatcher.dispatch(
                recipient=entry.agent,
                msg_type=msg_type,
                payload=payload,
                mode=entry.mode,
                trace_id=trace_id,
            )
            return self._result_to_response(
                result,
                response_schema=entry.response_schema,
                method=method,
                path=path,
                raw_response=entry.raw_response,
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
        raw_response: bool = False,
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

        # v0.9.5 (topology-gateway-merge.md D4): a raw_response route's OK reply
        # is {"__raw_body__": str, "__content_type__": str, "__status__": int} --
        # sent verbatim, never JSON-encoded or contract-validated (there is no
        # JSON schema for Prometheus text exposition, and TopologyAgent's own
        # 404s -- e.g. an unknown agent name -- need a status other than 200).
        # Only reachable when the ROUTE, not the reply shape, opted in -- an
        # ordinary route returning these keys is unaffected. __status__ defaults
        # to 200 so a raw-response route that never varies its status (like
        # /health) doesn't need to set it explicitly.
        if raw_response and result.status is DispatchStatus.OK:
            raw = result.payload.get("__raw_body__", "")
            content_type = result.payload.get("__content_type__", "text/plain; charset=utf-8")
            status = int(result.payload.get("__status__", 200))
            return GatewayResponse(status, raw_body=raw.encode(), content_type=content_type)

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
        if response.stream is not None:
            await self._respond_stream(send, response, extra_headers)
            return
        # v0.9.5 (topology-gateway-merge.md D4): raw_body bypasses JSON encoding
        # entirely -- Prometheus text exposition, not a JSON object.
        if response.raw_body is not None:
            encoded = response.raw_body
            content_type = response.content_type or "text/plain; charset=utf-8"
            headers = [
                (b"content-type", content_type.encode()),
                (b"content-length", str(len(encoded)).encode()),
            ]
        else:
            encoded = json.dumps(response.body).encode()
            headers = [
                _CONTENT_TYPE_JSON,
                (b"content-length", str(len(encoded)).encode()),
            ]
        if self._config.enable_http3 and self._config.port_quic:
            headers.append((b"alt-svc", f'h3=":{self._config.port_quic}"'.encode()))
        for k, v in response.headers.items():
            headers.append((k.encode(), v.encode()))
        for k, v in (extra_headers or {}).items():
            headers.append((k.encode(), v.encode()))

        await send({"type": "http.response.start", "status": response.status, "headers": headers})
        await send({"type": "http.response.body", "body": encoded})

    async def _respond_stream(
        self,
        send: _Send,
        response: GatewayResponse,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        stream = response.stream
        if stream is None:
            return
        headers: list[tuple[bytes, bytes]] = [
            (b"content-type", b"text/event-stream"),
            (b"cache-control", b"no-cache"),
        ]
        if self._config.enable_http3 and self._config.port_quic:
            headers.append((b"alt-svc", f'h3=":{self._config.port_quic}"'.encode()))
        for k, v in response.headers.items():
            headers.append((k.encode(), v.encode()))
        for k, v in (extra_headers or {}).items():
            headers.append((k.encode(), v.encode()))
        await send({"type": "http.response.start", "status": response.status, "headers": headers})

        event_id = 0
        try:
            async for chunk in stream:
                event_id += 1
                frame = f"id: {event_id}\ndata: {json.dumps(chunk)}\n\n".encode()
                await send({"type": "http.response.body", "body": frame, "more_body": True})
        except _StreamClosed as exc:
            frame = f"event: error\ndata: {json.dumps({'error': str(exc)})}\n\n".encode()
            await send({"type": "http.response.body", "body": frame, "more_body": True})
        finally:
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                await aclose()
        await send({"type": "http.response.body", "body": b"", "more_body": False})
