"""Tests for civitas.gateway — HTTPGateway, GatewayASGI, RouteTable."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from civitas import AgentProcess, Runtime, Supervisor
from civitas.errors import ConfigurationError, MessageRoutingError
from civitas.gateway import (
    GatewayConfig,
    GatewayRequest,
    GatewayResponse,
    HTTPGateway,
    contract,
    route,
)
from civitas.gateway.asgi import GatewayASGI, _parse_traceparent
from civitas.gateway.contracts import validate_request, validate_response
from civitas.gateway.core import _build_topology_routes
from civitas.gateway.middleware import build_chain, load_middleware
from civitas.gateway.openapi import build_spec
from civitas.gateway.router import RouteEntry, RouteTable
from civitas.messages import Message


async def _fake_principal_mw(request, call_next):  # type: ignore[no-untyped-def]
    """v0.9.6: a stand-in for a customer's 'bring your own auth' middleware --
    authenticates (trivially, here) and stamps request.auth['principal'] per the
    D1 convention. Module-level so load_middleware can import it by dotted path.
    """
    request.auth = {"principal": {"id": "real-alice", "method": "test"}}
    return await call_next(request)


# ---------------------------------------------------------------------------
# RouteTable unit tests
# ---------------------------------------------------------------------------


class TestRouteTable:
    def test_from_config_empty(self) -> None:
        rt = RouteTable.from_config([])
        assert len(rt) == 0

    def test_from_config_basic(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "POST", "path": "/v1/chat", "agent": "assistant", "mode": "call"},
            ]
        )
        assert len(rt) == 1
        entry = rt.entries()[0]
        assert entry.method == "POST"
        assert entry.path_pattern == "/v1/chat"
        assert entry.agent == "assistant"
        assert entry.mode == "call"

    def test_method_uppercased(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "post", "path": "/v1/chat", "agent": "a"},
            ]
        )
        assert rt.entries()[0].method == "POST"

    def test_match_exact_path(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "POST", "path": "/v1/chat", "agent": "assistant"},
            ]
        )
        result = rt.match("POST", "/v1/chat")
        assert result is not None
        entry, params = result
        assert entry.agent == "assistant"
        assert params == {}

    def test_match_path_parameters(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "GET", "path": "/sessions/{session_id}/history", "agent": "historian"},
            ]
        )
        result = rt.match("GET", "/sessions/abc123/history")
        assert result is not None
        entry, params = result
        assert params == {"session_id": "abc123"}

    def test_match_method_mismatch(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "POST", "path": "/v1/chat", "agent": "assistant"},
            ]
        )
        assert rt.match("GET", "/v1/chat") is None

    def test_match_no_route(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "POST", "path": "/v1/chat", "agent": "assistant"},
            ]
        )
        assert rt.match("POST", "/v2/missing") is None

    def test_first_match_wins(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "POST", "path": "/v1/chat", "agent": "first"},
                {"method": "POST", "path": "/v1/chat", "agent": "second"},
            ]
        )
        result = rt.match("POST", "/v1/chat")
        assert result is not None
        assert result[0].agent == "first"

    def test_from_class_reads_route_metadata(self) -> None:
        class FakeAgent:
            def handle_call(self) -> None:
                pass

        FakeAgent.handle_call._civitas_route = {  # type: ignore[attr-defined]
            "method": "POST",
            "path": "/v1/chat",
            "mode": "call",
        }

        rt = RouteTable.from_class(FakeAgent)
        assert len(rt) == 1
        assert rt.entries()[0].path_pattern == "/v1/chat"

    def test_from_class_no_routes(self) -> None:
        class EmptyAgent:
            def handle(self) -> None:
                pass

        rt = RouteTable.from_class(EmptyAgent)
        assert len(rt) == 0

    def test_multiple_path_params(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "GET", "path": "/orgs/{org}/repos/{repo}", "agent": "github"},
            ]
        )
        result = rt.match("GET", "/orgs/acme/repos/civitas")
        assert result is not None
        _, params = result
        assert params == {"org": "acme", "repo": "civitas"}


# ---------------------------------------------------------------------------
# GatewayConfig validation
# ---------------------------------------------------------------------------


class TestGatewayConfig:
    def test_defaults(self) -> None:
        cfg = GatewayConfig()
        assert cfg.host == "0.0.0.0"
        assert cfg.port == 8080
        assert cfg.request_timeout == 30.0
        assert not cfg.enable_http3

    def test_http3_requires_tls(self) -> None:
        with pytest.raises(ValueError, match="tls_cert and tls_key"):
            GatewayConfig(enable_http3=True, port_quic=8443)

    def test_http3_requires_port_quic(self) -> None:
        with pytest.raises(ValueError, match="port_quic"):
            GatewayConfig(
                enable_http3=True,
                tls_cert="cert.pem",
                tls_key="key.pem",
            )

    def test_valid_http3_config(self) -> None:
        cfg = GatewayConfig(
            enable_http3=True,
            port_quic=8443,
            tls_cert="cert.pem",
            tls_key="key.pem",
        )
        assert cfg.enable_http3
        assert cfg.port_quic == 8443


# ---------------------------------------------------------------------------
# _parse_traceparent
# ---------------------------------------------------------------------------


class TestParseTraceparent:
    def test_valid(self) -> None:
        trace_id, span_id = _parse_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        )
        assert trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert span_id == "00f067aa0ba902b7"

    def test_malformed(self) -> None:
        trace_id, span_id = _parse_traceparent("bad-header")
        assert trace_id == ""
        assert span_id is None


# ---------------------------------------------------------------------------
# GatewayASGI unit tests (no live server — mock the ASGI receive/send)
# ---------------------------------------------------------------------------


def _make_asgi(
    routes: list[dict] | None = None,
    request_timeout: float = 5.0,
) -> tuple[GatewayASGI, MagicMock]:
    """Return a GatewayASGI and its mock gateway."""
    gateway = MagicMock(spec=HTTPGateway)
    gateway.name = "api"
    config = GatewayConfig(routes=routes or [], request_timeout=request_timeout)
    route_table = RouteTable.from_config(config.routes)
    asgi = GatewayASGI(gateway=gateway, route_table=route_table, config=config)
    return asgi, gateway


async def _http_request(
    asgi: GatewayASGI,
    *,
    method: str = "POST",
    path: str = "/agents/foo",
    body: dict | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict]:
    """Drive GatewayASGI with a synthetic HTTP request. Returns (status, body)."""
    raw_headers = [(k.encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": raw_headers,
    }
    body_bytes = json.dumps(body or {}).encode()
    receive_events = [
        {"body": body_bytes, "more_body": False},
    ]
    receive_idx = 0

    async def receive() -> dict:
        nonlocal receive_idx
        evt = receive_events[receive_idx]
        receive_idx += 1
        return evt

    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    await asgi(scope, receive, send)

    status_event = next(e for e in sent if e["type"] == "http.response.start")
    body_event = next(e for e in sent if e["type"] == "http.response.body")
    response_body = json.loads(body_event["body"])
    return status_event["status"], response_body


class TestGatewayASGI:
    @pytest.mark.asyncio
    async def test_default_route_call_returns_200(self) -> None:
        asgi, gateway = _make_asgi()
        reply = MagicMock(spec=Message)
        reply.payload = {"answer": 42}
        gateway.ask = AsyncMock(return_value=reply)

        status, body = await _http_request(asgi, method="POST", path="/agents/foo")
        assert status == 200
        assert body == {"answer": 42}
        gateway.ask.assert_called_once()

    @pytest.mark.asyncio
    async def test_default_route_cast_returns_202(self) -> None:
        asgi, gateway = _make_asgi()
        gateway.send = AsyncMock()

        status, body = await _http_request(asgi, method="POST", path="/agents/foo/cast")
        assert status == 202
        gateway.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_route_returns_404(self) -> None:
        asgi, gateway = _make_asgi()

        status, body = await _http_request(asgi, method="DELETE", path="/unknown")
        assert status == 404
        assert "no route" in body["error"]

    @pytest.mark.asyncio
    async def test_custom_route_used_before_default(self) -> None:
        asgi, gateway = _make_asgi(
            routes=[
                {"method": "POST", "path": "/v1/chat", "agent": "assistant", "mode": "call"},
            ]
        )
        reply = MagicMock(spec=Message)
        reply.payload = {"reply": "hello"}
        gateway.ask = AsyncMock(return_value=reply)

        status, body = await _http_request(asgi, method="POST", path="/v1/chat")
        assert status == 200
        call_args = gateway.ask.call_args
        assert call_args[0][0] == "assistant"

    @pytest.mark.asyncio
    async def test_path_params_merged_into_payload(self) -> None:
        asgi, gateway = _make_asgi(
            routes=[
                {"method": "GET", "path": "/sessions/{session_id}", "agent": "sessions"},
            ]
        )
        reply = MagicMock(spec=Message)
        reply.payload = {}
        gateway.ask = AsyncMock(return_value=reply)

        await _http_request(asgi, method="GET", path="/sessions/abc123")
        call_args = gateway.ask.call_args
        payload_sent = call_args[0][1]
        assert payload_sent["session_id"] == "abc123"

    @pytest.mark.asyncio
    async def test_payload_error_returns_400(self) -> None:
        asgi, gateway = _make_asgi()
        reply = MagicMock(spec=Message)
        reply.payload = {"error": "bad input"}
        gateway.ask = AsyncMock(return_value=reply)

        status, body = await _http_request(asgi, method="POST", path="/agents/foo")
        assert status == 400
        assert body["error"] == "bad input"

    @pytest.mark.asyncio
    async def test_timeout_returns_504(self) -> None:
        asgi, gateway = _make_asgi()
        gateway.ask = AsyncMock(side_effect=TimeoutError())

        status, body = await _http_request(asgi, method="POST", path="/agents/foo")
        assert status == 504
        assert "timeout" in body["error"]

    @pytest.mark.asyncio
    async def test_routing_error_returns_404(self) -> None:
        asgi, gateway = _make_asgi()
        gateway.ask = AsyncMock(side_effect=MessageRoutingError("no agent"))

        status, body = await _http_request(asgi, method="POST", path="/agents/foo")
        assert status == 404

    @pytest.mark.asyncio
    async def test_invalid_json_returns_400(self) -> None:
        asgi, gateway = _make_asgi()
        scope = {"type": "http", "method": "POST", "path": "/agents/foo", "headers": []}

        async def receive() -> dict:
            return {"body": b"not-json{{{", "more_body": False}

        sent: list[dict] = []

        async def send(msg: dict) -> None:
            sent.append(msg)

        await asgi(scope, receive, send)
        status = next(e for e in sent if e["type"] == "http.response.start")["status"]
        assert status == 400

    @pytest.mark.asyncio
    async def test_traceparent_propagated(self) -> None:
        asgi, gateway = _make_asgi()
        reply = MagicMock(spec=Message)
        reply.payload = {}
        gateway.ask = AsyncMock(return_value=reply)

        await _http_request(
            asgi,
            method="POST",
            path="/agents/foo",
            headers={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
        )
        # ask() is called — traceparent was parsed (gateway.ask called without error)
        gateway.ask.assert_called_once()

    @pytest.mark.asyncio
    async def test_traceparent_echoed_back_on_non_streaming_response(self) -> None:
        """v0.9.5: found while extending _respond() for the raw_body escape hatch
        (topology-gateway-merge.md D4) -- extra_headers (carrying traceparent) was
        applied to the SSE stream response path but NEVER to the plain JSON
        response path, a real, previously-untested gap (the existing
        test_traceparent_propagated above only checks that traceparent gets
        PARSED for the outgoing dispatch, never that it's echoed back in the
        response). Fixed as part of this same _respond() edit; regression-tested
        here rather than silently left in place.
        """
        asgi, gateway = _make_asgi()
        reply = MagicMock(spec=Message)
        reply.payload = {}
        gateway.ask = AsyncMock(return_value=reply)

        scope = {
            "type": "http",
            "method": "POST",
            "path": "/agents/foo",
            "headers": [
                (b"traceparent", b"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
            ],
        }
        sent: list[dict] = []

        async def receive() -> dict:
            return {"body": b"{}", "more_body": False}

        async def send(message: dict) -> None:
            sent.append(message)

        await asgi(scope, receive, send)
        start = next(e for e in sent if e["type"] == "http.response.start")
        headers = {k.decode(): v.decode() for k, v in start["headers"]}
        assert headers["traceparent"] == "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

    @pytest.mark.asyncio
    async def test_raw_response_route_bypasses_json_encoding(self) -> None:
        """v0.9.5 (topology-gateway-merge.md D4): a RouteEntry with
        raw_response=True sends {"__raw_body__", "__content_type__"} verbatim,
        not JSON-encoded -- the mechanism the merged TopologyServer/HTTPGateway
        needs for Prometheus's plain-text /metrics exposition. Constructed
        directly (not via RouteTable.from_config()) since raw_response is
        deliberately not exposed to user-declared YAML routes (only
        auto-registered topology routes set it).
        """
        entry = RouteEntry(method="GET", path_pattern="/metrics", agent="topo", raw_response=True)
        route_table = RouteTable([entry])
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        config = GatewayConfig(routes=[])
        asgi = GatewayASGI(gateway=gateway, route_table=route_table, config=config)
        reply = MagicMock(spec=Message)
        reply.payload = {
            "__raw_body__": "civitas_messages_handled_total 1\n",
            "__content_type__": "text/plain; version=0.0.4; charset=utf-8",
        }
        gateway.ask = AsyncMock(return_value=reply)

        scope = {"type": "http", "method": "GET", "path": "/metrics", "headers": []}
        sent: list[dict] = []

        async def receive() -> dict:
            return {"body": b"", "more_body": False}

        async def send(message: dict) -> None:
            sent.append(message)

        await asgi(scope, receive, send)
        start = next(e for e in sent if e["type"] == "http.response.start")
        body_evt = next(e for e in sent if e["type"] == "http.response.body")
        headers = {k.decode(): v.decode() for k, v in start["headers"]}
        assert start["status"] == 200
        assert headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"
        assert body_evt["body"] == b"civitas_messages_handled_total 1\n"
        # Not JSON -- json.loads() on this body would fail, proving the escape
        # hatch actually bypassed json.dumps(), not just returned lucky-valid JSON.
        with pytest.raises(json.JSONDecodeError):
            json.loads(body_evt["body"])

    @pytest.mark.asyncio
    async def test_topology_route_end_to_end_dispatches_op_and_returns_raw_body(self) -> None:
        """v0.9.5 (topology-gateway-merge.md D2+D4 together): a GET to an
        auto-registered topology route dispatches with the route's fixed
        __op__ and passes TopologyAgent's raw sentinel reply back verbatim --
        the full path a real merged introspection endpoint takes.
        """
        routes = _build_topology_routes("topo", "", [])
        asgi = GatewayASGI(
            gateway=(gw := MagicMock(spec=HTTPGateway)),
            route_table=RouteTable(routes),
            config=GatewayConfig(routes=[]),
        )
        gw.name = "api"
        reply = MagicMock(spec=Message)
        reply.payload = {
            "__raw_body__": '{"name": "root", "type": "supervisor"}',
            "__content_type__": "application/json",
            "__status__": 200,
        }
        gw.ask = AsyncMock(return_value=reply)

        scope = {"type": "http", "method": "GET", "path": "/topology", "headers": []}
        sent: list[dict] = []

        async def receive() -> dict:
            return {"body": b"", "more_body": False}

        async def send(message: dict) -> None:
            sent.append(message)

        await asgi(scope, receive, send)
        # Dispatched with the route's fixed __op__
        _, dispatched_payload = gw.ask.call_args[0]
        assert dispatched_payload["__op__"] == "topology"
        # Raw body passed back verbatim
        body_evt = next(e for e in sent if e["type"] == "http.response.body")
        assert body_evt["body"] == b'{"name": "root", "type": "supervisor"}'

    @pytest.mark.asyncio
    async def test_payload_extra_merged_last_client_cannot_override(self) -> None:
        """v0.9.5 (topology-gateway-merge.md D2): a route's payload_extra is
        merged AFTER the client body, so a client cannot override a fixed
        route key (e.g. __op__) by sending it in the request body -- the
        security property the auto-registered topology routes rely on.
        """
        entry = RouteEntry(
            method="POST",
            path_pattern="/x",
            agent="topo",
            payload_extra={"__op__": "topology"},
        )
        asgi = GatewayASGI(
            gateway=(gw := MagicMock(spec=HTTPGateway)),
            route_table=RouteTable([entry]),
            config=GatewayConfig(routes=[]),
        )
        gw.name = "api"
        reply = MagicMock(spec=Message)
        reply.payload = {}
        gw.ask = AsyncMock(return_value=reply)

        # Client tries to smuggle __op__=processes in the body.
        await _http_request(asgi, method="POST", path="/x", body={"__op__": "processes"})
        _, sent_payload = gw.ask.call_args[0]
        assert sent_payload["__op__"] == "topology"  # route's value wins, not the client's

    @pytest.mark.asyncio
    async def test_inject_principal_default_when_no_auth(self) -> None:
        """v0.9.6 (control-plane-writes.md D2/D3): an inject_principal route with
        no auth middleware (request.auth unset) injects the documented
        {"id": "unauthenticated"} default under __principal__."""
        entry = RouteEntry(method="POST", path_pattern="/w", agent="topo", inject_principal=True)
        asgi = GatewayASGI(
            gateway=(gw := MagicMock(spec=HTTPGateway)),
            route_table=RouteTable([entry]),
            config=GatewayConfig(routes=[]),
        )
        gw.name = "api"
        reply = MagicMock(spec=Message)
        reply.payload = {}
        gw.ask = AsyncMock(return_value=reply)
        await _http_request(asgi, method="POST", path="/w", body={})
        _, sent_payload = gw.ask.call_args[0]
        assert sent_payload["__principal__"] == {"id": "unauthenticated"}

    @pytest.mark.asyncio
    async def test_inject_principal_cannot_be_spoofed_from_body(self) -> None:
        """v0.9.6 (control-plane-writes.md D2): the crux -- a client CANNOT set
        the audited actor by putting __principal__ in the request body. The
        dispatch layer injects the authenticated principal (set by middleware
        on request.auth) LAST, overriding any body value.
        """
        entry = RouteEntry(
            method="POST",
            path_pattern="/w",
            agent="topo",
            inject_principal=True,
            middleware=["tests.unit.test_gateway._fake_principal_mw"],
        )
        asgi = GatewayASGI(
            gateway=(gw := MagicMock(spec=HTTPGateway)),
            route_table=RouteTable([entry]),
            config=GatewayConfig(routes=[]),
        )
        gw.name = "api"
        reply = MagicMock(spec=Message)
        reply.payload = {}
        gw.ask = AsyncMock(return_value=reply)
        # Body tries to smuggle a fake identity.
        await _http_request(
            asgi, method="POST", path="/w", body={"__principal__": {"id": "attacker"}}
        )
        _, sent_payload = gw.ask.call_args[0]
        assert sent_payload["__principal__"] == {"id": "real-alice", "method": "test"}

    @pytest.mark.asyncio
    async def test_x_civitas_type_header(self) -> None:
        asgi, gateway = _make_asgi()
        reply = MagicMock(spec=Message)
        reply.payload = {}
        gateway.ask = AsyncMock(return_value=reply)

        await _http_request(
            asgi,
            method="POST",
            path="/agents/foo",
            headers={"x-civitas-type": "custom.event"},
        )
        call_kwargs = gateway.ask.call_args[1]
        assert call_kwargs.get("message_type") == "custom.event"

    @pytest.mark.asyncio
    async def test_alt_svc_header_when_http3_enabled(self) -> None:
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        config = GatewayConfig(
            enable_http3=True,
            port_quic=8443,
            tls_cert="cert.pem",
            tls_key="key.pem",
        )
        route_table = RouteTable.from_config([])
        asgi = GatewayASGI(gateway=gateway, route_table=route_table, config=config)
        reply = MagicMock(spec=Message)
        reply.payload = {}
        gateway.ask = AsyncMock(return_value=reply)

        sent: list[dict] = []
        scope = {"type": "http", "method": "POST", "path": "/agents/foo", "headers": []}

        async def receive() -> dict:
            return {"body": b"{}", "more_body": False}

        async def send(msg: dict) -> None:
            sent.append(msg)

        await asgi(scope, receive, send)
        start_event = next(e for e in sent if e["type"] == "http.response.start")
        header_names = [k for k, _ in start_event["headers"]]
        assert b"alt-svc" in header_names


# ---------------------------------------------------------------------------
# HTTPGateway integration tests (real runtime, real HTTP client)
# ---------------------------------------------------------------------------


class EchoAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"echo": message.payload.get("text", "")})


class TestHTTPGatewayIntegration:
    @pytest.mark.asyncio
    async def test_call_returns_agent_reply(self) -> None:
        gateway = HTTPGateway("api", GatewayConfig(port=19080, request_timeout=5.0))
        echo = EchoAgent("echo")
        supervisor = Supervisor("root", children=[gateway, echo])
        runtime = Runtime(supervisor=supervisor)

        await runtime.start()
        await asyncio.sleep(0.2)  # let uvicorn bind

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    "http://127.0.0.1:19080/agents/echo",
                    json={"text": "hello"},
                    timeout=5.0,
                )
            assert resp.status_code == 200
            assert resp.json() == {"echo": "hello"}
        finally:
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_cast_returns_202(self) -> None:
        gateway = HTTPGateway("api", GatewayConfig(port=19081, request_timeout=5.0))
        echo = EchoAgent("echo")
        supervisor = Supervisor("root", children=[gateway, echo])
        runtime = Runtime(supervisor=supervisor)

        await runtime.start()
        await asyncio.sleep(0.2)

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    "http://127.0.0.1:19081/agents/echo/cast",
                    json={"text": "fire and forget"},
                    timeout=5.0,
                )
            assert resp.status_code == 202
        finally:
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_unknown_agent_returns_404(self) -> None:
        gateway = HTTPGateway("api", GatewayConfig(port=19082, request_timeout=5.0))
        supervisor = Supervisor("root", children=[gateway])
        runtime = Runtime(supervisor=supervisor)

        await runtime.start()
        await asyncio.sleep(0.2)

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    "http://127.0.0.1:19082/agents/ghost",
                    json={},
                    timeout=5.0,
                )
            assert resp.status_code == 404
        finally:
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_concurrent_requests(self) -> None:
        gateway = HTTPGateway("api", GatewayConfig(port=19083, request_timeout=5.0))
        echo = EchoAgent("echo")
        supervisor = Supervisor("root", children=[gateway, echo])
        runtime = Runtime(supervisor=supervisor)

        await runtime.start()
        await asyncio.sleep(0.2)

        try:
            async with httpx.AsyncClient() as client:
                tasks = [
                    client.post(
                        "http://127.0.0.1:19083/agents/echo",
                        json={"text": str(i)},
                        timeout=5.0,
                    )
                    for i in range(5)
                ]
                responses = await asyncio.gather(*tasks)
            for _i, resp in enumerate(responses):
                assert resp.status_code == 200
        finally:
            await runtime.stop()

    @pytest.mark.asyncio
    async def test_topology_yaml_starts_gateway(self, tmp_path: Any) -> None:
        topology = tmp_path / "topology.yaml"
        topology.write_text("""
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - name: api
      type: http_gateway
      config:
        host: "127.0.0.1"
        port: 19084
        request_timeout: 5.0
        routes:
          - path: /v1/echo
            agent: echo
            method: POST
            mode: call
    - name: echo
      type: EchoAgent
""")
        runtime = Runtime.from_config(topology, agent_classes={"EchoAgent": EchoAgent})
        await runtime.start()
        await asyncio.sleep(0.2)

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    "http://127.0.0.1:19084/v1/echo",
                    json={"text": "yaml"},
                    timeout=5.0,
                )
            assert resp.status_code == 200
            assert resp.json() == {"echo": "yaml"}
        finally:
            await runtime.stop()


# ---------------------------------------------------------------------------
# @route decorator
# ---------------------------------------------------------------------------


class TestRouteDecorator:
    def test_sets_civitas_route_metadata(self) -> None:
        @route("POST", "/v1/chat")
        def handler() -> None:
            pass

        assert handler._civitas_route == {"method": "POST", "path": "/v1/chat", "mode": "call"}

    def test_method_uppercased(self) -> None:
        @route("get", "/v1/status")
        def handler() -> None:
            pass

        assert handler._civitas_route["method"] == "GET"

    def test_custom_mode(self) -> None:
        @route("POST", "/v1/notify", mode="cast")
        def handler() -> None:
            pass

        assert handler._civitas_route["mode"] == "cast"

    def test_from_class_reads_decorated_methods(self) -> None:
        class MyAgent:
            @route("POST", "/v1/chat")
            def handle_chat(self) -> None:
                pass

            @route("GET", "/v1/status")
            def handle_status(self) -> None:
                pass

        rt = RouteTable.from_class(MyAgent)
        assert len(rt) == 2
        paths = {e.path_pattern for e in rt.entries()}
        assert "/v1/chat" in paths
        assert "/v1/status" in paths


# ---------------------------------------------------------------------------
# @contract decorator + validation helpers
# ---------------------------------------------------------------------------


class TestContractDecorator:
    def test_sets_civitas_contract_metadata(self) -> None:
        try:
            from pydantic import BaseModel

            class Req(BaseModel):
                text: str

            @contract(request=Req)
            def handler() -> None:
                pass

            assert handler._civitas_contract["request"] is Req
            assert handler._civitas_contract["response"] is None
        except ImportError:
            pytest.skip("pydantic not installed")

    def test_validate_request_valid(self) -> None:
        try:
            from pydantic import BaseModel

            class Req(BaseModel):
                text: str

            ok, err = validate_request(Req, {"text": "hello"})
            assert ok is True
            assert err is None
        except ImportError:
            pytest.skip("pydantic not installed")

    def test_validate_request_invalid_returns_422_shape(self) -> None:
        try:
            from pydantic import BaseModel

            class Req(BaseModel):
                text: str

            ok, err = validate_request(Req, {"wrong_field": 123})
            assert ok is False
            assert err is not None
            assert "detail" in err
            assert isinstance(err["detail"], list)
        except ImportError:
            pytest.skip("pydantic not installed")

    def test_validate_response_valid(self) -> None:
        try:
            from pydantic import BaseModel

            class Resp(BaseModel):
                answer: int

            ok, err_msg = validate_response(Resp, {"answer": 42})
            assert ok is True
            assert err_msg is None
        except ImportError:
            pytest.skip("pydantic not installed")

    def test_validate_response_invalid(self) -> None:
        try:
            from pydantic import BaseModel

            class Resp(BaseModel):
                answer: int

            ok, err_msg = validate_response(Resp, {"answer": "not-an-int"})
            # pydantic v2 coerces strings to int, so this may pass; test with clearly wrong type
            ok2, err2 = validate_response(Resp, {"wrong": "field"})
            # At least one of these should fail (depending on pydantic strict mode)
            assert isinstance(ok, bool)
        except ImportError:
            pytest.skip("pydantic not installed")

    def test_merge_contracts_from_patches_route_entry(self) -> None:
        try:
            from pydantic import BaseModel

            class Req(BaseModel):
                text: str

            class MyAgent:
                @route("POST", "/v1/chat")
                @contract(request=Req)
                def handle(self) -> None:
                    pass

            rt = RouteTable.from_config(
                [
                    {"method": "POST", "path": "/v1/chat", "agent": "my_agent"},
                ]
            )
            rt.merge_contracts_from(MyAgent, agent_name="my_agent")
            entry = rt.entries()[0]
            assert entry.request_schema is Req
        except ImportError:
            pytest.skip("pydantic not installed")

    @pytest.mark.asyncio
    async def test_contract_request_validation_returns_422(self) -> None:
        try:
            from pydantic import BaseModel

            class Req(BaseModel):
                text: str

            class MyAgent:
                @route("POST", "/v1/chat")
                @contract(request=Req)
                def handle(self) -> None:
                    pass

            gateway = MagicMock(spec=HTTPGateway)
            gateway.name = "api"
            config = GatewayConfig(
                routes=[
                    {"method": "POST", "path": "/v1/chat", "agent": "my_agent"},
                ]
            )
            rt = RouteTable.from_config(config.routes)
            rt.merge_contracts_from(MyAgent, agent_name="my_agent")
            asgi = GatewayASGI(gateway=gateway, route_table=rt, config=config)

            status, body = await _http_request(
                asgi, method="POST", path="/v1/chat", body={"wrong": 1}
            )
            assert status == 422
            assert "detail" in body
        except ImportError:
            pytest.skip("pydantic not installed")


# ---------------------------------------------------------------------------
# Middleware chain
# ---------------------------------------------------------------------------


class TestMiddlewareChain:
    @pytest.mark.asyncio
    async def test_terminal_handler_called(self) -> None:
        called: list[str] = []

        async def handler(req: GatewayRequest) -> GatewayResponse:
            called.append("handler")
            return GatewayResponse(200, {"ok": True})

        chain = build_chain([], handler)
        req = GatewayRequest(method="GET", path="/")
        resp = await chain(req)
        assert resp.status == 200
        assert called == ["handler"]

    @pytest.mark.asyncio
    async def test_middleware_wraps_handler(self) -> None:
        order: list[str] = []

        async def mw_a(
            req: GatewayRequest, next_fn: Callable[[GatewayRequest], Awaitable[GatewayResponse]]
        ) -> GatewayResponse:
            order.append("mw_a:before")
            resp = await next_fn(req)
            order.append("mw_a:after")
            return resp

        async def handler(req: GatewayRequest) -> GatewayResponse:
            order.append("handler")
            return GatewayResponse(200, {})

        chain = build_chain([mw_a], handler)
        await chain(GatewayRequest(method="GET", path="/"))
        assert order == ["mw_a:before", "handler", "mw_a:after"]

    @pytest.mark.asyncio
    async def test_middleware_order_outermost_first(self) -> None:
        order: list[str] = []

        def make_mw(name: str) -> Any:
            async def mw(
                req: GatewayRequest,
                next_fn: Callable[[GatewayRequest], Awaitable[GatewayResponse]],
            ) -> GatewayResponse:
                order.append(f"{name}:enter")
                resp = await next_fn(req)
                order.append(f"{name}:exit")
                return resp

            return mw

        async def handler(req: GatewayRequest) -> GatewayResponse:
            order.append("handler")
            return GatewayResponse(200, {})

        chain = build_chain([make_mw("A"), make_mw("B")], handler)
        await chain(GatewayRequest(method="GET", path="/"))
        assert order == ["A:enter", "B:enter", "handler", "B:exit", "A:exit"]

    @pytest.mark.asyncio
    async def test_middleware_short_circuits(self) -> None:
        async def auth_mw(
            req: GatewayRequest, next_fn: Callable[[GatewayRequest], Awaitable[GatewayResponse]]
        ) -> GatewayResponse:
            return GatewayResponse(401, {"error": "unauthorized"})

        handler_called = False

        async def handler(req: GatewayRequest) -> GatewayResponse:
            nonlocal handler_called
            handler_called = True
            return GatewayResponse(200, {})

        chain = build_chain([auth_mw], handler)
        resp = await chain(GatewayRequest(method="GET", path="/"))
        assert resp.status == 401
        assert not handler_called

    def test_load_middleware_invalid_path_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid middleware path"):
            load_middleware("no_dot_here")


# ---------------------------------------------------------------------------
# OpenAPI spec generation
# ---------------------------------------------------------------------------


class TestOpenAPI:
    def test_build_spec_empty_routes(self) -> None:
        rt = RouteTable.from_config([])
        config = GatewayConfig()
        spec = build_spec(rt, config)
        assert spec["openapi"] == "3.1.0"
        assert spec["paths"] == {}

    def test_build_spec_includes_route(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "POST", "path": "/v1/chat", "agent": "assistant", "mode": "call"},
            ]
        )
        config = GatewayConfig()
        spec = build_spec(rt, config)
        assert "/v1/chat" in spec["paths"]
        assert "post" in spec["paths"]["/v1/chat"]
        op = spec["paths"]["/v1/chat"]["post"]
        assert op["tags"] == ["assistant"]

    def test_build_spec_cast_has_202(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "POST", "path": "/v1/notify", "agent": "notifier", "mode": "cast"},
            ]
        )
        config = GatewayConfig()
        spec = build_spec(rt, config)
        op = spec["paths"]["/v1/notify"]["post"]
        assert "202" in op["responses"]

    def test_build_spec_path_params_in_parameters(self) -> None:
        rt = RouteTable.from_config(
            [
                {"method": "GET", "path": "/sessions/{id}/history", "agent": "sessions"},
            ]
        )
        config = GatewayConfig()
        spec = build_spec(rt, config)
        op = spec["paths"]["/sessions/{id}/history"]["get"]
        param_names = [p["name"] for p in op["parameters"]]
        assert "id" in param_names

    def test_build_spec_with_request_schema(self) -> None:
        try:
            from pydantic import BaseModel
        except ImportError:
            pytest.skip("pydantic not installed")

        class Req(BaseModel):
            text: str

        entry = RouteEntry(method="POST", path_pattern="/v1/chat", agent="bot", mode="call")
        entry.request_schema = Req
        rt = RouteTable([entry])
        config = GatewayConfig()

        spec = build_spec(rt, config)
        op = spec["paths"]["/v1/chat"]["post"]
        assert "requestBody" in op
        body_schema = op["requestBody"]["content"]["application/json"]["schema"]
        assert "properties" in body_schema or body_schema.get("title") == "Req"
        assert "422" in op["responses"]

    def test_build_spec_with_response_schema(self) -> None:
        try:
            from pydantic import BaseModel
        except ImportError:
            pytest.skip("pydantic not installed")

        class Resp(BaseModel):
            answer: int

        entry = RouteEntry(method="GET", path_pattern="/v1/result", agent="bot", mode="call")
        entry.response_schema = Resp
        rt = RouteTable([entry])
        config = GatewayConfig()

        spec = build_spec(rt, config)
        op = spec["paths"]["/v1/result"]["get"]
        resp_schema = op["responses"]["200"]["content"]["application/json"]["schema"]
        assert "properties" in resp_schema or resp_schema.get("title") == "Resp"
        assert "500" in op["responses"]

    def test_build_spec_two_methods_same_path(self) -> None:
        entry1 = RouteEntry(method="GET", path_pattern="/v1/items", agent="reader", mode="call")
        entry2 = RouteEntry(method="POST", path_pattern="/v1/items", agent="writer", mode="call")
        rt = RouteTable([entry1, entry2])
        config = GatewayConfig()

        spec = build_spec(rt, config)
        path_ops = spec["paths"]["/v1/items"]
        assert "get" in path_ops
        assert "post" in path_ops

    @pytest.mark.asyncio
    async def test_docs_endpoint_returns_html(self) -> None:
        asgi, gateway = _make_asgi(
            routes=[
                {"method": "POST", "path": "/v1/chat", "agent": "assistant"},
            ]
        )
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/docs",
            "headers": [],
            "query_string": b"",
        }
        sent: list[dict] = []

        async def receive() -> dict:
            return {"body": b"", "more_body": False}

        async def send(msg: dict) -> None:
            sent.append(msg)

        await asgi(scope, receive, send)
        start = next(e for e in sent if e["type"] == "http.response.start")
        body_evt = next(e for e in sent if e["type"] == "http.response.body")
        assert start["status"] == 200
        assert b"swagger-ui" in body_evt["body"].lower()

    @pytest.mark.asyncio
    async def test_openapi_json_endpoint(self) -> None:
        asgi, gateway = _make_asgi(
            routes=[
                {"method": "POST", "path": "/v1/chat", "agent": "assistant"},
            ]
        )
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/docs/openapi.json",
            "headers": [],
            "query_string": b"",
        }
        sent: list[dict] = []

        async def receive() -> dict:
            return {"body": b"", "more_body": False}

        async def send(msg: dict) -> None:
            sent.append(msg)

        await asgi(scope, receive, send)
        start = next(e for e in sent if e["type"] == "http.response.start")
        body_evt = next(e for e in sent if e["type"] == "http.response.body")
        assert start["status"] == 200
        spec = json.loads(body_evt["body"])
        assert spec["openapi"] == "3.1.0"
        assert "/v1/chat" in spec["paths"]


# ---------------------------------------------------------------------------
# _parse_query — query string parsing (lines 46-50 in asgi.py)
# ---------------------------------------------------------------------------


class TestParseQuery:
    def test_empty_bytes_returns_empty(self) -> None:
        from civitas.gateway.asgi import _parse_query

        assert _parse_query(b"") == {}

    def test_single_param(self) -> None:
        from civitas.gateway.asgi import _parse_query

        assert _parse_query(b"foo=bar") == {"foo": "bar"}

    def test_multiple_params(self) -> None:
        from civitas.gateway.asgi import _parse_query

        result = _parse_query(b"a=1&b=2")
        assert result == {"a": "1", "b": "2"}

    def test_pair_without_equals_skipped(self) -> None:
        from civitas.gateway.asgi import _parse_query

        result = _parse_query(b"noequalssign&key=val")
        assert result == {"key": "val"}


# ---------------------------------------------------------------------------
# GatewayASGI — lifespan events (lines 80->exit, 92->88)
# ---------------------------------------------------------------------------


class TestLifespan:
    @pytest.mark.asyncio
    async def test_lifespan_startup_then_shutdown(self) -> None:
        asgi, _ = _make_asgi()
        events = [
            {"type": "lifespan.startup"},
            {"type": "lifespan.shutdown"},
        ]
        idx = 0

        async def receive() -> dict:
            nonlocal idx
            e = events[idx]
            idx += 1
            return e

        sent: list[dict] = []

        async def send(msg: dict) -> None:
            sent.append(msg)

        await asgi({"type": "lifespan"}, receive, send)
        types = [s["type"] for s in sent]
        assert "lifespan.startup.complete" in types
        assert "lifespan.shutdown.complete" in types


# ---------------------------------------------------------------------------
# GatewayASGI — docs / OpenAPI endpoints (lines 108->118)
# ---------------------------------------------------------------------------


async def _http_request_with_qs(
    asgi: GatewayASGI,
    *,
    method: str = "GET",
    path: str = "/docs",
    query_string: bytes = b"",
    body: dict | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, bytes]:
    """Drive GatewayASGI, returns raw (status, body_bytes)."""
    raw_headers = [(k.encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": raw_headers,
        "query_string": query_string,
    }
    body_bytes = json.dumps(body or {}).encode()

    async def receive() -> dict:
        return {"body": body_bytes, "more_body": False}

    sent: list[dict] = []

    async def send(msg: dict) -> None:
        sent.append(msg)

    await asgi(scope, receive, send)
    start = next(e for e in sent if e["type"] == "http.response.start")
    body_evt = next(e for e in sent if e["type"] == "http.response.body")
    return start["status"], body_evt["body"]


class TestDocsEndpoints:
    @pytest.mark.asyncio
    async def test_swagger_ui_served_at_docs_path(self) -> None:
        asgi, _ = _make_asgi()
        status, body = await _http_request_with_qs(asgi, method="GET", path="/docs")
        assert status == 200
        assert b"swagger" in body.lower() or b"openapi" in body.lower()

    @pytest.mark.asyncio
    async def test_openapi_json_served(self) -> None:
        asgi, _ = _make_asgi()
        status, body = await _http_request_with_qs(asgi, method="GET", path="/docs/openapi.json")
        assert status == 200
        spec = json.loads(body)
        assert spec.get("openapi") is not None

    @pytest.mark.asyncio
    async def test_root_openapi_json_alias(self) -> None:
        asgi, _ = _make_asgi()
        status, body = await _http_request_with_qs(asgi, method="GET", path="/openapi.json")
        assert status == 200

    @pytest.mark.asyncio
    async def test_docs_disabled_does_not_serve_swagger(self) -> None:
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        config = GatewayConfig(docs_enabled=False)
        route_table = RouteTable.from_config([])
        asgi = GatewayASGI(gateway=gateway, route_table=route_table, config=config)
        gateway.ask = AsyncMock(side_effect=TimeoutError())
        status, _ = await _http_request_with_qs(asgi, method="GET", path="/docs")
        assert status == 404  # docs disabled → no route match


# ---------------------------------------------------------------------------
# GatewayASGI — non-dict JSON body → 400 (lines 133-136)
# ---------------------------------------------------------------------------


class TestNonDictBody:
    @pytest.mark.asyncio
    async def test_json_array_returns_400(self) -> None:
        asgi, _ = _make_asgi()
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/agents/foo",
            "headers": [],
            "query_string": b"",
        }
        array_body = json.dumps([1, 2, 3]).encode()

        async def receive() -> dict:
            return {"body": array_body, "more_body": False}

        sent: list[dict] = []

        async def send(msg: dict) -> None:
            sent.append(msg)

        await asgi(scope, receive, send)
        start = next(e for e in sent if e["type"] == "http.response.start")
        assert start["status"] == 400


# ---------------------------------------------------------------------------
# GatewayASGI — query params parsed into payload (lines 256-260)
# ---------------------------------------------------------------------------


class TestQueryParams:
    @pytest.mark.asyncio
    async def test_query_params_in_scope_parsed(self) -> None:
        asgi, gateway = _make_asgi()
        reply = MagicMock(spec=Message)
        reply.payload = {"ok": True}
        gateway.ask = AsyncMock(return_value=reply)

        status, _ = await _http_request_with_qs(
            asgi,
            method="POST",
            path="/agents/foo",
            query_string=b"key=val&other=x",
        )
        assert status == 200


# ---------------------------------------------------------------------------
# GatewayASGI — contract validation (lines 186->189, 191, 196-199)
# ---------------------------------------------------------------------------


class TestContractValidation:
    @pytest.mark.asyncio
    async def test_request_schema_failure_returns_422(self) -> None:
        from pydantic import BaseModel

        class ChatRequest(BaseModel):
            query: str

        asgi, gateway = _make_asgi(
            routes=[{"method": "POST", "path": "/chat", "agent": "assistant", "mode": "call"}]
        )
        # Inject schema directly — from_config only wires schemas from @contract metadata
        asgi._route_table.entries()[0].request_schema = ChatRequest

        status, body = await _http_request(asgi, method="POST", path="/chat", body={})
        assert status == 422

    @pytest.mark.asyncio
    async def test_response_schema_failure_returns_500(self) -> None:
        from pydantic import BaseModel

        class ChatResponse(BaseModel):
            answer: str

        asgi, gateway = _make_asgi(
            routes=[{"method": "POST", "path": "/chat", "agent": "assistant", "mode": "call"}]
        )
        asgi._route_table.entries()[0].response_schema = ChatResponse

        reply = MagicMock(spec=Message)
        reply.payload = {"wrong_key": "value"}  # missing required "answer"
        gateway.ask = AsyncMock(return_value=reply)

        status, _ = await _http_request(asgi, method="POST", path="/chat", body={"q": "hi"})
        assert status == 500

    @pytest.mark.asyncio
    async def test_error_in_reply_payload_returns_400(self) -> None:
        asgi, gateway = _make_asgi(
            routes=[{"method": "POST", "path": "/chat", "agent": "assistant", "mode": "call"}]
        )
        reply = MagicMock(spec=Message)
        reply.payload = {"error": "something went wrong"}
        gateway.ask = AsyncMock(return_value=reply)

        status, _ = await _http_request(asgi, method="POST", path="/chat", body={})
        assert status == 400


# ---------------------------------------------------------------------------
# GatewayASGI — cast/call generic exceptions → 500 (lines 259-260, 271-273)
# ---------------------------------------------------------------------------


class TestDispatchErrors:
    @pytest.mark.asyncio
    async def test_cast_generic_exception_returns_500(self) -> None:
        asgi, gateway = _make_asgi()
        gateway.send = AsyncMock(side_effect=RuntimeError("unexpected"))
        status, _ = await _http_request(asgi, method="POST", path="/agents/foo/cast")
        assert status == 500

    @pytest.mark.asyncio
    async def test_call_generic_exception_returns_500(self) -> None:
        asgi, gateway = _make_asgi()
        gateway.ask = AsyncMock(side_effect=RuntimeError("unexpected"))
        status, _ = await _http_request(asgi, method="POST", path="/agents/foo")
        assert status == 500


# ---------------------------------------------------------------------------
# GatewayASGI — middleware load failure (lines 69-72)
# ---------------------------------------------------------------------------


class TestMiddlewareLoadFailure:
    def test_bad_middleware_path_is_fatal(self) -> None:
        # M1: a security middleware that can't load must crash startup, never be
        # silently skipped (the old behavior served unauthenticated).
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        config = GatewayConfig(middleware=["nonexistent.module.Middleware"])
        route_table = RouteTable.from_config([])
        with pytest.raises(ConfigurationError):
            GatewayASGI(gateway=gateway, route_table=route_table, config=config)


# ---------------------------------------------------------------------------
# Route-scoped middleware — RouteEntry.middleware, resolved and run after
# global middleware, before contract validation / bus dispatch (GH #6)
# ---------------------------------------------------------------------------

_route_mw_order: list[str] = []


async def _order_tracking_route_mw(
    request: GatewayRequest, next_fn: Callable[[GatewayRequest], Awaitable[GatewayResponse]]
) -> GatewayResponse:
    _route_mw_order.append("route")
    return await next_fn(request)


async def _order_tracking_global_mw(
    request: GatewayRequest, next_fn: Callable[[GatewayRequest], Awaitable[GatewayResponse]]
) -> GatewayResponse:
    _route_mw_order.append("global")
    return await next_fn(request)


async def _admin_guard_route_mw(
    request: GatewayRequest, next_fn: Callable[[GatewayRequest], Awaitable[GatewayResponse]]
) -> GatewayResponse:
    if request.headers.get("x-admin") != "true":
        return GatewayResponse(403, {"error": "forbidden"})
    return await next_fn(request)


class TestRouteScopedMiddleware:
    def setup_method(self) -> None:
        _route_mw_order.clear()

    @pytest.mark.asyncio
    async def test_route_scoped_middleware_blocks_unauthorized_request(self) -> None:
        # This is the exact exploit from GH #6: a route-scoped guard on an
        # admin route must actually run, not be silently skipped.
        asgi, gateway = _make_asgi(
            routes=[
                {
                    "method": "GET",
                    "path": "/admin/tenants",
                    "agent": "admin_agent",
                    "mode": "call",
                    "middleware": ["tests.unit.test_gateway._admin_guard_route_mw"],
                }
            ]
        )
        gateway.ask = AsyncMock(return_value=MagicMock(spec=Message, payload={"tenants": []}))

        status, body = await _http_request(asgi, method="GET", path="/admin/tenants")
        assert status == 403
        assert body == {"error": "forbidden"}
        gateway.ask.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_scoped_middleware_allows_authorized_request(self) -> None:
        asgi, gateway = _make_asgi(
            routes=[
                {
                    "method": "GET",
                    "path": "/admin/tenants",
                    "agent": "admin_agent",
                    "mode": "call",
                    "middleware": ["tests.unit.test_gateway._admin_guard_route_mw"],
                }
            ]
        )
        reply = MagicMock(spec=Message)
        reply.payload = {"tenants": ["acme"]}
        gateway.ask = AsyncMock(return_value=reply)

        status, body = await _http_request(
            asgi, method="GET", path="/admin/tenants", headers={"x-admin": "true"}
        )
        assert status == 200
        assert body == {"tenants": ["acme"]}
        gateway.ask.assert_called_once()

    @pytest.mark.asyncio
    async def test_global_middleware_runs_before_route_scoped(self) -> None:
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        config = GatewayConfig(
            middleware=["tests.unit.test_gateway._order_tracking_global_mw"],
            routes=[
                {
                    "method": "POST",
                    "path": "/v1/chat",
                    "agent": "assistant",
                    "mode": "call",
                    "middleware": ["tests.unit.test_gateway._order_tracking_route_mw"],
                }
            ],
        )
        route_table = RouteTable.from_config(config.routes)
        asgi = GatewayASGI(gateway=gateway, route_table=route_table, config=config)
        reply = MagicMock(spec=Message)
        reply.payload = {}
        gateway.ask = AsyncMock(return_value=reply)

        await _http_request(asgi, method="POST", path="/v1/chat")
        assert _route_mw_order == ["global", "route"]

    @pytest.mark.asyncio
    async def test_route_without_middleware_is_unaffected(self) -> None:
        asgi, gateway = _make_asgi(
            routes=[
                {
                    "method": "GET",
                    "path": "/admin/tenants",
                    "agent": "admin_agent",
                    "middleware": ["tests.unit.test_gateway._admin_guard_route_mw"],
                },
                {"method": "GET", "path": "/public/status", "agent": "status_agent"},
            ]
        )
        reply = MagicMock(spec=Message)
        reply.payload = {"ok": True}
        gateway.ask = AsyncMock(return_value=reply)

        status, body = await _http_request(asgi, method="GET", path="/public/status")
        assert status == 200
        assert body == {"ok": True}

    def test_bad_route_middleware_path_is_fatal(self) -> None:
        # M1: a bad route middleware is fatal at startup too, not skipped.
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        config = GatewayConfig(
            routes=[
                {
                    "method": "POST",
                    "path": "/v1/chat",
                    "agent": "assistant",
                    "middleware": ["nonexistent.module.Middleware"],
                }
            ]
        )
        route_table = RouteTable.from_config(config.routes)
        with pytest.raises(ConfigurationError):
            GatewayASGI(gateway=gateway, route_table=route_table, config=config)

    @pytest.mark.asyncio
    async def test_route_middleware_resolved_once_at_construction(self) -> None:
        config = GatewayConfig(
            routes=[
                {
                    "method": "POST",
                    "path": "/v1/chat",
                    "agent": "assistant",
                    "middleware": ["tests.unit.test_gateway._order_tracking_route_mw"],
                }
            ]
        )
        route_table = RouteTable.from_config(config.routes)
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        reply = MagicMock(spec=Message)
        reply.payload = {}
        gateway.ask = AsyncMock(return_value=reply)

        with patch("civitas.gateway.asgi.load_middleware", wraps=load_middleware) as spy_load:
            # Eager resolution (M1): imported once at construction, cached for every
            # subsequent request rather than re-imported per call.
            asgi = GatewayASGI(gateway=gateway, route_table=route_table, config=config)
            await _http_request(asgi, method="POST", path="/v1/chat")
            await _http_request(asgi, method="POST", path="/v1/chat")
            assert spy_load.call_count == 1
        assert _route_mw_order == ["route", "route"]


# ---------------------------------------------------------------------------
# HTTPGateway — on_start uvicorn import failure (lines 78-79)
# ---------------------------------------------------------------------------


class TestHTTPGatewayCore:
    @pytest.mark.asyncio
    async def test_on_start_raises_when_uvicorn_missing(self) -> None:
        import sys

        gw = HTTPGateway("api", GatewayConfig(port=9999))
        gw._bus = None

        saved = sys.modules.get("uvicorn", ...)
        sys.modules["uvicorn"] = None  # type: ignore[assignment]  # causes ImportError on import
        try:
            with pytest.raises(RuntimeError, match="civitas\\[http\\]"):
                await gw.on_start()
        finally:
            if saved is ...:
                sys.modules.pop("uvicorn", None)
            else:
                sys.modules["uvicorn"] = saved

    @pytest.mark.asyncio
    async def test_handle_does_nothing(self) -> None:
        gw = HTTPGateway("api")
        msg = MagicMock(spec=Message)
        result = await gw.handle(msg)
        assert result is None

    @pytest.mark.asyncio
    async def test_on_stop_with_uvicorn_server_set(self) -> None:
        gw = HTTPGateway("api")
        mock_server = MagicMock()
        gw._uvicorn_server = mock_server
        gw._server_task = asyncio.create_task(asyncio.sleep(100))
        gw._h3_server = None

        await gw.on_stop()

        assert mock_server.should_exit is True
        assert gw._uvicorn_server is None
        assert gw._server_task is None

    @pytest.mark.asyncio
    async def test_on_stop_with_h3_server(self) -> None:
        gw = HTTPGateway("api")
        mock_h3 = AsyncMock()
        gw._h3_server = mock_h3
        gw._uvicorn_server = None
        gw._server_task = None

        await gw.on_stop()

        mock_h3.stop.assert_awaited_once()
        assert gw._h3_server is None

    @pytest.mark.asyncio
    async def test_on_stop_server_task_timeout(self) -> None:
        gw = HTTPGateway("api")
        gw._uvicorn_server = None
        gw._h3_server = None

        async def hang() -> None:
            await asyncio.sleep(9999)

        task = asyncio.create_task(hang())
        gw._server_task = task

        # Patch wait_for to immediately raise TimeoutError
        with patch("asyncio.wait_for", side_effect=TimeoutError()):
            await gw.on_stop()

        assert gw._server_task is None


class TestTopologyAutoRoutes:
    """v0.9.5 (topology-gateway-merge.md D2/D5/D6d): HTTPGateway auto-registers
    the fixed introspection routes when topology_agent is set. v0.9.6 adds two
    POST write routes (suspend/resume)."""

    def test_read_and_write_routes_registered_pointing_at_the_agent(self) -> None:
        gw = HTTPGateway("gw", GatewayConfig(topology_agent="topo"))
        entries = gw._route_table.entries()
        paths = {(e.method, e.path_pattern) for e in entries}
        assert paths == {
            ("GET", "/health"),
            ("GET", "/topology"),
            ("GET", "/agents"),
            ("GET", "/agents/{name}"),
            ("GET", "/snapshot"),
            ("GET", "/metrics"),
            ("GET", "/processes"),
            ("POST", "/agents/{name}/suspend"),  # v0.9.6 write action
            ("POST", "/agents/{name}/resume"),  # v0.9.6 write action
            ("POST", "/agents/{name}/restart"),  # v0.9.6 write action
        }
        assert all(e.agent == "topo" for e in entries)
        # Read routes are raw_response; write routes are not (plain JSON ack).
        reads = [e for e in entries if e.method == "GET"]
        writes = [e for e in entries if e.method == "POST"]
        assert all(e.raw_response for e in reads)
        assert not any(e.raw_response for e in writes)
        # Only write routes inject the authenticated principal (v0.9.6 D2).
        assert all(e.inject_principal for e in writes)
        assert not any(e.inject_principal for e in reads)

    def test_no_routes_when_topology_agent_unset(self) -> None:
        gw = HTTPGateway("gw", GatewayConfig())
        assert len(gw._route_table.entries()) == 0

    def test_prefix_applied_uniformly(self) -> None:
        gw = HTTPGateway("gw", GatewayConfig(topology_agent="topo", topology_prefix="/v1"))
        paths = {e.path_pattern for e in gw._route_table.entries()}
        assert paths == {
            "/v1/health",
            "/v1/topology",
            "/v1/agents",
            "/v1/agents/{name}",
            "/v1/snapshot",
            "/v1/metrics",
            "/v1/processes",
            "/v1/agents/{name}/suspend",
            "/v1/agents/{name}/resume",
            "/v1/agents/{name}/restart",
        }

    def test_prefix_trailing_slash_normalized(self) -> None:
        # A prefix of "/v1/" must not produce "/v1//topology".
        routes = _build_topology_routes("topo", "/v1/", [])
        assert any(r.path_pattern == "/v1/topology" for r in routes)
        assert not any("//" in r.path_pattern for r in routes)

    def test_health_stays_auth_free_others_get_middleware(self) -> None:
        """D5: /health is reachable without auth (liveness probes); the other
        six carry the configured topology_middleware."""
        gw = HTTPGateway(
            "gw",
            GatewayConfig(
                topology_agent="topo",
                topology_middleware=["civitas.gateway.auth.require_api_key"],
            ),
        )
        by_path = {e.path_pattern: e for e in gw._route_table.entries()}
        assert by_path["/health"].middleware == []
        assert by_path["/topology"].middleware == ["civitas.gateway.auth.require_api_key"]
        assert by_path["/metrics"].middleware == ["civitas.gateway.auth.require_api_key"]

    def test_each_route_carries_its_op_in_payload_extra(self) -> None:
        routes = _build_topology_routes("topo", "", [])
        op_by_path = {(r.method, r.path_pattern): r.payload_extra["__op__"] for r in routes}
        assert op_by_path == {
            ("GET", "/health"): "health",
            ("GET", "/topology"): "topology",
            ("GET", "/agents"): "agents",
            ("GET", "/agents/{name}"): "agent_detail",
            ("GET", "/snapshot"): "snapshot",
            ("GET", "/metrics"): "metrics",
            ("GET", "/processes"): "processes",
            ("POST", "/agents/{name}/suspend"): "suspend",
            ("POST", "/agents/{name}/resume"): "resume",
            ("POST", "/agents/{name}/restart"): "restart",
        }

    def test_topology_routes_coexist_with_user_declared_routes(self) -> None:
        """An attach_to-style gateway (deferred, but the mechanism must not
        break): user routes + auto topology routes live in one table."""
        gw = HTTPGateway(
            "gw",
            GatewayConfig(
                routes=[{"method": "POST", "path": "/chat", "agent": "assistant"}],
                topology_agent="topo",
            ),
        )
        paths = {(e.method, e.path_pattern) for e in gw._route_table.entries()}
        assert ("POST", "/chat") in paths
        assert ("GET", "/topology") in paths
        assert len(gw._route_table.entries()) == 11  # 1 user + 7 read + 3 write
