from __future__ import annotations

import base64
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from civitas.config import Settings
from civitas.gateway.asgi import GatewayASGI, _parse_multipart
from civitas.gateway.auth import require_api_key
from civitas.gateway.core import GatewayConfig, HTTPGateway
from civitas.gateway.ratelimit import RateLimiter, rate_limit
from civitas.gateway.router import RouteTable
from civitas.gateway.types import GatewayRequest, GatewayResponse
from civitas.messages import Message


async def _next_ok(request: GatewayRequest) -> GatewayResponse:
    return GatewayResponse(200, {"ok": True})


# ---------------------------------------------------------------------------
# G4 — rate limiting
# ---------------------------------------------------------------------------


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_allows_up_to_limit_then_blocks(self) -> None:
        rl = RateLimiter("rl", max_requests=2, window_seconds=60.0)
        assert (await rl.handle_call({"client_id": "a"}, ""))["allowed"] is True
        assert (await rl.handle_call({"client_id": "a"}, ""))["allowed"] is True
        blocked = await rl.handle_call({"client_id": "a"}, "")
        assert blocked["allowed"] is False
        assert blocked["remaining"] == 0

    @pytest.mark.asyncio
    async def test_per_client_isolation(self) -> None:
        rl = RateLimiter("rl", max_requests=1, window_seconds=60.0)
        await rl.handle_call({"client_id": "a"}, "")
        assert (await rl.handle_call({"client_id": "b"}, ""))["allowed"] is True

    @pytest.mark.asyncio
    async def test_middleware_429_when_blocked(self) -> None:
        gw = MagicMock()
        gw.call = AsyncMock(return_value={"allowed": False, "retry_after": 30})
        req = GatewayRequest(method="POST", path="/x", client_ip="1.2.3.4", gateway=gw)
        resp = await rate_limit(req, _next_ok)
        assert resp.status == 429
        assert resp.headers["Retry-After"] == "30"

    @pytest.mark.asyncio
    async def test_middleware_passes_when_allowed(self) -> None:
        gw = MagicMock()
        gw.call = AsyncMock(return_value={"allowed": True})
        req = GatewayRequest(method="POST", path="/x", client_ip="1.2.3.4", gateway=gw)
        resp = await rate_limit(req, _next_ok)
        assert resp.status == 200


# ---------------------------------------------------------------------------
# G5 — API-key auth
# ---------------------------------------------------------------------------


class TestRequireApiKey:
    @pytest.mark.asyncio
    async def test_valid_key_passes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "civitas.gateway.auth.settings", Settings(env={"CIVITAS_GATEWAY_API_KEY": "s3cret"})
        )
        req = GatewayRequest(method="GET", path="/x", headers={"x-api-key": "s3cret"})
        resp = await require_api_key(req, _next_ok)
        assert resp.status == 200

    @pytest.mark.asyncio
    async def test_wrong_key_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "civitas.gateway.auth.settings", Settings(env={"CIVITAS_GATEWAY_API_KEY": "s3cret"})
        )
        req = GatewayRequest(method="GET", path="/x", headers={"x-api-key": "nope"})
        resp = await require_api_key(req, _next_ok)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_missing_key_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "civitas.gateway.auth.settings", Settings(env={"CIVITAS_GATEWAY_API_KEY": "s3cret"})
        )
        req = GatewayRequest(method="GET", path="/x", headers={})
        resp = await require_api_key(req, _next_ok)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_unconfigured_fails_closed_500(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("civitas.gateway.auth.settings", Settings(env={}))
        req = GatewayRequest(method="GET", path="/x", headers={"x-api-key": "anything"})
        resp = await require_api_key(req, _next_ok)
        assert resp.status == 500


# ---------------------------------------------------------------------------
# G6 — multipart/form-data uploads
# ---------------------------------------------------------------------------


def _multipart(boundary: str) -> bytes:
    return (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="title"\r\n\r\n'
        "Hello\r\n"
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="doc"; filename="a.txt"\r\n'
        "Content-Type: text/plain\r\n\r\n"
        "file-content\r\n"
        f"--{boundary}--\r\n"
    ).encode()


class TestMultipart:
    def test_parse_fields_and_file(self) -> None:
        result = _parse_multipart(_multipart("BOUND"), "multipart/form-data; boundary=BOUND")
        assert result["title"] == "Hello"
        doc = result["__files__"]["doc"]
        assert doc["filename"] == "a.txt"
        assert doc["content_type"] == "text/plain"
        assert doc["size"] == len(b"file-content")
        assert base64.b64decode(doc["content_base64"]) == b"file-content"

    @pytest.mark.asyncio
    async def test_multipart_reaches_agent(self) -> None:
        gateway = MagicMock(spec=HTTPGateway)
        gateway.name = "api"
        asgi = GatewayASGI(
            gateway=gateway, route_table=RouteTable.from_config([]), config=GatewayConfig()
        )
        reply = MagicMock(spec=Message)
        reply.payload = {"stored": True}
        gateway.ask = AsyncMock(return_value=reply)

        boundary = "BOUND"
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/agents/uploader",
            "headers": [(b"content-type", f"multipart/form-data; boundary={boundary}".encode())],
        }
        events = [{"body": _multipart(boundary), "more_body": False}]
        idx = 0

        async def receive() -> dict[str, Any]:
            nonlocal idx
            event = events[idx]
            idx += 1
            return event

        sent: list[dict[str, Any]] = []

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await asgi(scope, receive, send)

        status = next(e for e in sent if e["type"] == "http.response.start")["status"]
        assert status == 200
        payload = gateway.ask.call_args.args[1]
        assert payload["title"] == "Hello"
        assert base64.b64decode(payload["__files__"]["doc"]["content_base64"]) == b"file-content"
