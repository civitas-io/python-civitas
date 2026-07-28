"""HTTP gateway JWT bearer auth (gateway-auth.md, gateway-ws-grpc-auth.md) — v0.9.2.

Requires: pip install 'civitas[http,jwt]'

``require_jwt`` middleware verifies an ``Authorization: Bearer <token>`` header
against ``CIVITAS_JWT_*`` settings — env vars, not YAML, since they're secrets.
This demo generates its own HS256 secret and a short-lived token to sign requests
with, entirely self-contained (no external identity provider needed to try it).

IMPORTANT: ``civitas.config``'s ``Settings`` is a module-level singleton read
ONCE at import time — the ``CIVITAS_JWT_*`` env vars below MUST be set before
anything under ``civitas`` is imported, which is why they're set at the very top
of this file, before any ``from civitas import ...`` line.

Usage:
    python examples/gateway_auth.py
"""

from __future__ import annotations

import os
import secrets

# Must happen before any `civitas` import (see module docstring). Generated at
# runtime, not a fixed string literal -- a static-analysis scanner (correctly)
# flags any hardcoded JWT secret regardless of "it's just a demo" intent, and
# there is no real reason a throwaway demo secret needs to be reproducible
# across runs, or checked into git history at all.
_JWT_SECRET = secrets.token_urlsafe(32)
os.environ["CIVITAS_JWT_SECRET"] = _JWT_SECRET
os.environ["CIVITAS_JWT_AUDIENCE"] = "civitas-demo"
os.environ["CIVITAS_JWT_ISSUER"] = "civitas-examples"
os.environ["CIVITAS_JWT_ALGORITHMS"] = "HS256"

import asyncio  # noqa: E402
import json as jsonlib  # noqa: E402
import time  # noqa: E402

import jwt as pyjwt  # noqa: E402

from civitas import AgentProcess, Runtime, Supervisor  # noqa: E402
from civitas.gateway import GatewayConfig, HTTPGateway, route  # noqa: E402
from civitas.messages import Message  # noqa: E402


class EchoAgent(AgentProcess):
    @route("POST", "/v1/echo")
    async def handle(self, message: Message) -> Message | None:
        return self.reply({"echo": message.payload.get("text", "")})


async def _post(url: str, json: dict, headers: dict[str, str] | None = None) -> tuple[int, str]:
    """A tiny, dependency-free async HTTP POST -- civitas has no bundled HTTP
    client, and pulling in httpx (a test-only dependency, not part of any
    civitas[...] extra) just for one example would be a hidden requirement.
    Uses asyncio.open_connection, matching the same non-blocking pattern
    civitas/dashboard/client.py already established for the same reason."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    host, port, path = parsed.hostname or "127.0.0.1", parsed.port or 80, parsed.path or "/"
    body = jsonlib.dumps(json).encode()
    header_lines = [
        f"POST {path} HTTP/1.1",
        f"Host: {host}:{port}",
        "Content-Type: application/json",
    ]
    for key, value in (headers or {}).items():
        header_lines.append(f"{key}: {value}")
    header_lines += [f"Content-Length: {len(body)}", "Connection: close", "", ""]
    request_bytes = "\r\n".join(header_lines).encode() + body

    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(request_bytes)
        await writer.drain()
        raw = b""
        while chunk := await reader.read(65536):
            raw += chunk
    finally:
        writer.close()
    header_end = raw.find(b"\r\n\r\n")
    status_line = raw[:header_end].decode(errors="replace").splitlines()[0]
    status_code = int(status_line.split()[1])
    return status_code, raw[header_end + 4 :].decode(errors="replace")


def _make_token(*, expired: bool = False) -> str:
    now = int(time.time())
    claims = {
        "sub": "demo-user",
        "aud": "civitas-demo",
        "iss": "civitas-examples",
        "iat": now,
        # jwt_auth.py's verifier applies a 60s clock-skew leeway (_LEEWAY_SECONDS)
        # -- an expiry only slightly in the past would still verify as valid
        # (found running this exact example against a -10s expiry; fixed to
        # -120s, safely past the leeway window).
        "exp": now - 120 if expired else now + 60,
    }
    return pyjwt.encode(claims, _JWT_SECRET, algorithm="HS256")


async def main() -> None:
    config = GatewayConfig(
        host="127.0.0.1",
        port=8082,
        routes=[{"method": "POST", "path": "/v1/echo", "agent": "echo", "mode": "call"}],
        middleware=["civitas.gateway.jwt_auth.require_jwt"],
    )
    runtime = Runtime(
        supervisor=Supervisor(
            "root", children=[HTTPGateway("api", config=config), EchoAgent("echo")]
        )
    )
    await runtime.start()
    # HTTPGateway.on_start() launches uvicorn as a background task; a moment
    # for it to actually bind before the first request avoids a startup race
    # (found running this exact example -- ConnectionRefusedError, not a
    # signing/auth issue).
    await asyncio.sleep(0.3)

    print("No Authorization header:")
    status, body = await _post("http://127.0.0.1:8082/v1/echo", {"text": "hi"})
    print(f"  {status} {body}")

    print("\nExpired token:")
    status, body = await _post(
        "http://127.0.0.1:8082/v1/echo",
        {"text": "hi"},
        headers={"Authorization": f"Bearer {_make_token(expired=True)}"},
    )
    print(f"  {status} {body}")

    print("\nValid token:")
    status, body = await _post(
        "http://127.0.0.1:8082/v1/echo",
        {"text": "hi"},
        headers={"Authorization": f"Bearer {_make_token()}"},
    )
    print(f"  {status} {body}")

    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
