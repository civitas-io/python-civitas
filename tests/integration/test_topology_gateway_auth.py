"""v0.9.5 (topology-gateway-merge.md phase 5) — the actual payoff of the merge:
a topology_server node's introspection endpoints now inherit HTTPGateway's
already-audited AuthN. Verified against a REAL running Runtime (real uvicorn,
real HTTP requests), not mocks -- the whole point was that TopologyServer had
zero auth and now it doesn't.

Uses API-key auth (require_api_key) as the simplest real middleware to prove
the inheritance end-to-end; JWT/mTLS ride the exact same route-middleware
mechanism (already covered by the gateway's own auth test suites).
"""

from __future__ import annotations

import asyncio
import json
import socket
import textwrap
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("uvicorn")  # civitas[http]

from civitas import Runtime, Supervisor  # noqa: E402
from civitas.audit.types import AuditEvent  # noqa: E402
from civitas.config import SecretStr, settings  # noqa: E402
from civitas.gateway import GatewayConfig, HTTPGateway  # noqa: E402
from civitas.process import ProcessStatus  # noqa: E402
from civitas.topology_server import TopologyAgent  # noqa: E402
from tests.conftest import EchoAgent, wait_for  # noqa: E402


async def _alice_mw(request, call_next):  # type: ignore[no-untyped-def]
    """A stand-in for a customer's 'bring your own auth' middleware (the D1 seam):
    it authenticates (trivially, as everyone = alice) and stamps the principal.
    A real one would call SCIM/IdP/OPA; civitas only ever reads principal['id'].
    Module-level so load_middleware can import it by dotted path.
    """
    request.auth = {"principal": {"id": "alice", "method": "test"}}
    return await call_next(request)


class _CaptureAudit:
    """Minimal AuditSink that records every emitted event in memory."""

    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    async def emit(self, event: AuditEvent) -> None:
        self.events.append(event)

    async def flush(self) -> None: ...

    async def close(self) -> None: ...


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


async def _http_get(port: int, path: str, headers: dict[str, str] | None = None) -> int:
    """Return the HTTP status code for GET path with optional headers."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        lines = [f"GET {path} HTTP/1.1", f"Host: 127.0.0.1:{port}", "Connection: close"]
        for k, v in (headers or {}).items():
            lines.append(f"{k}: {v}")
        writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
        await writer.drain()
        raw = await reader.read(65536)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    status_line = raw.split(b"\r\n", 1)[0].decode(errors="replace")
    return int(status_line.split()[1]) if len(status_line.split()) >= 2 else 500


async def _get_with_body(port: int, path: str, headers: dict[str, str] | None = None) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        lines = [f"GET {path} HTTP/1.1", f"Host: 127.0.0.1:{port}", "Connection: close"]
        for k, v in (headers or {}).items():
            lines.append(f"{k}: {v}")
        writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
        await writer.drain()
        # Read to EOF (Connection: close) -- uvicorn flushes headers and body in
        # separate writes, so a single read() can return just the headers.
        chunks: list[bytes] = []
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break
            chunks.append(chunk)
        raw = b"".join(chunks)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    end = raw.find(b"\r\n\r\n")
    return raw[end + 4 :] if end != -1 else b""


async def _wait_listening(port: int, timeout: float = 8.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            if await _http_get(port, "/health") == 200:
                return
        except Exception:
            pass
        await asyncio.sleep(0.1)
    raise TimeoutError(f"gateway on port {port} never became ready")


def _topology_yaml(tmp_path: Path, port: int, *, with_auth: bool) -> Path:
    # NB indentation: auth: must sit UNDER config: (16 spaces here, same as
    # host:/port:), so that after textwrap.dedent strips the common 8-space
    # lead it lands at 8 spaces -- a child of config:, not a sibling. Getting
    # this wrong (14 spaces) makes auth: a sibling of config: and the whole
    # auth block silently vanishes -- caught by a real 200-not-401 test
    # failure, not by review.
    auth_block = (
        "\n                auth:"
        "\n                  middleware: [civitas.gateway.auth.require_api_key]"
        if with_auth
        else ""
    )
    yaml_file = tmp_path / "t.yaml"
    yaml_file.write_text(
        textwrap.dedent(f"""\
        supervision:
          name: root
          children:
            - type: topology_server
              name: topo
              config:
                host: 127.0.0.1
                port: {port}{auth_block}
        """)
    )
    return yaml_file


@pytest.mark.asyncio
async def test_api_key_auth_gates_introspection_but_not_health(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The core proof: with require_api_key configured, /topology needs the key
    (401 without, 200 with), but /health stays reachable WITHOUT it (D5 --
    liveness probes must not be gated)."""
    monkeypatch.setattr(settings, "gateway_api_key", SecretStr("secret-123"))
    port = _free_port()
    rt = Runtime.from_config(_topology_yaml(tmp_path, port, with_auth=True))
    await rt.start()
    try:
        await _wait_listening(port)

        # /health: auth-free even with auth configured (D5)
        assert await _http_get(port, "/health") == 200

        # /topology: denied without the key
        assert await _http_get(port, "/topology") == 401
        # denied with the WRONG key
        assert await _http_get(port, "/topology", {"X-API-Key": "wrong"}) == 401
        # allowed with the correct key
        assert await _http_get(port, "/topology", {"X-API-Key": "secret-123"}) == 200

        # /metrics is gated the same way, and still returns real Prometheus text
        assert await _http_get(port, "/metrics") == 401
        body = await _get_with_body(port, "/metrics", {"X-API-Key": "secret-123"})
        assert b"civitas_messages_handled_total" in body
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_no_auth_block_is_wide_open_exactly_as_before(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A topology_server with no auth: block behaves byte-for-byte as the old
    zero-auth TopologyServer did -- every endpoint reachable with no
    credentials. Backward compatibility for the common case."""
    monkeypatch.setattr(settings, "gateway_api_key", SecretStr("secret-123"))
    port = _free_port()
    rt = Runtime.from_config(_topology_yaml(tmp_path, port, with_auth=False))
    await rt.start()
    try:
        await _wait_listening(port)
        assert await _http_get(port, "/health") == 200
        assert await _http_get(port, "/topology") == 200  # no key needed
        assert await _http_get(port, "/metrics") == 200
        assert await _http_get(port, "/agents") == 200
    finally:
        await rt.stop()


async def _http_post(port: int, path: str, body: dict[str, Any] | None = None) -> tuple[int, bytes]:
    """POST with a JSON body; return (status, body-bytes) reading to EOF."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        payload = json.dumps(body or {}).encode()
        req = (
            f"POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(payload)}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode() + payload
        writer.write(req)
        await writer.drain()
        chunks: list[bytes] = []
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break
            chunks.append(chunk)
        raw = b"".join(chunks)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    status_line = raw.split(b"\r\n", 1)[0].decode(errors="replace")
    code = int(status_line.split()[1]) if len(status_line.split()) >= 2 else 500
    end = raw.find(b"\r\n\r\n")
    return code, (raw[end + 4 :] if end != -1 else b"")


@pytest.mark.asyncio
async def test_suspend_write_action_records_authenticated_principal() -> None:
    """The v0.9.6 payoff, end-to-end against a REAL running Runtime: a POST to
    the control-plane suspend route (behind a custom 'bring your own auth'
    middleware) actually suspends the agent AND records the AUTHENTICATED
    principal as initiated_by in the AuditEvent -- not a client-supplied value.
    This is the honest-audit binding (D2) proven for real.
    """
    port = _free_port()
    audit = _CaptureAudit()
    topo = TopologyAgent("topo")
    gw = HTTPGateway(
        "topo_gateway",
        GatewayConfig(
            host="127.0.0.1",
            port=port,
            topology_agent="topo",
            topology_middleware=["tests.integration.test_topology_gateway_auth._alice_mw"],
        ),
    )
    worker = EchoAgent("worker")
    rt = Runtime(supervisor=Supervisor("root", children=[topo, gw, worker]))
    rt._audit_sink = audit  # wired into every agent via ComponentSet at start()
    await rt.start()
    try:
        await _wait_listening(port)

        # The client even TRIES to smuggle a fake actor in the body; it must lose.
        code, body = await _http_post(
            port,
            "/agents/worker/suspend",
            {"reason": "spend check", "__principal__": {"id": "attacker"}},
        )
        assert code == 200
        assert json.loads(body)["initiated_by"] == "alice"  # authenticated, not "attacker"

        # The agent actually transitions to SUSPENDED.
        await wait_for(lambda: worker.status == ProcessStatus.SUSPENDED, msg="worker suspended")

        # The AuditEvent records the AUTHENTICATED principal, not the body value.
        suspend_events = [e for e in audit.events if e["event"] == "agent.suspend"]
        assert len(suspend_events) >= 1
        assert suspend_events[-1]["details"]["initiated_by"] == "alice"
        assert suspend_events[-1]["details"]["reason"] == "spend check"
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_resume_write_action_records_principal_as_approver() -> None:
    """resume over the control plane records the authenticated principal as the
    audited approver (S6 requires a non-empty approver -- the principal is it)."""
    port = _free_port()
    audit = _CaptureAudit()
    topo = TopologyAgent("topo")
    gw = HTTPGateway(
        "topo_gateway",
        GatewayConfig(
            host="127.0.0.1",
            port=port,
            topology_agent="topo",
            topology_middleware=["tests.integration.test_topology_gateway_auth._alice_mw"],
        ),
    )
    worker = EchoAgent("worker")
    rt = Runtime(supervisor=Supervisor("root", children=[topo, gw, worker]))
    rt._audit_sink = audit
    await rt.start()
    try:
        await _wait_listening(port)
        await _http_post(port, "/agents/worker/suspend", {})
        await wait_for(lambda: worker.status == ProcessStatus.SUSPENDED, msg="worker suspended")

        code, body = await _http_post(port, "/agents/worker/resume", {})
        assert code == 200
        assert json.loads(body)["approver"] == "alice"
        await wait_for(lambda: worker.status == ProcessStatus.RUNNING, msg="worker resumed")

        resume_events = [e for e in audit.events if e["event"] == "agent.resume"]
        assert resume_events[-1]["details"]["approver"] == "alice"
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_dashboard_client_authenticates_with_headers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """v0.9.6 (D7): the dashboard's own polling client (fetch_json) can attach
    to an auth-protected endpoint by sending headers -- without them it gets
    401, with the right one it gets 200. This is what lets civitas dashboard/top
    drive the endpoints the control-plane auth seam now protects.
    """
    from civitas.dashboard.client import fetch_json

    monkeypatch.setattr(settings, "gateway_api_key", SecretStr("secret-123"))
    port = _free_port()
    rt = Runtime.from_config(_topology_yaml(tmp_path, port, with_auth=True))
    await rt.start()
    try:
        await _wait_listening(port)
        # No header -> 401 (fetch_json returns the status, doesn't raise on 401).
        status, _ = await fetch_json("127.0.0.1", port, "/topology")
        assert status == 401
        # Correct header -> 200.
        status, data = await fetch_json(
            "127.0.0.1", port, "/topology", headers={"X-API-Key": "secret-123"}
        )
        assert status == 200
        assert data["name"] == "root"
    finally:
        await rt.stop()
