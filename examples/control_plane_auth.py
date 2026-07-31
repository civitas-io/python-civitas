"""Control-plane write actions + the "bring your own auth" seam — v0.9.6.

Requires: pip install 'civitas[http]'

The control plane (suspend/resume an agent over HTTP) is exposed by a
``topology_server`` node, which since v0.9.5 is served by ``HTTPGateway`` and
inherits its middleware chain. civitas does NOT ship AuthZ (roles, scopes, SCIM,
IdP integration) — it ships the *seam*: you plug in a middleware that
authenticates against your own system (SCIM / IdP / OPA / anything), and civitas
records whoever you authenticated as the honest actor in the audit trail.

The entire contract civitas asks of your middleware (control-plane-writes.md D1):
set ``request.auth["principal"] = {"id": "<who this is>"}``. civitas reads only
``id`` (recorded as the audited ``initiated_by``/``approver``); you may add any
sibling keys you like. If you deny the request, return a 403 yourself — that is
YOUR AuthZ; civitas never sees a role or scope.

A single dev who configures no middleware gets suspend/resume with zero
ceremony on localhost; the audit log honestly reads ``initiated_by:
"unauthenticated"``.

This file shows the seam with a deliberately trivial stand-in middleware (it
"authenticates" everyone as ``alice``). Swap the body of ``require_my_auth`` for
a real call to your identity system.

Usage:
    python examples/control_plane_auth.py
"""

from __future__ import annotations

import asyncio

from civitas import Runtime, Supervisor
from civitas.gateway import GatewayConfig, HTTPGateway
from civitas.gateway.types import GatewayRequest, GatewayResponse, NextMiddleware
from civitas.process import AgentProcess, ProcessStatus
from civitas.topology_server import TopologyAgent


# --- The seam: your middleware. Module-level so it's importable by dotted path
#     (topology YAML references middleware by "module.function"). -------------
async def require_my_auth(request: GatewayRequest, call_next: NextMiddleware) -> GatewayResponse:
    """Authenticate against YOUR system, then stamp the principal (D1).

    Replace the body with a real check: verify a bearer token against your IdP,
    call SCIM to resolve the user, ask OPA for an allow/deny, etc. On deny,
    return ``GatewayResponse(status=403, ...)`` — that is your AuthZ. On allow,
    set ``request.auth["principal"]`` and continue.
    """
    # ---- your real auth goes here; this stand-in allows everyone as "alice" ----
    verified_user = "alice"
    request.auth = {**(request.auth or {}), "principal": {"id": verified_user, "method": "demo"}}
    return await call_next(request)


class Worker(AgentProcess):
    async def handle(self, message):  # type: ignore[no-untyped-def]
        return None


async def main() -> None:
    port = 8770
    topo = TopologyAgent("topo")
    gateway = HTTPGateway(
        "topo_gateway",
        GatewayConfig(
            host="127.0.0.1",
            port=port,
            topology_agent="topo",
            # Your middleware gates every introspection + write route except
            # /health. Omit this list entirely and a single dev gets an open
            # control plane on localhost (principal defaults to "unauthenticated").
            topology_middleware=["examples.control_plane_auth.require_my_auth"],
        ),
    )
    worker = Worker("worker")
    runtime = Runtime(supervisor=Supervisor("root", children=[topo, gateway, worker]))
    await runtime.start()
    print(f"control plane on http://127.0.0.1:{port}  (Ctrl+C to stop)")
    print(f"  suspend:  curl -XPOST http://127.0.0.1:{port}/agents/worker/suspend")
    print(f"  resume:   curl -XPOST http://127.0.0.1:{port}/agents/worker/resume")

    # Drive it in-process so the example is self-contained: suspend the worker
    # over the real HTTP control plane, then show it actually transitioned and
    # who the audit will attribute it to.
    await asyncio.sleep(0.5)  # let uvicorn bind
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    payload = b'{"reason": "demo run"}'
    writer.write(
        b"POST /agents/worker/suspend HTTP/1.1\r\nHost: x\r\n"
        b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(payload)}\r\n".encode()
        + b"Connection: close\r\n\r\n"
        + payload
    )
    await writer.drain()
    body = (await reader.read()).split(b"\r\n\r\n", 1)[-1]
    writer.close()
    print("suspend response:", body.decode())

    for _ in range(50):
        if worker.status == ProcessStatus.SUSPENDED:
            break
        await asyncio.sleep(0.05)
    print("worker status is now:", worker.status.value)
    print("(the audit event records initiated_by='alice' — the authenticated principal,")
    print(" never a client-supplied value)")

    await runtime.stop()


if __name__ == "__main__":
    asyncio.run(main())
