"""civitas/dashboard/client.py — the async HTTP client the Textual dashboard
polls with (v0.9.1, dashboard-v2 Phase E). Reuses a real introspection endpoint
over a real ZMQ-free HTTP loop, not a mocked socket — the whole point of this
client is "never a blocking call sharing the app's event loop" (Phase D's
deadlock lesson), worth proving for real.

v0.9.5 (topology-gateway-merge.md phase 6): the endpoint is now a real
HTTPGateway + TopologyAgent pair (TopologyServer was removed), constructed
directly here as siblings under one Supervisor — Runtime wires the TopologyAgent
via the same _TopologyIntrospection injection and starts the gateway's uvicorn.
"""

from __future__ import annotations

import asyncio
import socket

import pytest

from civitas import Runtime, Supervisor
from civitas.dashboard.client import DashboardConnectionError, fetch_json
from civitas.gateway import GatewayConfig, HTTPGateway
from civitas.topology_server import TopologyAgent


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


def _topology_runtime(port: int) -> Runtime:
    topo = TopologyAgent("topo")
    gw = HTTPGateway(
        "topo_gateway", GatewayConfig(host="127.0.0.1", port=port, topology_agent="topo")
    )
    return Runtime(supervisor=Supervisor("root", children=[topo, gw]))


async def _wait_ready(port: int, timeout: float = 8.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            status, _ = await fetch_json("127.0.0.1", port, "/health")
            if status == 200:
                return
        except DashboardConnectionError:
            pass
        await asyncio.sleep(0.1)
    raise TimeoutError(f"gateway on port {port} never became ready")


@pytest.mark.asyncio
async def test_fetch_json_success() -> None:
    port = _free_port()
    runtime = _topology_runtime(port)
    await runtime.start()
    try:
        await _wait_ready(port)
        status, data = await fetch_json("127.0.0.1", port, "/health")
        assert status == 200
        assert data == {"status": "ok"}
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_fetch_json_topology_shape() -> None:
    """A real /topology round-trip, not just /health — proves JSON bodies of
    non-trivial size and shape (nested dicts/lists) parse correctly."""
    port = _free_port()
    runtime = _topology_runtime(port)
    await runtime.start()
    try:
        await _wait_ready(port)
        status, data = await fetch_json("127.0.0.1", port, "/topology")
        assert status == 200
        assert data["name"] == "root"
        assert data["type"] == "supervisor"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_fetch_json_connection_refused_raises_dashboard_error() -> None:
    """Nothing is listening on this port \u2014 fetch_json must raise the ONE
    exception type the app's poll loop catches, never a raw OSError."""
    with pytest.raises(DashboardConnectionError):
        await fetch_json("127.0.0.1", 1, "/health", timeout=0.5)


@pytest.mark.asyncio
async def test_fetch_json_timeout_raises_dashboard_error() -> None:
    """A server that accepts the connection but never writes a response
    (simulated via a bare asyncio server that just holds the connection open)
    must time out into DashboardConnectionError, not hang forever \u2014 this is
    exactly the failure mode a stalled/overloaded TopologyServer could exhibit."""

    async def _hang(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Long enough to outlast fetch_json's own 0.2s timeout below (proving
        # OUR timeout fires, not the handler finishing first), short enough
        # that this test's own server.wait_closed() cleanup doesn't take
        # forever — asyncio.Server (3.12+) waits for in-flight handlers too.
        await asyncio.sleep(0.6)
        writer.close()

    server = await asyncio.start_server(_hang, "127.0.0.1", 16902)
    try:
        with pytest.raises(DashboardConnectionError):
            await fetch_json("127.0.0.1", 16902, "/health", timeout=0.2)
    finally:
        server.close()
        await server.wait_closed()
