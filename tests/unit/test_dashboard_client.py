"""civitas/dashboard/client.py — the async HTTP client the Textual dashboard
polls with (v0.9.1, dashboard-v2 Phase E). Reuses a real TopologyServer over a
real ZMQ-free HTTP loop (matches the existing test_topology_server.py pattern),
not a mocked socket — the whole point of this client is "never a blocking call
sharing the app's event loop" (Phase D's deadlock lesson), worth proving for
real.
"""

from __future__ import annotations

import asyncio

import pytest

from civitas import Runtime, Supervisor, TopologyServer
from civitas.dashboard.client import DashboardConnectionError, fetch_json


@pytest.mark.asyncio
async def test_fetch_json_success() -> None:
    ts = TopologyServer(name="topo", port=16900)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    try:
        status, data = await fetch_json("127.0.0.1", 16900, "/health")
        assert status == 200
        assert data == {"status": "ok"}
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_fetch_json_topology_shape() -> None:
    """A real /topology round-trip, not just /health — proves JSON bodies of
    non-trivial size and shape (nested dicts/lists) parse correctly."""
    ts = TopologyServer(name="topo", port=16901)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    try:
        status, data = await fetch_json("127.0.0.1", 16901, "/topology")
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
