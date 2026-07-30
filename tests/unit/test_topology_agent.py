"""Tests for TopologyAgent (v0.9.5, docs/design/topology-gateway-merge.md phase 2).

Byte-for-byte parity is the actual goal of this phase, not just "does it
respond" — every op is exercised as a REAL side-by-side comparison against
TopologyServer._route_http()'s existing behavior over the exact same injected
state, not against a hand-written expected dict that could silently drift
from what TopologyServer actually produces.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from civitas.topology_server import TopologyAgent, TopologyServer
from tests.unit.test_topology_server import (
    _make_mock_agent,
    _make_mock_dyn,
    _make_mock_supervisor,
)


def _wire_same_state(server: TopologyServer, agent: TopologyAgent) -> None:
    """Inject byte-for-byte identical state into both, mirroring how Runtime
    wires a real TopologyServer today (runtime.py's isinstance(agent,
    TopologyServer) loop) — the same shape TopologyAgent's __init__ sets."""
    root = _make_mock_supervisor(
        "root",
        children=[
            _make_mock_agent("worker-a"),
            _make_mock_dyn("workers", dynamic_children={"job-1": _make_mock_agent("job-1")}),
        ],
    )
    static_agent = _make_mock_agent("static-1")
    for target in (server, agent):
        target._root_supervisor = root
        target._agents = {"static-1": static_agent}
        target._metrics_collector = None


class TestByteForByteParity:
    def setup_method(self) -> None:
        # Same name for both: _process_id_for() falls back to self.name when a
        # target has no registry entry, so a real parity check needs them to
        # match -- this is a test-fixture confound, not something Runtime would
        # ever actually do (a real TopologyServer/TopologyAgent instance is
        # never named identically to its own comparison twin).
        self.server = TopologyServer(name="shared", port=0)
        self.agent = TopologyAgent(name="shared")
        _wire_same_state(self.server, self.agent)

    @pytest.mark.asyncio
    async def test_health_parity(self) -> None:
        server_body, server_status, server_ct = await self.server._route_http("/health")
        reply = await self.agent.handle_call({"__op__": "health"}, "tester")
        assert reply["__raw_body__"] == server_body
        assert reply["__status__"] == server_status
        assert reply["__content_type__"] == server_ct

    @pytest.mark.asyncio
    async def test_topology_parity(self) -> None:
        server_body, server_status, _ = await self.server._route_http("/topology")
        reply = await self.agent.handle_call({"__op__": "topology"}, "tester")
        assert reply["__raw_body__"] == server_body
        assert reply["__status__"] == server_status
        # Round-trips to the exact same structure, not just the same string,
        # so this survives incidental dict key-ordering differences too.
        assert json.loads(reply["__raw_body__"]) == json.loads(server_body)

    @pytest.mark.asyncio
    async def test_agents_parity_including_bare_array_shape(self) -> None:
        """The one genuinely tricky case: /agents' wire body is a bare JSON
        ARRAY, not an object — GenServer.handle_call() can't return a list
        directly (must be a dict), so this proves the raw_body sentinel
        actually preserves that shape rather than silently wrapping it."""
        server_body, server_status, _ = await self.server._route_http("/agents")
        reply = await self.agent.handle_call({"__op__": "agents"}, "tester")
        assert reply["__raw_body__"] == server_body
        assert reply["__status__"] == server_status
        parsed = json.loads(reply["__raw_body__"])
        assert isinstance(parsed, list)  # bare array, not {"agents": [...]}

    @pytest.mark.asyncio
    async def test_agent_detail_found_parity(self) -> None:
        server_body, server_status, _ = await self.server._route_http("/agents/static-1")
        reply = await self.agent.handle_call(
            {"__op__": "agent_detail", "name": "static-1"}, "tester"
        )
        assert reply["__raw_body__"] == server_body
        assert reply["__status__"] == server_status == 200

    @pytest.mark.asyncio
    async def test_agent_detail_not_found_parity_including_404(self) -> None:
        """The other genuinely tricky case: a real, varying (non-200) status
        code flowing through the raw_body sentinel's __status__ field."""
        server_body, server_status, _ = await self.server._route_http("/agents/ghost")
        reply = await self.agent.handle_call({"__op__": "agent_detail", "name": "ghost"}, "tester")
        assert reply["__raw_body__"] == server_body
        assert reply["__status__"] == server_status == 404

    @pytest.mark.asyncio
    async def test_snapshot_parity_no_metrics_collector_404(self) -> None:
        server_body, server_status, _ = await self.server._route_http("/snapshot")
        reply = await self.agent.handle_call({"__op__": "snapshot"}, "tester")
        assert reply["__raw_body__"] == server_body
        assert reply["__status__"] == server_status == 404

    @pytest.mark.asyncio
    async def test_snapshot_parity_with_metrics_collector(self) -> None:
        collector = MagicMock()
        snapshot = MagicMock()
        snapshot.agents = {}
        snapshot.total_messages = 5
        snapshot.total_cost_usd = 0.02
        snapshot.uptime_seconds = 12.0
        snapshot.restart_history = []
        collector.snapshot = snapshot
        self.server._metrics_collector = collector
        self.agent._metrics_collector = collector

        server_body, server_status, _ = await self.server._route_http("/snapshot")
        reply = await self.agent.handle_call({"__op__": "snapshot"}, "tester")
        assert reply["__raw_body__"] == server_body
        assert reply["__status__"] == server_status == 200

    @pytest.mark.asyncio
    async def test_metrics_parity_prometheus_text_not_json(self) -> None:
        server_body, server_status, server_ct = await self.server._route_http("/metrics")
        reply = await self.agent.handle_call({"__op__": "metrics"}, "tester")
        assert reply["__raw_body__"] == server_body
        assert reply["__status__"] == server_status == 200
        assert reply["__content_type__"] == server_ct
        assert "application/json" not in reply["__content_type__"]
        # Genuinely not JSON -- proves this isn't accidentally routed through
        # the same JSON-wrapping path as every other op.
        with pytest.raises(json.JSONDecodeError):
            json.loads(reply["__raw_body__"])

    @pytest.mark.asyncio
    async def test_processes_parity(self) -> None:
        # Both share the same (real, module-level) process sampler and no bus,
        # so neither ever finds a Worker to probe — deterministic parity.
        server_body, server_status, _ = await self.server._route_http("/processes")
        reply = await self.agent.handle_call({"__op__": "processes"}, "tester")
        assert reply["__status__"] == server_status == 200
        assert json.loads(reply["__raw_body__"]).keys() == json.loads(server_body).keys()

    @pytest.mark.asyncio
    async def test_unknown_op_parity_with_unknown_path(self) -> None:
        server_body, server_status, _ = await self.server._route_http("/nope")
        reply = await self.agent.handle_call({"__op__": "nope"}, "tester")
        assert reply["__raw_body__"] == server_body
        assert reply["__status__"] == server_status == 404


class TestTopologyAgentIsolated:
    """A few TopologyAgent-only checks not covered by the parity suite above."""

    def setup_method(self) -> None:
        self.agent = TopologyAgent(name="ta")

    @pytest.mark.asyncio
    async def test_no_root_supervisor_is_error_not_crash(self) -> None:
        reply = await self.agent.handle_call({"__op__": "topology"}, "tester")
        body = json.loads(reply["__raw_body__"])
        assert body == {"error": "runtime not available"}
        assert reply["__status__"] == 200  # matches TopologyServer's own /topology shape

    @pytest.mark.asyncio
    async def test_processes_probes_bus_for_remote_workers(self) -> None:
        """v0.9.1 D-DASH-3's real-bus-round-trip path still works through
        TopologyAgent -- GenServer already provides self._bus, no new wiring
        needed for this one op that isn't a pure same-process read."""
        self.agent._registry = MagicMock()
        self.agent._registry.all_names.return_value = ["remote"]
        entry = MagicMock()
        entry.health_channel = "chan-1"
        self.agent._registry.lookup.return_value = entry
        self.agent._bus = MagicMock()
        ack = MagicMock()
        ack.payload = {"process": {"pid": 123, "cpu_percent": 1.0}}
        self.agent._bus.request = AsyncMock(return_value=ack)

        reply = await self.agent.handle_call({"__op__": "processes"}, "tester")
        body = json.loads(reply["__raw_body__"])
        worker_rows = [p for p in body["processes"] if p["kind"] == "worker"]
        assert worker_rows == [{"kind": "worker", "id": "chan-1", "pid": 123, "cpu_percent": 1.0}]

    @pytest.mark.asyncio
    async def test_handle_call_always_returns_a_dict(self) -> None:
        """GenServer's own contract (_do_call raises TypeError otherwise) --
        every op, including the /agents bare-array case, must still return a
        dict at the handle_call() level (the array lives inside __raw_body__
        as a STRING, not as the top-level return value)."""
        for op in ("health", "topology", "agents", "snapshot", "metrics", "processes"):
            reply = await self.agent.handle_call({"__op__": op}, "tester")
            assert isinstance(reply, dict)
