"""Tests for TopologyAgent (v0.9.5, docs/design/topology-gateway-merge.md).

Originally (phase 2) these compared TopologyAgent.handle_call() against the
old TopologyServer._route_http() for byte-for-byte parity. TopologyServer was
removed in phase 6, so they now assert the equivalent property directly:
handle_call() dispatches to the correct _build_* method and wraps its output
in the raw-body sentinel verbatim (JSON for every op except Prometheus
/metrics). Same guarantee -- handle_call adds no drift over the builders --
without a reference class that no longer exists.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from civitas.topology_server import TopologyAgent
from tests.unit.test_topology_server import (
    _make_mock_agent,
    _make_mock_dyn,
    _make_mock_supervisor,
)


def _wire_state(agent: TopologyAgent) -> None:
    """Inject the same privileged state Runtime wires into a real TopologyAgent
    (runtime.py's isinstance(agent, _TopologyIntrospection) loop)."""
    root = _make_mock_supervisor(
        "root",
        children=[
            _make_mock_agent("worker-a"),
            _make_mock_dyn("workers", dynamic_children={"job-1": _make_mock_agent("job-1")}),
        ],
    )
    agent._root_supervisor = root
    agent._agents = {"static-1": _make_mock_agent("static-1")}
    agent._metrics_collector = None


class TestHandleCallWrapsBuilders:
    """handle_call() must dispatch to the right _build_* method and wrap its
    result in the sentinel verbatim -- no drift added on top of the builders."""

    def setup_method(self) -> None:
        self.agent = TopologyAgent(name="shared")
        _wire_state(self.agent)

    @pytest.mark.asyncio
    async def test_health(self) -> None:
        reply = await self.agent.handle_call({"__op__": "health"}, "tester")
        assert reply["__raw_body__"] == json.dumps({"status": "ok"})
        assert reply["__status__"] == 200
        assert reply["__content_type__"] == "application/json"

    @pytest.mark.asyncio
    async def test_topology(self) -> None:
        reply = await self.agent.handle_call({"__op__": "topology"}, "tester")
        assert reply["__raw_body__"] == json.dumps(self.agent._build_topology())
        assert reply["__status__"] == 200

    @pytest.mark.asyncio
    async def test_agents_is_a_bare_array(self) -> None:
        """The tricky case: /agents' wire body is a bare JSON ARRAY, not an
        object -- GenServer.handle_call() cannot return a list directly (must
        be a dict), so this proves the raw_body sentinel preserves that shape
        rather than wrapping it in an object."""
        reply = await self.agent.handle_call({"__op__": "agents"}, "tester")
        assert reply["__raw_body__"] == json.dumps(self.agent._build_agents_list())
        parsed = json.loads(reply["__raw_body__"])
        assert isinstance(parsed, list)

    @pytest.mark.asyncio
    async def test_agent_detail_found(self) -> None:
        reply = await self.agent.handle_call(
            {"__op__": "agent_detail", "name": "static-1"}, "tester"
        )
        assert reply["__raw_body__"] == json.dumps(self.agent._build_agent_detail("static-1"))
        assert reply["__status__"] == 200

    @pytest.mark.asyncio
    async def test_agent_detail_not_found_is_404(self) -> None:
        """A real, varying (non-200) status code flowing through the sentinel's
        __status__ field."""
        reply = await self.agent.handle_call({"__op__": "agent_detail", "name": "ghost"}, "tester")
        assert reply["__status__"] == 404
        assert json.loads(reply["__raw_body__"]) == {"error": "agent 'ghost' not found"}

    @pytest.mark.asyncio
    async def test_snapshot_no_collector_is_404(self) -> None:
        data, code = self.agent._build_metrics()
        reply = await self.agent.handle_call({"__op__": "snapshot"}, "tester")
        assert reply["__raw_body__"] == json.dumps(data)
        assert reply["__status__"] == code == 404

    @pytest.mark.asyncio
    async def test_snapshot_with_collector_is_200(self) -> None:
        collector = MagicMock()
        snapshot = MagicMock()
        snapshot.agents = {}
        snapshot.total_messages = 5
        snapshot.total_cost_usd = 0.02
        snapshot.uptime_seconds = 12.0
        snapshot.restart_history = []
        collector.snapshot = snapshot
        self.agent._metrics_collector = collector

        data, code = self.agent._build_metrics()
        reply = await self.agent.handle_call({"__op__": "snapshot"}, "tester")
        assert reply["__raw_body__"] == json.dumps(data)
        assert reply["__status__"] == code == 200

    @pytest.mark.asyncio
    async def test_metrics_is_prometheus_text_not_json(self) -> None:
        body, status, content_type = self.agent._build_prometheus_metrics()
        reply = await self.agent.handle_call({"__op__": "metrics"}, "tester")
        assert reply["__raw_body__"] == body
        assert reply["__status__"] == status == 200
        assert reply["__content_type__"] == content_type
        assert "application/json" not in reply["__content_type__"]
        # Genuinely not JSON -- proves metrics isn't accidentally routed through
        # the JSON-wrapping path every other op uses.
        with pytest.raises(json.JSONDecodeError):
            json.loads(reply["__raw_body__"])

    @pytest.mark.asyncio
    async def test_processes(self) -> None:
        # _build_processes() samples live CPU%/uptime, so two calls legitimately
        # differ -- assert the wrapping shape/keys, not exact live values.
        reply = await self.agent.handle_call({"__op__": "processes"}, "tester")
        assert reply["__status__"] == 200
        assert reply["__content_type__"] == "application/json"
        body = json.loads(reply["__raw_body__"])
        assert "processes" in body
        assert isinstance(body["processes"], list)

    @pytest.mark.asyncio
    async def test_unknown_op_is_404(self) -> None:
        reply = await self.agent.handle_call({"__op__": "nope"}, "tester")
        assert reply["__status__"] == 404
        assert json.loads(reply["__raw_body__"]) == {"error": "not found"}


class TestTopologyAgentIsolated:
    """TopologyAgent-only behavioral checks."""

    def setup_method(self) -> None:
        self.agent = TopologyAgent(name="ta")

    @pytest.mark.asyncio
    async def test_no_root_supervisor_is_error_not_crash(self) -> None:
        reply = await self.agent.handle_call({"__op__": "topology"}, "tester")
        body = json.loads(reply["__raw_body__"])
        assert body == {"error": "runtime not available"}
        assert reply["__status__"] == 200  # matches the /topology shape

    @pytest.mark.asyncio
    async def test_processes_probes_bus_for_remote_workers(self) -> None:
        """v0.9.1 D-DASH-3's real-bus-round-trip path works through
        TopologyAgent -- GenServer already provides self._bus, no new wiring
        needed for the one op that isn't a pure same-process read."""
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
        every op, including the /agents bare-array case, must return a dict at
        the handle_call() level (the array lives inside __raw_body__ as a
        STRING, not as the top-level return value)."""
        _wire_state(self.agent)
        for op in ("health", "topology", "agents", "snapshot", "metrics", "processes"):
            reply = await self.agent.handle_call({"__op__": op}, "tester")
            assert isinstance(reply, dict)
