"""Tests for TopologyServer and the topology CLI live-ping path."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from civitas import DynamicSupervisor, Runtime, Supervisor, TopologyServer
from civitas.messages import Message
from tests.conftest import wait_for

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_agent(name: str, status: str = "RUNNING") -> MagicMock:
    """v0.9.1 (D-DASH-1): sets real, JSON-serializable values for the fields
    TopologyServer's serializers now read (capabilities/capability_metadata/
    uptime_seconds, and v0.9.4's session_turn_count/session_duration_seconds)
    — a bare MagicMock() auto-vivifies those as further MagicMocks, which
    json.dumps() cannot serialize.
    """
    agent = MagicMock()
    agent.name = name
    agent.status = MagicMock()
    agent.status.value = status
    agent.capabilities = []
    agent.capability_metadata = {}
    agent.uptime_seconds = 0.0
    agent.session_turn_count = 0
    agent.session_duration_seconds = 0.0
    return agent


def _make_mock_child_rec(agent: MagicMock) -> MagicMock:
    """Wrap a mock agent as a _ChildRec-like value (R1 — .agent holds the agent)."""
    rec = MagicMock()
    rec.agent = agent
    return rec


def _make_mock_supervisor(
    name: str,
    strategy: str = "ONE_FOR_ONE",
    children: list[Any] | None = None,
    restart_counts: dict[str, int] | None = None,
    crashes_in_window: int = 0,
) -> MagicMock:
    sup = MagicMock(spec=Supervisor)
    sup.name = name
    sup.strategy = MagicMock()
    sup.strategy.value = strategy
    sup.children = children or []
    # v0.9.1 (D-DASH-1): _restart_counts/_engine.window are real instance
    # attributes TopologyServer now reads directly (same-process, no bus hop).
    sup._restart_counts = restart_counts or {}
    sup._engine = MagicMock()
    sup._engine.window = [0.0] * crashes_in_window
    return sup


def _make_mock_dyn(
    name: str,
    max_children: int = 10,
    max_total_spawns: int | None = None,
    dynamic_children: dict[str, Any] | None = None,
) -> MagicMock:
    dyn = MagicMock(spec=DynamicSupervisor)
    dyn.name = name
    dyn.status = MagicMock()
    dyn.status.value = "RUNNING"
    dyn.max_children = max_children
    dyn.max_total_spawns = max_total_spawns
    dyn._dynamic_children = {
        n: _make_mock_child_rec(a) for n, a in (dynamic_children or {}).items()
    }
    dyn._child_restart_counts = {}  # v0.9.1 (D-DASH-1)
    return dyn


# ---------------------------------------------------------------------------
# Unit: _route_http dispatch
# ---------------------------------------------------------------------------


class TestRouteHttp:
    def setup_method(self) -> None:
        self.ts = TopologyServer(name="ts", port=0)

    async def test_health(self) -> None:
        body, code, content_type = await self.ts._route_http("/health")
        assert code == 200
        assert content_type == "application/json"
        assert json.loads(body) == {"status": "ok"}

    async def test_unknown_path(self) -> None:
        body, code, content_type = await self.ts._route_http("/notexist")
        assert code == 404
        assert content_type == "application/json"
        assert "error" in json.loads(body)

    async def test_topology_no_runtime(self) -> None:
        body, code, _ = await self.ts._route_http("/topology")
        assert code == 200
        data = json.loads(body)
        assert "error" in data

    async def test_agents_no_runtime(self) -> None:
        body, code, _ = await self.ts._route_http("/agents")
        assert code == 200
        assert isinstance(json.loads(body), list)

    async def test_agent_detail_not_found(self) -> None:
        body, code, _ = await self.ts._route_http("/agents/missing")
        assert code == 404
        assert "not found" in json.loads(body)["error"]

    async def test_snapshot_route_exists(self) -> None:
        """v0.9.3.1: /metrics (JSON) was renamed to /snapshot to make room
        for real Prometheus exposition at the standard /metrics path."""
        body, code, content_type = await self.ts._route_http("/snapshot")
        assert code == 404  # no MetricsCollector wired in this bare TopologyServer
        assert content_type == "application/json"
        assert "error" in json.loads(body)

    async def test_metrics_route_is_prometheus_text(self) -> None:
        """v0.9.3.1: /metrics is now Prometheus text-format exposition, not
        civitas's own JSON snapshot (see /snapshot above)."""
        from civitas.observability.prometheus_export import PROMETHEUS_CONTENT_TYPE

        body, code, content_type = await self.ts._route_http("/metrics")
        assert code == 200
        assert content_type == PROMETHEUS_CONTENT_TYPE
        assert body == ""  # no MetricsCollector wired -- empty, not an error


# ---------------------------------------------------------------------------
# Unit: serialisers
# ---------------------------------------------------------------------------


class TestSerializers:
    def setup_method(self) -> None:
        self.ts = TopologyServer(name="ts", port=0)

    def test_serialize_agent(self) -> None:
        agent = _make_mock_agent("worker-1", "RUNNING")
        result = self.ts._serialize_node(agent)
        # v0.9.1 (D-DASH-1): capabilities/capability_metadata/uptime_seconds/
        # restart_count are new fields on every serialized agent node.
        # process_id (D-DASH addendum, 2026-07-26): no registry wired here, so
        # it falls back to this TopologyServer's own name (same-process case).
        # session_turn_count/session_duration_seconds (v0.9.4): new fields too.
        assert result == {
            "name": "worker-1",
            "type": "agent",
            "status": "RUNNING",
            "restart_count": 0,
            "process_id": "ts",
            "capabilities": [],
            "capability_metadata": {},
            "uptime_seconds": 0.0,
            "session_turn_count": 0,
            "session_duration_seconds": 0.0,
        }

    def test_serialize_supervisor(self) -> None:
        child = _make_mock_agent("child-a")
        sup = _make_mock_supervisor("my-sup", children=[child])
        result = self.ts._serialize_node(sup)
        assert result["name"] == "my-sup"
        assert result["type"] == "supervisor"
        assert result["strategy"] == "ONE_FOR_ONE"
        assert len(result["children"]) == 1

    def test_serialize_supervisor_children_get_own_restart_count(self) -> None:
        """v0.9.1 (D-DASH-1): restart_count is attributed by the PARENT to each
        child individually — a supervisor with two children and only one
        crashed must not conflate their counts (the bug the naive "sum at the
        parent" approach would have introduced).
        """
        flaky = _make_mock_agent("flaky")
        stable = _make_mock_agent("stable")
        sup = _make_mock_supervisor("root", children=[flaky, stable], restart_counts={"flaky": 3})
        result = self.ts._serialize_node(sup)
        children_by_name = {c["name"]: c for c in result["children"]}
        assert children_by_name["flaky"]["restart_count"] == 3
        assert children_by_name["stable"]["restart_count"] == 0

    def test_serialize_supervisor_reports_own_crashes_in_window(self) -> None:
        """crashes_in_window is the SUPERVISOR's own restart-window occupancy
        (v0.9.1, D-DASH-1) — same field civitas.supervision.status already
        computes (v0.9.0 Phase C), now also on /topology."""
        sup = _make_mock_supervisor("root", crashes_in_window=2)
        result = self.ts._serialize_node(sup)
        assert result["crashes_in_window"] == 2

    def test_serialize_nested_supervisor_restart_count_attributed_correctly(self) -> None:
        """A child SUPERVISOR's own restart_count (as tracked by ITS parent) is
        distinct from its crashes_in_window (its own escalation budget) —
        v0.9.1 (D-DASH-1) exercises both together in one nested tree."""
        inner = _make_mock_supervisor("inner", crashes_in_window=1)
        root = _make_mock_supervisor("root", children=[inner], restart_counts={"inner": 2})
        result = self.ts._serialize_node(root)
        assert result["children"][0]["restart_count"] == 2  # from root's tracking
        assert result["children"][0]["crashes_in_window"] == 1  # inner's own window

    def test_serialize_dynamic_supervisor(self) -> None:
        dyn_child = _make_mock_agent("dyn-1")
        dyn = _make_mock_dyn("workers", max_children=5, dynamic_children={"dyn-1": dyn_child})
        result = self.ts._serialize_node(dyn)
        assert result["type"] == "dynamic_supervisor"
        assert result["live_count"] == 1
        assert result["max_children"] == 5
        assert result["children"][0]["name"] == "dyn-1"

    async def test_serialize_nested_supervisor(self) -> None:
        leaf = _make_mock_agent("leaf")
        inner = _make_mock_supervisor("inner", children=[leaf])
        root = _make_mock_supervisor("root", children=[inner])
        self.ts._root_supervisor = root
        body, code, _ = await self.ts._route_http("/topology")
        assert code == 200
        data = json.loads(body)
        assert data["name"] == "root"
        assert data["children"][0]["name"] == "inner"

    def test_build_agents_list_includes_static(self) -> None:
        agent = _make_mock_agent("static-1")
        self.ts._agents = {"static-1": agent}
        result = self.ts._build_agents_list()
        assert any(a["name"] == "static-1" for a in result)

    def test_build_agents_list_includes_dynamic(self) -> None:
        dyn_child = _make_mock_agent("dyn-1")
        dyn = _make_mock_dyn("workers", dynamic_children={"dyn-1": dyn_child})
        root = _make_mock_supervisor("root", children=[dyn])
        self.ts._root_supervisor = root
        result = self.ts._build_agents_list()
        assert any(a["name"] == "dyn-1" for a in result)

    def test_build_agent_detail_found(self) -> None:
        agent = _make_mock_agent("svc", "RUNNING")
        self.ts._agents = {"svc": agent}
        result = self.ts._build_agent_detail("svc")
        # v0.9.1 (D-DASH-1): capabilities/capability_metadata/uptime_seconds
        # are new fields on every agent-detail response. process_id (D-DASH
        # addendum, 2026-07-26): no registry wired, falls back to this
        # TopologyServer's own name (same-process case).
        # session_turn_count/session_duration_seconds (v0.9.4): new fields too.
        assert result == {
            "name": "svc",
            "status": "RUNNING",
            "process_id": "ts",
            "capabilities": [],
            "capability_metadata": {},
            "uptime_seconds": 0.0,
            "session_turn_count": 0,
            "session_duration_seconds": 0.0,
        }

    def test_build_agent_detail_not_found(self) -> None:
        assert self.ts._build_agent_detail("ghost") is None

    def test_build_agent_detail_searches_dynamic(self) -> None:
        dyn_child = _make_mock_agent("dyn-2", "RUNNING")
        dyn = _make_mock_dyn("workers", dynamic_children={"dyn-2": dyn_child})
        root = _make_mock_supervisor("root", children=[dyn])
        self.ts._root_supervisor = root
        result = self.ts._build_agent_detail("dyn-2")
        assert result is not None
        assert result["name"] == "dyn-2"

    async def test_agents_endpoint_returns_agent_detail(self) -> None:
        agent = _make_mock_agent("svc", "RUNNING")
        self.ts._agents = {"svc": agent}
        body, code, _ = await self.ts._route_http("/agents/svc")
        assert code == 200
        assert json.loads(body)["name"] == "svc"


# ---------------------------------------------------------------------------
# Integration: TopologyServer via Runtime
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_topology_server_starts_and_stops() -> None:
    """TopologyServer starts without error and stops cleanly."""
    ts = TopologyServer(name="topo", port=16788)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    try:
        agents = runtime.all_agents()
        assert any(isinstance(a, TopologyServer) for a in agents)
    finally:
        await runtime.stop()


async def _http_get(url: str) -> tuple[int, bytes]:
    """Async HTTP GET using asyncio streams (no blocking calls in event loop)."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80
    path = parsed.path or "/"

    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(
            f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n".encode()
        )
        await writer.drain()
        raw = await reader.read(65536)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    # Split status line from body
    header_end = raw.find(b"\r\n\r\n")
    headers_raw = raw[:header_end].decode(errors="replace") if header_end != -1 else ""
    body = raw[header_end + 4 :] if header_end != -1 else b""
    status_line = headers_raw.splitlines()[0] if headers_raw else "HTTP/1.1 500"
    code = int(status_line.split()[1]) if len(status_line.split()) >= 2 else 500
    return code, body


@pytest.mark.asyncio
async def test_topology_server_http_health() -> None:
    """TopologyServer /health endpoint responds 200."""
    ts = TopologyServer(name="topo", port=16789)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    await asyncio.sleep(0.05)
    try:
        code, body = await _http_get("http://127.0.0.1:16789/health")
        assert code == 200
        assert json.loads(body) == {"status": "ok"}
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_topology_server_http_topology() -> None:
    """TopologyServer /topology endpoint returns supervision tree."""
    ts = TopologyServer(name="topo", port=16790)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    await asyncio.sleep(0.05)
    try:
        code, body = await _http_get("http://127.0.0.1:16790/topology")
        assert code == 200
        data = json.loads(body)
        assert data["name"] == "root"
        assert data["type"] == "supervisor"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_topology_server_http_agents() -> None:
    """TopologyServer /agents endpoint returns flat list."""
    ts = TopologyServer(name="topo", port=16791)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    await asyncio.sleep(0.05)
    try:
        code, body = await _http_get("http://127.0.0.1:16791/agents")
        assert code == 200
        data = json.loads(body)
        assert isinstance(data, list)
        names = [a["name"] for a in data]
        assert "topo" in names
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_topology_server_http_agent_detail() -> None:
    """TopologyServer /agents/{name} returns detail or 404."""
    ts = TopologyServer(name="topo", port=16792)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    await asyncio.sleep(0.05)
    try:
        code, body = await _http_get("http://127.0.0.1:16792/agents/topo")
        assert code == 200
        assert json.loads(body)["name"] == "topo"

        code404, body404 = await _http_get("http://127.0.0.1:16792/agents/ghost")
        assert code404 == 404
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_topology_server_http_uptime_resets_after_restart() -> None:
    """v0.9.1 (D-DASH-1) end-to-end: uptime_seconds is per-INCARNATION, not
    per-child-lifetime — a crash-restart (D1a fresh instance) resets it,
    exactly like the fresh-instance restart semantics uptime is meant to
    reflect. Real Supervisor + real crashing agent, not a mock."""
    from civitas.process import AgentProcess

    class _CrashOnce(AgentProcess):
        async def handle(self, message: Message) -> Message | None:
            if message.payload.get("cmd") == "boom":
                raise RuntimeError("boom")
            return None

    ts = TopologyServer(name="topo", port=16793)
    worker = _CrashOnce("worker")
    root = Supervisor("root", children=[ts, worker], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        # Give the FIRST incarnation a long, unambiguous uptime before crashing
        # it, so "did it reset" has a wide, non-flaky margin to check against.
        await asyncio.sleep(1.0)
        code, body = await _http_get("http://127.0.0.1:16793/agents/worker")
        assert code == 200
        first_uptime = json.loads(body)["uptime_seconds"]
        assert first_uptime > 0.8

        await runtime.send("worker", {"cmd": "boom"})
        await asyncio.sleep(0.2)  # crash + backoff (0.01s) + fresh-incarnation restart

        code2, body2 = await _http_get("http://127.0.0.1:16793/agents/worker")
        assert code2 == 200
        second_uptime = json.loads(body2)["uptime_seconds"]
        # A NOT-reset uptime would be >= first_uptime (it would have kept
        # accumulating from the original start, now over 1.2s); a reset one is
        # a small fraction of it, well under a second.
        assert second_uptime < 0.5
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_auto_provisions_metrics_collector_for_topology_server() -> None:
    """v0.9.1 (D-DASH-4): a Runtime with a TopologyServer and no explicit
    metrics= gets a real MetricsCollector for free — /metrics has something
    to read without any extra caller wiring."""
    from civitas.dashboard.collector import MetricsCollector

    ts = TopologyServer(name="topo", port=16794)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    try:
        assert isinstance(runtime._metrics, MetricsCollector)
        assert ts._metrics_collector is runtime._metrics
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_respects_explicit_custom_metrics_sink() -> None:
    """v0.9.1 (D-DASH-4): a caller's own metrics= sink is never overridden —
    auto-provisioning only fires when metrics is None. A non-MetricsCollector
    sink means /snapshot reports 'not available', not a silent empty snapshot.
    (v0.9.3.1: this was /metrics at the time -- renamed to /snapshot.)"""

    class _CustomSink:
        def message_handled(self, agent_name: str, latency_ms: float) -> None: ...
        def message_sent(self, agent_name: str) -> None: ...
        def agent_error(self, agent_name: str) -> None: ...
        def agent_restarted(self, agent_name: str, reason: str = "") -> None: ...

    custom = _CustomSink()
    ts = TopologyServer(name="topo", port=16795)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]), metrics=custom)
    await runtime.start()
    try:
        assert runtime._metrics is custom  # never overridden
        assert ts._metrics_collector is None  # not a MetricsCollector
        code, body = await _http_get("http://127.0.0.1:16795/snapshot")
        assert code == 404
        assert json.loads(body) == {"error": "metrics not available"}
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_topology_server_http_metrics_shape() -> None:
    """v0.9.1 (D-DASH-2): /snapshot returns the documented shape, reflecting
    real message-handling activity end-to-end (no mocks). (v0.9.3.1: this was
    /metrics at the time -- renamed to /snapshot.)"""
    from civitas.process import AgentProcess

    class _Echo(AgentProcess):
        async def handle(self, message: Message) -> Message | None:
            return self.reply({"ok": True})

    ts = TopologyServer(name="topo", port=16796)
    echo = _Echo("echo")
    runtime = Runtime(supervisor=Supervisor("root", children=[ts, echo]))
    await runtime.start()
    try:
        await runtime.ask("echo", {"q": 1})
        code, body = await _http_get("http://127.0.0.1:16796/snapshot")
        assert code == 200
        data = json.loads(body)
        assert "echo" in data["agents"]
        echo_metrics = data["agents"]["echo"]
        assert echo_metrics["messages_handled"] == 1
        assert echo_metrics["avg_latency_ms"] >= 0.0
        assert echo_metrics["last_model"] == ""  # nothing reported an LLM call yet
        assert data["total_messages"] >= 1
        assert data["uptime_seconds"] >= 0.0
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_topology_server_http_metrics_includes_dynamically_spawned_agent() -> None:
    """v0.9.1 (dashboard-v2 D-DASH addendum): a DynamicSupervisor-spawned
    child — never known to Runtime's static all_agents() registration loop —
    still shows up in /snapshot with real numbers, via MetricsCollector's lazy
    self-registration. This is the actual fix for the gap Phase B's design
    addendum flagged as a documented limitation; it is no longer one.
    """
    from tests.conftest import EchoAgent

    ts = TopologyServer(name="topo", port=16797)
    dyn = DynamicSupervisor("workers")
    runtime = Runtime(supervisor=Supervisor("root", children=[ts, dyn]))
    await runtime.start()
    try:
        await runtime.spawn("workers", EchoAgent, "spawned-1")
        await runtime.ask("spawned-1", {"msg": "hi"})

        code, body = await _http_get("http://127.0.0.1:16797/snapshot")
        assert code == 200
        data = json.loads(body)
        assert "spawned-1" in data["agents"]
        assert data["agents"]["spawned-1"]["messages_handled"] == 1
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_topology_server_http_processes_includes_runtime_self_sample() -> None:
    """v0.9.1 (D-DASH-3): /processes always includes at least the Runtime's
    own self-measured process — no Worker/remote channel needed for this
    part. Uses tcp:// (not ipc://) so this test stays Windows-compatible
    (design dashboard-v2.md §12 cross-platform note).
    """
    ts = TopologyServer(name="topo", port=16798)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    try:
        code, body = await _http_get("http://127.0.0.1:16798/processes")
        assert code == 200
        data = json.loads(body)
        runtime_entries = [p for p in data["processes"] if p["kind"] == "runtime"]
        assert len(runtime_entries) == 1
        entry = runtime_entries[0]
        assert entry["id"] == "topo"
        assert entry["pid"] > 0
        assert entry["cpu_percent"] >= 0.0
        assert entry["rss_bytes"] > 0
    finally:
        await runtime.stop()


class TestProcessIdLinkage:
    """v0.9.1 (D-DASH addendum, 2026-07-26): every agent's process_id must
    match one of /processes' own 'id' fields exactly, so a client can join
    the two endpoints directly — tested as an explicit contract, not just
    that SOME string is present.
    """

    def setup_method(self) -> None:
        self.ts = TopologyServer(name="ts", port=0)

    def test_local_agent_falls_back_to_own_name(self) -> None:
        """No registry wired (or no health_channel entry) => runs in the
        SAME process as this TopologyServer — matches /processes' runtime
        entry, whose id is this server's own name."""
        assert self.ts._process_id_for("any-agent") == "ts"

    def test_remote_agent_uses_its_workers_health_channel(self) -> None:
        """A registered agent with a health_channel => hosted on that
        Worker — matches /processes' worker entry, whose id is the channel."""
        self.ts._registry = _fake_registry_with_channels({"remote-agent": "chan-1"})
        assert self.ts._process_id_for("remote-agent") == "chan-1"

    def test_registered_but_no_channel_still_falls_back(self) -> None:
        """A registry entry that exists but carries no health_channel (a
        pre-v0.9 worker, or a purely local registration) is still local."""
        registry = MagicMock()
        entry = MagicMock()
        entry.health_channel = ""
        registry.lookup.return_value = entry
        self.ts._registry = registry
        assert self.ts._process_id_for("agent") == "ts"


async def test_topology_server_http_process_id_matches_processes_endpoint() -> None:
    """v0.9.1 (D-DASH addendum) end-to-end: a real agent's /topology
    process_id equals the exact 'id' of its /processes entry — the actual
    contract the linkage exists to provide, proven over real HTTP, not just
    unit-level on _process_id_for() in isolation."""
    ts = TopologyServer(name="topo", port=16800)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    try:
        topo_code, topo_body = await _http_get("http://127.0.0.1:16800/topology")
        assert topo_code == 200
        topo_data = json.loads(topo_body)
        assert topo_data["process_id"] == "topo"  # the root Supervisor's own process_id

        proc_code, proc_body = await _http_get("http://127.0.0.1:16800/processes")
        assert proc_code == 200
        proc_data = json.loads(proc_body)
        process_ids = {p["id"] for p in proc_data["processes"]}
        assert topo_data["process_id"] in process_ids  # the actual join contract
    finally:
        await runtime.stop()


async def test_topology_server_http_metrics_includes_restart_history() -> None:
    """v0.9.1 (D-DASH addendum, 2026-07-26): restart_history was already
    collected by MetricsCollector but never exposed (v0.9.3.1: this endpoint
    was /metrics at the time -- renamed to /snapshot) — a real, safe,
    read-only timeline, verified end-to-end with a real crash-restart."""
    from civitas.process import AgentProcess

    class _CrashOnce(AgentProcess):
        async def handle(self, message: Message) -> Message | None:
            if message.payload.get("cmd") == "boom":
                raise RuntimeError("boom")
            return None

    ts = TopologyServer(name="topo", port=16801)
    worker = _CrashOnce("flaky")
    root = Supervisor("root", children=[ts, worker], max_restarts=3, backoff_base=0.01)
    runtime = Runtime(supervisor=root)
    await runtime.start()
    try:
        await runtime.send("flaky", {"cmd": "boom"})
        await wait_for(lambda: root._restart_counts.get("flaky", 0) >= 1)
        await asyncio.sleep(0.05)  # let the restart complete before polling

        code, body = await _http_get("http://127.0.0.1:16801/snapshot")
        assert code == 200
        data = json.loads(body)
        assert "restart_history" in data
        events = [e for e in data["restart_history"] if e["agent_name"] == "flaky"]
        assert len(events) >= 1
        assert events[0]["timestamp"] > 0
    finally:
        await runtime.stop()


async def test_topology_server_http_metrics_is_real_prometheus_text() -> None:
    """v0.9.3.1: /metrics (the standard Prometheus scrape path, no
    metrics_path override needed) returns real text-format exposition
    reflecting actual message-handling activity end-to-end (no mocks) --
    the same underlying MetricsCollector data /snapshot exposes as JSON.
    """
    from civitas.observability.prometheus_export import PROMETHEUS_CONTENT_TYPE
    from civitas.process import AgentProcess

    class _Echo(AgentProcess):
        async def handle(self, message: Message) -> Message | None:
            return self.reply({"ok": True})

    ts = TopologyServer(name="topo", port=16802)
    echo = _Echo("echo")
    runtime = Runtime(supervisor=Supervisor("root", children=[ts, echo]))
    await runtime.start()
    try:
        await runtime.ask("echo", {"q": 1})

        reader, writer = await asyncio.open_connection("127.0.0.1", 16802)
        writer.write(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        await writer.drain()
        raw = await asyncio.wait_for(reader.read(), timeout=5.0)
        writer.close()

        header, _, body = raw.partition(b"\r\n\r\n")
        header_text = header.decode()
        assert "200" in header_text.split("\r\n")[0]
        assert f"Content-Type: {PROMETHEUS_CONTENT_TYPE}" in header_text

        text = body.decode()
        assert 'civitas_messages_handled_total{agent="echo"} 1' in text
        assert "# HELP civitas_messages_handled_total" in text
        assert "# TYPE civitas_messages_handled_total counter" in text
        # echo never made an LLM call -- no all-zero LLM series should appear
        assert "civitas_llm_cost_usd_total" not in text
    finally:
        await runtime.stop()


def _fake_registry_with_channels(channels: dict[str, str]) -> MagicMock:
    """A minimal fake Registry: all_names() + lookup(name).health_channel,
    the only two members _distinct_health_channels() touches."""
    registry = MagicMock()
    registry.all_names.return_value = list(channels)

    def _lookup(name: str) -> MagicMock | None:
        channel = channels.get(name)
        if channel is None:
            return None
        entry = MagicMock()
        entry.health_channel = channel
        return entry

    registry.lookup.side_effect = _lookup
    return registry


async def test_topology_server_distinct_health_channels_deduplicates() -> None:
    """v0.9.1 (D-DASH-3): two agents hosted on the SAME Worker (one health
    channel) must probe that Worker exactly once, not twice."""
    ts = TopologyServer(name="topo", port=0)
    ts._registry = _fake_registry_with_channels(
        {"agent-a": "chan-1", "agent-b": "chan-1", "agent-c": "chan-2"}
    )
    assert ts._distinct_health_channels() == {"chan-1", "chan-2"}


async def test_topology_server_probe_worker_process_timeout_returns_none() -> None:
    """v0.9.1 (D-DASH-3): an unreachable Worker is simply absent from
    /processes, not an error response for the whole endpoint."""
    ts = TopologyServer(name="topo", port=0)

    class _DeadBus:
        async def request(self, message: Message, timeout: float) -> Message:
            raise TimeoutError

    ts._bus = _DeadBus()  # type: ignore[assignment]
    result = await ts._probe_worker_process("chan-1")
    assert result is None


# ---------------------------------------------------------------------------
# Unit: CLI helpers — _find_topology_server, _try_live_topology,
#         _build_rich_tree_from_live, _add_children dynamic branches
# ---------------------------------------------------------------------------


class TestFindTopologyServer:
    def test_finds_topology_server_in_root_children(self) -> None:
        from civitas.cli.topology import _find_topology_server

        config = {
            "supervision": {
                "name": "root",
                "children": [
                    {
                        "type": "topology_server",
                        "name": "ts",
                        "config": {"host": "127.0.0.1", "port": 9999},
                    }
                ],
            }
        }
        result = _find_topology_server(config)
        assert result == ("127.0.0.1", 9999)

    def test_returns_none_when_absent(self) -> None:
        from civitas.cli.topology import _find_topology_server

        config = {
            "supervision": {
                "name": "root",
                "children": [{"type": "dynamic_supervisor", "name": "workers"}],
            }
        }
        assert _find_topology_server(config) is None

    def test_finds_nested_inside_supervisor(self) -> None:
        from civitas.cli.topology import _find_topology_server

        config = {
            "supervision": {
                "name": "root",
                "children": [
                    {
                        "supervisor": {
                            "name": "inner",
                            "children": [
                                {
                                    "type": "topology_server",
                                    "name": "ts",
                                    "config": {"host": "0.0.0.0", "port": 7777},
                                }
                            ],
                        }
                    }
                ],
            }
        }
        result = _find_topology_server(config)
        assert result == ("0.0.0.0", 7777)

    def test_defaults_host_and_port(self) -> None:
        from civitas.cli.topology import _find_topology_server

        config = {
            "supervision": {
                "name": "root",
                "children": [{"type": "topology_server", "name": "ts"}],
            }
        }
        result = _find_topology_server(config)
        assert result == ("127.0.0.1", 6789)


class TestTryLiveTopology:
    def test_returns_none_on_connection_error(self) -> None:
        from civitas.cli.topology import _try_live_topology

        # Nothing listening on this port
        result = _try_live_topology("127.0.0.1", 19999)
        assert result is None

    def test_returns_parsed_json_on_success(self) -> None:
        from civitas.cli.topology import _try_live_topology

        fake_data = {"name": "root", "type": "supervisor", "children": []}
        mock_response = MagicMock()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.read = MagicMock(return_value=json.dumps(fake_data).encode())

        with patch("civitas.cli.topology.urlopen", return_value=mock_response):
            result = _try_live_topology("127.0.0.1", 6789)

        assert result == fake_data

    def test_returns_none_on_bad_json(self) -> None:
        from civitas.cli.topology import _try_live_topology

        mock_response = MagicMock()
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_response.read = MagicMock(return_value=b"not-json{{{")

        with patch("civitas.cli.topology.urlopen", return_value=mock_response):
            result = _try_live_topology("127.0.0.1", 6789)

        assert result is None


class TestBuildRichTreeFromLive:
    def test_supervisor_root(self) -> None:
        from civitas.cli.topology import _build_rich_tree_from_live

        data = {
            "name": "root",
            "type": "supervisor",
            "strategy": "ONE_FOR_ONE",
            "children": [],
        }
        tree = _build_rich_tree_from_live(data)
        assert "root" in tree.label

    def test_dynamic_supervisor_root(self) -> None:
        from civitas.cli.topology import _build_rich_tree_from_live

        data = {
            "name": "workers",
            "type": "dynamic_supervisor",
            "status": "RUNNING",
            "live_count": 3,
            "max_children": 10,
            "children": [],
        }
        tree = _build_rich_tree_from_live(data)
        assert "workers" in tree.label

    def test_agent_root(self) -> None:
        from civitas.cli.topology import _build_rich_tree_from_live

        data = {"name": "my-agent", "type": "agent", "status": "RUNNING"}
        tree = _build_rich_tree_from_live(data)
        assert "my-agent" in tree.label

    def test_nested_children_rendered(self) -> None:
        from civitas.cli.topology import _build_rich_tree_from_live

        data = {
            "name": "root",
            "type": "supervisor",
            "strategy": "ONE_FOR_ONE",
            "children": [
                {"name": "agent-1", "type": "agent", "status": "RUNNING"},
                {
                    "name": "workers",
                    "type": "dynamic_supervisor",
                    "status": "RUNNING",
                    "live_count": 1,
                    "max_children": 5,
                    "children": [{"name": "dyn-1", "type": "agent", "status": "RUNNING"}],
                },
            ],
        }
        tree = _build_rich_tree_from_live(data)
        assert len(tree.children) == 2


class TestAddChildrenDynamicBranches:
    """Test _add_children handles dynamic_supervisor and topology_server nodes."""

    def test_dynamic_supervisor_rendered(self) -> None:
        from civitas.cli.topology import _build_rich_tree

        config = {
            "supervision": {
                "name": "root",
                "strategy": "ONE_FOR_ONE",
                "children": [{"type": "dynamic_supervisor", "name": "workers", "max_children": 20}],
            }
        }
        tree = _build_rich_tree(config)
        # Tree has one child for "workers"
        assert len(tree.children) == 1
        assert "workers" in tree.children[0].label

    def test_topology_server_rendered(self) -> None:
        from civitas.cli.topology import _build_rich_tree

        config = {
            "supervision": {
                "name": "root",
                "strategy": "ONE_FOR_ONE",
                "children": [
                    {
                        "type": "topology_server",
                        "name": "ts",
                        "config": {"host": "127.0.0.1", "port": 6789},
                    }
                ],
            }
        }
        tree = _build_rich_tree(config)
        assert len(tree.children) == 1
        assert "ts" in tree.children[0].label


class TestTopologyShowCommand:
    """Test topology show command live vs. static path."""

    def test_show_static_when_no_topo_server(self, tmp_path: Any) -> None:
        from typer.testing import CliRunner

        from civitas.cli.app import app

        topo_file = tmp_path / "topo.yaml"
        topo_file.write_text(
            "supervision:\n  name: root\n  strategy: ONE_FOR_ONE\n"
            "  children:\n    - agent:\n        name: a\n        type: myapp.A\n"
        )
        runner = CliRunner()
        result = runner.invoke(app, ["topology", "show", str(topo_file)])
        assert result.exit_code == 0
        assert "root" in result.output

    def test_show_fallback_when_runtime_not_running(self, tmp_path: Any) -> None:
        from typer.testing import CliRunner

        from civitas.cli.app import app

        topo_file = tmp_path / "topo.yaml"
        topo_file.write_text(
            "supervision:\n  name: root\n  strategy: ONE_FOR_ONE\n"
            "  children:\n"
            "    - type: topology_server\n      name: ts\n"
            "      config: {host: '127.0.0.1', port: 29999}\n"
        )
        runner = CliRunner()
        result = runner.invoke(app, ["topology", "show", str(topo_file)])
        assert result.exit_code == 0
        # Falls back to static with annotation. Rich word-wraps to the
        # terminal width, which is narrower in some CI/container environments
        # (the exact V1 class of bug, v0.8.1) — normalize whitespace before
        # the substring check so a mid-phrase wrap doesn't break the match.
        assert "runtime not running" in " ".join(result.output.split())

    def test_show_live_when_runtime_available(self, tmp_path: Any) -> None:
        from unittest.mock import patch

        from typer.testing import CliRunner

        from civitas.cli.app import app

        topo_file = tmp_path / "topo.yaml"
        topo_file.write_text(
            "supervision:\n  name: root\n  strategy: ONE_FOR_ONE\n"
            "  children:\n"
            "    - type: topology_server\n      name: ts\n"
            "      config: {host: '127.0.0.1', port: 29998}\n"
        )
        fake_live = {
            "name": "root",
            "type": "supervisor",
            "strategy": "ONE_FOR_ONE",
            "children": [],
        }
        runner = CliRunner()
        with patch("civitas.cli.topology._try_live_topology", return_value=fake_live):
            result = runner.invoke(app, ["topology", "show", str(topo_file)])
        assert result.exit_code == 0
        assert "live" in result.output
