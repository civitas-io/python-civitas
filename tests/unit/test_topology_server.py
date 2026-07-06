"""Tests for TopologyServer and the topology CLI live-ping path."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from civitas import DynamicSupervisor, Runtime, Supervisor, TopologyServer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_agent(name: str, status: str = "RUNNING") -> MagicMock:
    agent = MagicMock()
    agent.name = name
    agent.status = MagicMock()
    agent.status.value = status
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
) -> MagicMock:
    sup = MagicMock(spec=Supervisor)
    sup.name = name
    sup.strategy = MagicMock()
    sup.strategy.value = strategy
    sup.children = children or []
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
    return dyn


# ---------------------------------------------------------------------------
# Unit: _route_http dispatch
# ---------------------------------------------------------------------------


class TestRouteHttp:
    def setup_method(self) -> None:
        self.ts = TopologyServer(name="ts", port=0)

    def test_health(self) -> None:
        body, code = self.ts._route_http("/health")
        assert code == 200
        assert json.loads(body) == {"status": "ok"}

    def test_unknown_path(self) -> None:
        body, code = self.ts._route_http("/notexist")
        assert code == 404
        assert "error" in json.loads(body)

    def test_topology_no_runtime(self) -> None:
        body, code = self.ts._route_http("/topology")
        assert code == 200
        data = json.loads(body)
        assert "error" in data

    def test_agents_no_runtime(self) -> None:
        body, code = self.ts._route_http("/agents")
        assert code == 200
        assert isinstance(json.loads(body), list)

    def test_agent_detail_not_found(self) -> None:
        body, code = self.ts._route_http("/agents/missing")
        assert code == 404
        assert "not found" in json.loads(body)["error"]


# ---------------------------------------------------------------------------
# Unit: serialisers
# ---------------------------------------------------------------------------


class TestSerializers:
    def setup_method(self) -> None:
        self.ts = TopologyServer(name="ts", port=0)

    def test_serialize_agent(self) -> None:
        agent = _make_mock_agent("worker-1", "RUNNING")
        result = self.ts._serialize_node(agent)
        assert result == {"name": "worker-1", "type": "agent", "status": "RUNNING"}

    def test_serialize_supervisor(self) -> None:
        child = _make_mock_agent("child-a")
        sup = _make_mock_supervisor("my-sup", children=[child])
        result = self.ts._serialize_node(sup)
        assert result["name"] == "my-sup"
        assert result["type"] == "supervisor"
        assert result["strategy"] == "ONE_FOR_ONE"
        assert len(result["children"]) == 1

    def test_serialize_dynamic_supervisor(self) -> None:
        dyn_child = _make_mock_agent("dyn-1")
        dyn = _make_mock_dyn("workers", max_children=5, dynamic_children={"dyn-1": dyn_child})
        result = self.ts._serialize_node(dyn)
        assert result["type"] == "dynamic_supervisor"
        assert result["live_count"] == 1
        assert result["max_children"] == 5
        assert result["children"][0]["name"] == "dyn-1"

    def test_serialize_nested_supervisor(self) -> None:
        leaf = _make_mock_agent("leaf")
        inner = _make_mock_supervisor("inner", children=[leaf])
        root = _make_mock_supervisor("root", children=[inner])
        self.ts._root_supervisor = root
        body, code = self.ts._route_http("/topology")
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
        assert result == {"name": "svc", "status": "RUNNING"}

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

    def test_agents_endpoint_returns_agent_detail(self) -> None:
        agent = _make_mock_agent("svc", "RUNNING")
        self.ts._agents = {"svc": agent}
        body, code = self.ts._route_http("/agents/svc")
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
        # Falls back to static with annotation
        assert "runtime not running" in result.output

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
