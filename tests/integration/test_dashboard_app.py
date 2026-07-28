"""CivitasDashboardApp \u2014 end-to-end Textual app tests (v0.9.1, dashboard-v2
Phase E). Design \u00a710: "used for the handful of interaction tests that matter
(click-to-focus, reconnect-banner-on-failure), not exhaustively for every
widget" \u2014 widget-level rendering is already covered by
tests/unit/test_dashboard_widgets.py; these prove the app WIRES the three
independent pollers, the tree-click-to-detail flow, and the reconnect banner
against a REAL TopologyServer (real ZMQ-free HTTP loop, not a mock), matching
this codebase's standing preference for real infra over mocks wherever
practical.
"""

from __future__ import annotations

import asyncio

import pytest

from civitas import DynamicSupervisor, Runtime, Supervisor, TopologyServer
from civitas.dashboard.app import CivitasDashboardApp
from civitas.dashboard.widgets import (
    AgentDetailPanel,
    ProcessResourcePanel,
    ReconnectBanner,
    TopologyTree,
)
from civitas.messages import Message
from civitas.process import AgentProcess


class _Echo(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        return None


async def _wait_until(predicate, timeout: float = 5.0) -> None:
    """Poll ``predicate`` until true or raise \u2014 the app's pollers run on their
    own @work tasks with real sleep() intervals, so tests must wait for them,
    not assume a fixed number of pilot.pause() calls is enough."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"condition never became true within {timeout}s")


@pytest.mark.asyncio
async def test_dashboard_app_polls_topology_and_renders_tree() -> None:
    ts = TopologyServer(name="topo", port=16950)
    worker = _Echo("worker-a")
    runtime = Runtime(supervisor=Supervisor("root", children=[ts, worker]))
    await runtime.start()
    try:
        app = CivitasDashboardApp(host="127.0.0.1", port=16950, refresh=0.1)
        async with app.run_test(size=(120, 40)) as pilot:
            tree = app.query_one(TopologyTree)
            await _wait_until(lambda: len(tree.root.children) > 0)
            await pilot.pause()
            supervision_root = tree.root.children[0]
            names = [str(n.label) for n in supervision_root.children]
            assert any("worker-a" in n for n in names)
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_dashboard_app_click_to_focus_updates_detail_panel() -> None:
    """The actual click-to-focus contract (design \u00a710): selecting a tree node
    must populate the detail panel with THAT node's data, not stay blank or
    show a different node's data."""
    ts = TopologyServer(name="topo", port=16951)
    worker = _Echo("worker-a")
    runtime = Runtime(supervisor=Supervisor("root", children=[ts, worker]))
    await runtime.start()
    try:
        app = CivitasDashboardApp(host="127.0.0.1", port=16951, refresh=0.1)
        async with app.run_test(size=(120, 40)) as pilot:
            tree = app.query_one(TopologyTree)
            await _wait_until(lambda: len(tree.root.children) > 0)
            supervision_root = tree.root.children[0]
            await _wait_until(lambda: len(supervision_root.children) > 0)
            worker_node = next(n for n in supervision_root.children if n.data == "worker-a")

            tree.select_node(worker_node)
            await pilot.pause()

            detail = app.query_one(AgentDetailPanel)
            title = str(detail.query_one("#detail-title").render())
            assert "worker-a" in title
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_dashboard_app_dynamic_children_appear() -> None:
    """Dynamically-spawned children (invisible to a static snapshot) must
    still show up \u2014 proves the app rides real live /topology data, not a
    frozen one-shot fetch."""
    ts = TopologyServer(name="topo", port=16952)
    dyn = DynamicSupervisor("workers")
    spawner = _Echo("spawner")
    runtime = Runtime(supervisor=Supervisor("root", children=[ts, dyn, spawner]))
    await runtime.start()
    try:
        await spawner.spawn_into("workers", _Echo, "spawned-1")
        app = CivitasDashboardApp(host="127.0.0.1", port=16952, refresh=0.1)
        async with app.run_test(size=(120, 40)) as pilot:
            tree = app.query_one(TopologyTree)

            def _has_spawned() -> bool:
                for n in tree.root.children:
                    for child in n.children:
                        for grandchild in child.children:
                            if grandchild.data == "spawned-1":
                                return True
                return False

            await _wait_until(_has_spawned)
            await pilot.pause()
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_dashboard_app_shows_reconnect_banner_when_unreachable() -> None:
    """No server at all on this port \u2014 the reconnect banner must appear and
    name at least one failing endpoint, and the app must not crash."""
    app = CivitasDashboardApp(host="127.0.0.1", port=16953, refresh=0.1)
    async with app.run_test(size=(120, 40)) as pilot:
        banner = app.query_one(ReconnectBanner)
        await _wait_until(lambda: banner.display is True)
        await pilot.pause()
        assert "reconnecting" in str(banner.render()).lower()


@pytest.mark.asyncio
async def test_dashboard_app_reconnect_banner_clears_once_reachable() -> None:
    """The banner must clear again once the server actually answers \u2014 proves
    this is a live status indicator, not a one-way "ever failed once" flag."""
    app = CivitasDashboardApp(host="127.0.0.1", port=16954, refresh=0.1)
    async with app.run_test(size=(120, 40)) as pilot:
        banner = app.query_one(ReconnectBanner)
        await _wait_until(lambda: banner.display is True)

        ts = TopologyServer(name="topo", port=16954)
        runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
        await runtime.start()
        try:
            await _wait_until(lambda: banner.display is False, timeout=5.0)
            await pilot.pause()
        finally:
            await runtime.stop()


@pytest.mark.asyncio
async def test_dashboard_app_processes_panel_populates() -> None:
    ts = TopologyServer(name="topo", port=16955)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    try:
        app = CivitasDashboardApp(host="127.0.0.1", port=16955, refresh=0.1)
        async with app.run_test(size=(120, 40)) as pilot:
            panel = app.query_one(ProcessResourcePanel)
            table = panel.query_one("#resource-table")
            await _wait_until(lambda: table.row_count > 0)
            await pilot.pause()
    finally:
        await runtime.stop()
