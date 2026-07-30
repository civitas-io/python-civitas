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
from civitas.dashboard.app import CivitasDashboardApp, ClusterTarget, ClusterView
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


@pytest.mark.asyncio
async def test_dashboard_app_focus_mode_widens_detail_pane_and_toggles_off() -> None:
    """v0.9.4: design §7.0's deferred Mockup A idea -- the "f" key widens
    AgentDetailPanel at the expense of TopologyTree/ProcessResourcePanel,
    which stay VISIBLE (not hidden) -- Mockup B's three-equally-first-class-
    panels philosophy holds even while focused. Verified via real measured
    widget widths (not just the CSS class flag), and that it toggles back.
    """
    ts = TopologyServer(name="topo", port=16956)
    worker = _Echo("worker-a")
    runtime = Runtime(supervisor=Supervisor("root", children=[ts, worker]))
    await runtime.start()
    try:
        app = CivitasDashboardApp(host="127.0.0.1", port=16956, refresh=0.1)
        async with app.run_test(size=(120, 40)) as pilot:
            tree = app.query_one(TopologyTree)
            await _wait_until(lambda: len(tree.root.children) > 0)
            supervision_root = tree.root.children[0]
            await _wait_until(lambda: len(supervision_root.children) > 0)
            worker_node = next(n for n in supervision_root.children if n.data == "worker-a")
            tree.select_node(worker_node)
            await pilot.pause()

            detail_before = app.query_one(AgentDetailPanel).size.width
            tree_before = app.query_one(TopologyTree).size.width
            resource_before = app.query_one(ProcessResourcePanel).size.width

            await pilot.press("f")
            await pilot.pause()

            detail_after = app.query_one(AgentDetailPanel).size.width
            tree_after = app.query_one(TopologyTree).size.width
            resource_after = app.query_one(ProcessResourcePanel).size.width
            assert detail_after > detail_before
            assert tree_after < tree_before
            assert resource_after < resource_before
            assert tree_after > 0  # still visible, not hidden
            assert resource_after > 0  # still visible, not hidden

            await pilot.press("f")
            await pilot.pause()
            assert app.query_one(AgentDetailPanel).size.width == detail_before
            assert app.query_one(TopologyTree).size.width == tree_before
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_dashboard_app_focus_mode_is_a_noop_with_nothing_selected() -> None:
    """Expanding an empty placeholder has no real effect worth a keypress --
    the toggle only engages once a node has actually been selected."""
    ts = TopologyServer(name="topo", port=16957)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    try:
        app = CivitasDashboardApp(host="127.0.0.1", port=16957, refresh=0.1)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            assert app.query_one(ClusterView)._focused_name is None

            await pilot.press("f")
            await pilot.pause()
            assert app.query_one("#main").has_class("focused") is False
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_dashboard_app_single_cluster_has_no_tab_bar() -> None:
    """v0.9.4: the single-topology case must look EXACTLY as it did before
    multi-cluster support existed -- no TabbedContent chrome for the common
    case that has no use for it."""
    from textual.widgets import TabbedContent

    ts = TopologyServer(name="topo", port=16958)
    runtime = Runtime(supervisor=Supervisor("root", children=[ts]))
    await runtime.start()
    try:
        app = CivitasDashboardApp(host="127.0.0.1", port=16958, refresh=0.1)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            assert len(list(app.query(TabbedContent))) == 0
            assert len(list(app.query(ClusterView))) == 1
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_dashboard_app_multi_cluster_shows_a_tab_per_topology() -> None:
    """v0.9.4: two real, independently-running topologies, each with its
    own live TopologyServer -- confirms the tab bar appears with the right
    labels and each tab hosts its own independently-polling ClusterView.
    """
    from textual.widgets import TabbedContent

    ts_a = TopologyServer(name="topo-a", port=16959)
    worker_a = _Echo("worker-a")
    runtime_a = Runtime(supervisor=Supervisor("root", children=[ts_a, worker_a]))
    ts_b = TopologyServer(name="topo-b", port=16960)
    worker_b = _Echo("worker-b")
    runtime_b = Runtime(supervisor=Supervisor("root", children=[ts_b, worker_b]))
    await runtime_a.start()
    await runtime_b.start()
    try:
        clusters = [
            ClusterTarget(label="cluster-a", host="127.0.0.1", port=16959),
            ClusterTarget(label="cluster-b", host="127.0.0.1", port=16960),
        ]
        app = CivitasDashboardApp(clusters=clusters, refresh=0.1)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            tabs = app.query_one(TabbedContent)
            tab_ids = {tab.id for tab in tabs.query("TabPane")}
            assert tab_ids == {"cluster-cluster-a", "cluster-cluster-b"}

            views = list(app.query(ClusterView))
            assert len(views) == 2

            def _both_polled() -> bool:
                return all(v._topology is not None for v in views)

            await _wait_until(_both_polled)

            # Each ClusterView polled its OWN topology's own agent, not the
            # other cluster's -- real cross-cluster isolation, not assumed.
            # (Each tree also includes its own TopologyServer node itself,
            # e.g. "topo-a" -- membership check, not an exact set, matching
            # the existing single-cluster test's own established pattern.)
            trees = list(app.query(TopologyTree))
            assert len(trees) == 2
            flat_a = {n.data for n in trees[0].root.children[0].children}
            flat_b = {n.data for n in trees[1].root.children[0].children}
            assert "worker-a" in flat_a
            assert "worker-b" not in flat_a
            assert "worker-b" in flat_b
            assert "worker-a" not in flat_b
    finally:
        await runtime_a.stop()
        await runtime_b.stop()


@pytest.mark.asyncio
async def test_dashboard_app_multi_cluster_focus_toggle_is_per_cluster() -> None:
    """v0.9.4: pressing 'f' in one cluster's tab must not affect a sibling
    cluster's own focus/expand state -- real per-instance isolation."""
    ts_a = TopologyServer(name="topo-a", port=16961)
    worker_a = _Echo("worker-a")
    runtime_a = Runtime(supervisor=Supervisor("root", children=[ts_a, worker_a]))
    ts_b = TopologyServer(name="topo-b", port=16962)
    runtime_b = Runtime(supervisor=Supervisor("root", children=[ts_b]))
    await runtime_a.start()
    await runtime_b.start()
    try:
        clusters = [
            ClusterTarget(label="a", host="127.0.0.1", port=16961),
            ClusterTarget(label="b", host="127.0.0.1", port=16962),
        ]
        app = CivitasDashboardApp(clusters=clusters, refresh=0.1)
        async with app.run_test(size=(120, 40)) as pilot:
            views = list(app.query(ClusterView))
            tree_a = views[0].query_one(TopologyTree)
            await _wait_until(lambda: len(tree_a.root.children) > 0)
            await _wait_until(lambda: len(tree_a.root.children[0].children) > 0)
            worker_node = next(n for n in tree_a.root.children[0].children if n.data == "worker-a")
            tree_a.select_node(worker_node)
            await pilot.pause()

            await pilot.press("f")
            await pilot.pause()
            assert views[0].query_one("#main").has_class("focused") is True
            assert views[1].query_one("#main").has_class("focused") is False
    finally:
        await runtime_a.stop()
        await runtime_b.stop()
