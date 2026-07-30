"""civitas/dashboard/widgets.py \u2014 rendering-only widgets (v0.9.1, dashboard-v2
Phase E). Each ``update_*`` method is HTTP-free and takes plain dicts (the
app's poll workers own all I/O) \u2014 tested here with plain sample data, no
server needed, matching design \u00a710's "few, meaningful" scope for widget-level
tests (the end-to-end app/click/reconnect behavior lives in
tests/integration/test_dashboard_app.py).
"""

from __future__ import annotations

import pytest
from textual.app import App, ComposeResult
from textual.widgets import DataTable, Static

from civitas.dashboard.widgets import (
    AgentDetailPanel,
    ProcessResourcePanel,
    ReconnectBanner,
    TopologyTree,
    flatten_topology,
)

# ---------------------------------------------------------------------------
# flatten_topology \u2014 pure function, no Textual needed
# ---------------------------------------------------------------------------


def test_flatten_topology_none() -> None:
    assert flatten_topology(None) == {}


def test_flatten_topology_nested() -> None:
    tree = {
        "name": "root",
        "type": "supervisor",
        "children": [
            {"name": "worker-a", "type": "agent", "status": "running"},
            {
                "name": "dyn",
                "type": "dynamic_supervisor",
                "children": [{"name": "spawned-1", "type": "agent", "status": "suspended"}],
            },
        ],
    }
    flat = flatten_topology(tree)
    assert set(flat) == {"root", "worker-a", "dyn", "spawned-1"}
    assert flat["spawned-1"]["status"] == "suspended"


# ---------------------------------------------------------------------------
# Widget rendering \u2014 mounted inside a minimal throwaway App (Textual widgets
# need a running App/screen to safely query_one() their composed children).
# ---------------------------------------------------------------------------


class _SingleWidgetApp(App[None]):
    def __init__(self, widget: object) -> None:
        super().__init__()
        self._widget = widget

    def compose(self) -> ComposeResult:
        yield self._widget  # type: ignore[misc]


@pytest.mark.asyncio
async def test_topology_tree_renders_agents_and_supervisors() -> None:
    tree = TopologyTree()
    app = _SingleWidgetApp(tree)
    async with app.run_test():
        tree.update_topology(
            {
                "name": "root",
                "type": "supervisor",
                "strategy": "one_for_one",
                "crashes_in_window": 0,
                "children": [
                    {"name": "worker-a", "type": "agent", "status": "running", "restart_count": 0},
                    {"name": "worker-b", "type": "agent", "status": "crashed", "restart_count": 3},
                ],
            }
        )
        # Tree.root is the "civitas top" title node (never the supervision
        # root itself, matching the ratified mockup script) -- its one child
        # IS the supervision root, and THAT node's children are the workers.
        assert len(tree.root.children) == 1
        supervision_root = tree.root.children[0]
        labels = [str(n.label) for n in supervision_root.children]
        assert len(labels) == 2
        assert any("worker-a" in label for label in labels)
        assert any("restarts:3" in label for label in labels)


@pytest.mark.asyncio
async def test_topology_tree_handles_error_response() -> None:
    """A /topology error body (runtime not available) must not crash the
    tree \u2014 it should render a disconnected-looking root with zero children."""
    tree = TopologyTree()
    app = _SingleWidgetApp(tree)
    async with app.run_test():
        tree.update_topology({"error": "runtime not available"})
        assert len(tree.root.children) == 0


@pytest.mark.asyncio
async def test_agent_detail_panel_shows_placeholder_when_unselected() -> None:
    panel = AgentDetailPanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_detail(None, None, None)
        title = panel.query_one("#detail-title")
        assert "Select" in str(title.render())


@pytest.mark.asyncio
async def test_agent_detail_panel_joins_topology_and_metrics() -> None:
    """The join happens IN the widget (design \u00a77) \u2014 topology fields and
    metrics fields must both land in the same table from two separate dicts."""
    panel = AgentDetailPanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_detail(
            "worker-a",
            {
                "name": "worker-a",
                "type": "agent",
                "status": "running",
                "process_id": "topo",
                "restart_count": 1,
                "uptime_seconds": 90.0,
                "capabilities": ["chat"],
            },
            {
                "messages_handled": 12,
                "messages_sent": 5,
                "avg_latency_ms": 3.2,
                "errors": 0,
                "tokens_in": 100,
                "tokens_out": 50,
                "cost_usd": 0.02,
                "last_model": "claude-sonnet",
            },
        )
        table = panel.query_one("#detail-table", DataTable)
        rows = [table.get_row_at(i) for i in range(table.row_count)]
        fields = {row[0]: row[1] for row in rows}
        assert fields["restart_count"] == "1"
        assert fields["uptime"] == "1m 30s"
        assert fields["capabilities"] == "chat"
        assert fields["last_model"] == "claude-sonnet"
        assert "0.02" in fields["cost_usd"] or fields["cost_usd"] == "$0.02"


@pytest.mark.asyncio
async def test_agent_detail_panel_shows_session_when_agent_has_made_llm_calls() -> None:
    """v0.9.4 (design/dashboard-v2.md P1): session_turn_count/
    session_duration_seconds, when present and non-zero, render as a
    'session' row alongside uptime."""
    panel = AgentDetailPanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_detail(
            "chatty",
            {
                "name": "chatty",
                "type": "agent",
                "status": "running",
                "process_id": "topo",
                "uptime_seconds": 300.0,
                "session_turn_count": 3,
                "session_duration_seconds": 90.0,
            },
            None,
        )
        table = panel.query_one("#detail-table", DataTable)
        rows = [table.get_row_at(i) for i in range(table.row_count)]
        fields = {row[0]: row[1] for row in rows}
        assert fields["session"] == "3 turns, 1m 30s"


@pytest.mark.asyncio
async def test_agent_detail_panel_hides_session_row_with_zero_turns() -> None:
    """v0.9.4: no spurious 'session' row for an agent that has never made an
    LLM call this incarnation -- matches this panel's existing "no spurious
    zero entry" discipline (e.g. capabilities only shown when non-empty)."""
    panel = AgentDetailPanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_detail(
            "flaky",
            {
                "name": "flaky",
                "type": "agent",
                "status": "running",
                "process_id": "topo",
                "uptime_seconds": 300.0,
                "session_turn_count": 0,
                "session_duration_seconds": 0.0,
            },
            None,
        )
        table = panel.query_one("#detail-table", DataTable)
        rows = [table.get_row_at(i) for i in range(table.row_count)]
        fields = {row[0]: row[1] for row in rows}
        assert "session" not in fields


@pytest.mark.asyncio
async def test_agent_detail_panel_hides_session_row_when_field_absent() -> None:
    """Backward compatibility: a topology_node dict without session fields at
    all (e.g. from a not-yet-upgraded remote process) must not crash or show
    a bogus session row."""
    panel = AgentDetailPanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_detail(
            "worker-a",
            {"name": "worker-a", "type": "agent", "status": "running", "process_id": "topo"},
            None,
        )
        table = panel.query_one("#detail-table", DataTable)
        rows = [table.get_row_at(i) for i in range(table.row_count)]
        fields = {row[0]: row[1] for row in rows}
        assert "session" not in fields


@pytest.mark.asyncio
async def test_agent_detail_panel_shows_hitl_approval_row_and_distinct_color() -> None:
    """v0.9.4 (design/dashboard-v2.md §6/§18): a HITL-suspended agent's title
    renders the distinct blue signal (not the shared SUSPENDED grey a
    governance pause would use), and the table gains a readable
    'suspended_because' row."""
    panel = AgentDetailPanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_detail(
            "approval-worker",
            {
                "name": "approval-worker",
                "type": "agent",
                "status": "SUSPENDED",
                "process_id": "topo",
                "suspend_category": "hitl_approval",
            },
            None,
        )
        title = panel.query_one("#detail-title", Static)
        assert "blue" in str(title.content)
        table = panel.query_one("#detail-table", DataTable)
        rows = [table.get_row_at(i) for i in range(table.row_count)]
        fields = {row[0]: row[1] for row in rows}
        assert fields["suspended_because"] == "awaiting approval (HITL)"


@pytest.mark.asyncio
async def test_agent_detail_panel_governance_pause_uses_shared_grey_not_hitl_blue() -> None:
    """A governance-pause (or category-less/'other') SUSPENDED agent keeps
    the original shared grey -- the distinct blue is HITL-only."""
    panel = AgentDetailPanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_detail(
            "paused-worker",
            {
                "name": "paused-worker",
                "type": "agent",
                "status": "SUSPENDED",
                "process_id": "topo",
                "suspend_category": "governance_pause",
            },
            None,
        )
        title = panel.query_one("#detail-title", Static)
        rendered = str(title.content)
        assert "blue" not in rendered
        assert "grey58" in rendered
        table = panel.query_one("#detail-table", DataTable)
        rows = [table.get_row_at(i) for i in range(table.row_count)]
        fields = {row[0]: row[1] for row in rows}
        assert fields["suspended_because"] == "governance pause"


@pytest.mark.asyncio
async def test_topology_tree_renders_hitl_agent_with_distinct_color() -> None:
    """The tree's own leaf label (not just the detail panel's title) also
    carries the distinct HITL color -- both render surfaces stay consistent."""
    tree = TopologyTree()
    app = _SingleWidgetApp(tree)
    async with app.run_test():
        tree.update_topology(
            {
                "name": "root",
                "type": "supervisor",
                "children": [
                    {
                        "name": "approval-worker",
                        "type": "agent",
                        "status": "SUSPENDED",
                        "suspend_category": "hitl_approval",
                    }
                ],
            }
        )
        # update_topology's tree shape (see _add_topology_node): a non-"agent"
        # top-level node (here, "supervisor") is itself added as a NEW child
        # under self.root -- so tree.root.children[0] is MY "root" supervisor
        # node, and the agent LEAF is one level deeper still.
        supervisor_node = tree.root.children[0]
        leaf = supervisor_node.children[0]
        # The tree's label is a parsed Rich Text (markup already resolved into
        # style Spans by add_leaf()) -- "blue" lives in a Span's style, not in
        # str(label) itself, unlike the detail panel's raw-markup-string Static.
        assert any("blue" in str(span.style) for span in leaf.label.spans)


@pytest.mark.asyncio
async def test_process_resource_panel_renders_gauge_rows() -> None:
    panel = ProcessResourcePanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_processes(
            [
                {
                    "kind": "runtime",
                    "id": "topo",
                    "pid": 1,
                    "cpu_percent": 12.0,
                    "rss_bytes": 1024,
                    "uptime_seconds": 5.0,
                },
                {
                    "kind": "worker",
                    "id": "chan-1",
                    "pid": 2,
                    "cpu_percent": 90.0,
                    "rss_bytes": 2048,
                    "uptime_seconds": 5.0,
                },
            ]
        )
        table = panel.query_one("#resource-table", DataTable)
        assert table.row_count == 2


@pytest.mark.asyncio
async def test_process_resource_panel_handles_none() -> None:
    panel = ProcessResourcePanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_processes(None)
        table = panel.query_one("#resource-table", DataTable)
        assert table.row_count == 0


@pytest.mark.asyncio
async def test_reconnect_banner_show_and_clear() -> None:
    banner = ReconnectBanner()
    app = _SingleWidgetApp(banner)
    async with app.run_test():
        assert banner.display is False
        banner.show_for("/topology")
        assert banner.display is True
        banner.clear()
        assert banner.display is False
