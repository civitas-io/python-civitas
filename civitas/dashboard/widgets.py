"""Widgets for the Textual dashboard (v0.9.1, dashboard-v2 Phase E).

Mockup B's dense three-pane grid (design §7.0): ``TopologyTree`` | ``AgentDetailPanel``
| ``ProcessResourcePanel``, roughly equal thirds, all three first-class simultaneously
(not one "main" view with the others secondary). Each widget's ``update_*`` method is a
plain, synchronous, HTTP-free method taking already-fetched data — the app's polling
workers own all I/O (client.py), widgets only render; this keeps every widget trivially
testable with plain dicts, no server needed (design §10: "few, meaningful" tests).
"""

from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.widgets import DataTable, Static, Tree
from textual.widgets.tree import TreeNode

from civitas.dashboard.palette import (
    LLM_ACCENT,
    RESOURCE_ACCENT,
    TOPOLOGY_ACCENT,
    format_bytes,
    format_cost,
    format_uptime,
    gauge_bar,
    status_color,
    status_dot,
)


def flatten_topology(node: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Flatten the recursive ``/topology`` tree into ``{name: node_dict}``.

    ``AgentDetailPanel`` joins the focused name against this + the ``/snapshot``
    snapshot client-side (design §7's explicit "the JOIN happens in the widget" —
    each endpoint stays independently simple).
    """
    out: dict[str, dict[str, Any]] = {}
    if node is None:
        return out

    def _walk(n: dict[str, Any]) -> None:
        name = n.get("name")
        if isinstance(name, str):
            out[name] = n
        for child in n.get("children", []) or []:
            _walk(child)

    _walk(node)
    return out


class TopologyTree(Tree[str]):
    """Left pane — mouse-clickable supervision tree (design §7's `TopologyTree`).

    Each node's ``data`` carries the agent/supervisor name so a click can report
    exactly which entity was selected without re-parsing labels.

    NOTE: the private recursive helper below is named ``_add_topology_node``,
    not ``_add_node`` — caught by a failing test, not by review: Textual's own
    ``Tree`` base class already has a private ``_add_node`` method it calls
    from ``__init__``, and a same-named override here silently shadowed it
    with the wrong signature, crashing the moment ANY ``TopologyTree`` was
    constructed. Same class of bug as v0.9.0's D-E4-6 (a private-looking name
    accidentally colliding with a framework/base-class internal).
    """

    def __init__(self) -> None:
        super().__init__("civitas top", data="")

    def update_topology(self, data: dict[str, Any] | None) -> None:
        """Rebuild the tree from a fresh ``/topology`` response.

        Rebuilds from scratch each poll rather than diffing — topology snapshots
        are small (this is a management/observability tree, not a data grid) and
        a full rebuild can never drift from the server's actual current shape.
        """
        self.root.remove_children()
        if data is None or "error" in data:
            self.root.set_label("civitas top [dim](disconnected)[/]")
            return
        self.root.set_label("civitas top")
        self._add_topology_node(self.root, data)
        self.root.expand_all()

    def _add_topology_node(self, parent: TreeNode[str], node: dict[str, Any]) -> None:
        name = node.get("name", "?")
        node_type = node.get("type", "agent")
        children = node.get("children") or []

        if node_type == "agent":
            status = node.get("status", "unknown")
            restarts = node.get("restart_count", 0)
            label = f"[{status_color(status)}]{status_dot(status)}[/] {name}"
            if restarts:
                label += f"  [yellow]restarts:{restarts}[/]"
            parent.add_leaf(label, data=name)
            return

        if node_type == "dynamic_supervisor":
            live = node.get("live_count", 0)
            label = f"[b {TOPOLOGY_ACCENT}]{name}[/] [dim](dynamic, {live} live)[/]"
        else:  # supervisor
            strategy = node.get("strategy", "")
            crashes = node.get("crashes_in_window", 0)
            label = f"[b {TOPOLOGY_ACCENT}]{name}[/] [dim]{strategy}[/]"
            if crashes:
                label += f"  [yellow]crashes:{crashes}[/]"

        child_node = parent.add(label, data=name, expand=True)
        for child in children:
            self._add_topology_node(child_node, child)


class AgentDetailPanel(Static):
    """Middle pane — status, capabilities, restarts, and LLM/cost metrics for the
    currently-focused tree node (design §7's `AgentDetailPanel`).
    """

    def compose(self) -> ComposeResult:
        yield Static("[dim]Select an agent or supervisor[/]", id="detail-title")
        yield DataTable(id="detail-table", show_header=False)

    def on_mount(self) -> None:
        table = self.query_one("#detail-table", DataTable)
        table.add_columns("field", "value")

    def update_detail(
        self,
        name: str | None,
        topology_node: dict[str, Any] | None,
        agent_metrics: dict[str, Any] | None,
    ) -> None:
        title = self.query_one("#detail-title", Static)
        table = self.query_one("#detail-table", DataTable)
        table.clear()

        if name is None or topology_node is None:
            title.update("[dim]Select an agent or supervisor[/]")
            return

        node_type = topology_node.get("type", "agent")
        status = topology_node.get("status") or (
            "supervisor" if node_type != "agent" else "unknown"
        )
        title.update(
            f"[b {LLM_ACCENT}]{name}[/]  [{status_color(status)}]{status_dot(status)} {status.upper()}[/]"
        )

        rows: list[tuple[str, str]] = [
            ("type", node_type),
            ("process_id", str(topology_node.get("process_id", "-"))),
        ]
        if "uptime_seconds" in topology_node:
            rows.append(("uptime", format_uptime(topology_node["uptime_seconds"])))
        # v0.9.4 (design/dashboard-v2.md P1): incarnation-scoped session
        # signal -- only shown once the agent has actually made an LLM call
        # this incarnation (session_turn_count > 0), matching this whole
        # panel's existing "no spurious zero entry" discipline (e.g.
        # capabilities only shown when non-empty, above).
        turn_count = topology_node.get("session_turn_count", 0)
        if turn_count:
            duration = format_uptime(topology_node.get("session_duration_seconds", 0.0))
            rows.append(
                ("session", f"{turn_count} turn{'s' if turn_count != 1 else ''}, {duration}")
            )
        if "restart_count" in topology_node:
            rows.append(("restart_count", str(topology_node["restart_count"])))
        if "crashes_in_window" in topology_node:
            rows.append(("crashes_in_window", str(topology_node["crashes_in_window"])))
        if "live_count" in topology_node:
            rows.append(("live_count", str(topology_node["live_count"])))
        caps = topology_node.get("capabilities")
        if caps:
            rows.append(("capabilities", ", ".join(caps)))

        if agent_metrics is not None:
            rows.extend(
                [
                    ("messages_handled", str(agent_metrics.get("messages_handled", 0))),
                    ("messages_sent", str(agent_metrics.get("messages_sent", 0))),
                    ("avg_latency_ms", f"{agent_metrics.get('avg_latency_ms', 0.0):.1f}ms"),
                    ("errors", str(agent_metrics.get("errors", 0))),
                    (
                        "tokens in/out",
                        f"{agent_metrics.get('tokens_in', 0):,}/{agent_metrics.get('tokens_out', 0):,}",
                    ),
                    ("cost_usd", format_cost(agent_metrics.get("cost_usd", 0.0))),
                    ("last_model", agent_metrics.get("last_model") or "-"),
                ]
            )

        for field, value in rows:
            table.add_row(field, value)


class ProcessResourcePanel(Static):
    """Right pane — one row per OS process with a proportional colored gauge bar
    for CPU% and RSS (design §7's `ProcessResourcePanel`: a snapshot, not a
    history chart — multi-sample time series stay P1/v0.9.2 per the PRD).
    """

    def compose(self) -> ComposeResult:
        yield Static(f"[b {RESOURCE_ACCENT}]Processes[/]", id="resource-title")
        yield DataTable(id="resource-table")

    def on_mount(self) -> None:
        table = self.query_one("#resource-table", DataTable)
        # 3 columns, matching the ratified Mockup B exactly (design §7.0) --
        # a 4th "uptime" column was tried during the Phase E smoke run and
        # overflowed a 1/3-width pane at realistic terminal sizes, truncating
        # the mem column; uptime is already visible per-agent in the detail
        # panel, so it isn't lost information, just not duplicated here.
        table.add_columns("process", "cpu", "mem")

    def update_processes(self, processes: list[dict[str, Any]] | None) -> None:
        table = self.query_one("#resource-table", DataTable)
        table.clear()
        if not processes:
            return
        for proc in processes:
            table.add_row(
                f"{proc.get('kind', '?')}:{proc.get('id', '?')}",
                gauge_bar(proc.get("cpu_percent", 0.0)),
                format_bytes(proc.get("rss_bytes", 0)),
            )


class ReconnectBanner(Static):
    """A visible "reconnecting…" banner (design §7: independently retried per
    endpoint, "instead of the whole app dying"). Hidden by default via CSS
    (``display: none``); shown/hidden by the app as poll failures accumulate.
    """

    def __init__(self) -> None:
        super().__init__("", id="reconnect-banner")
        # Set explicitly, not left to app.tcss's `display: none` alone —
        # caught by a failing widget-only test (no App CSS loaded there): a
        # widget's own default state should not depend entirely on external
        # CSS being present to be correct. app.tcss's rule is now redundant
        # defense-in-depth, not the only mechanism.
        self.display = False

    def show_for(self, endpoint: str) -> None:
        # Static.update() DOES resolve "$tokens" (via Textual's Content
        # renderer, unlike Tree/DataTable content -- see palette.py's
        # STATUS_COLORS docstring), so this one is fine as a theme token.
        self.update(f"[b $error]⚠ reconnecting to {endpoint}…[/]")
        self.display = True

    def clear(self) -> None:
        self.update("")
        self.display = False


__all__ = [
    "AgentDetailPanel",
    "ProcessResourcePanel",
    "ReconnectBanner",
    "TopologyTree",
    "flatten_topology",
]
