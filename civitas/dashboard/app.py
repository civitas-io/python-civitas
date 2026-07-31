"""``civitas top`` — the Textual dashboard app (v0.9.1, dashboard-v2 Phase E).

Attaches to an already-running ``TopologyServer`` over HTTP and polls three
endpoints independently and continuously: ``/topology``, ``/snapshot``,
``/processes``. Layout is Mockup B's dense three-pane grid (design §7.0):
tree | detail | resources, all three equally first-class, all visible at once.

Each endpoint has its OWN background poll worker (design §7: "each independently
retried on failure with a visible 'reconnecting…' banner instead of the whole app
dying") — a stalled `/processes` probe (e.g. a Worker mid-restart) must never
freeze the topology tree or vice versa.

v0.9.4: multi-cluster/multi-topology view (design/dashboard-v2.md P2). The
per-cluster three-pane-view-plus-poll-workers logic that used to live
directly on ``CivitasDashboardApp`` was extracted into ``ClusterView`` (a
real, independently reusable Widget) so the app can host N of them — one
per topology given on the command line. Confirmed empirically before this
refactor (not assumed): a Widget's own ``query_one()`` scopes correctly to
its own subtree even with sibling instances sharing the same child IDs, and
a Widget's own ``BINDINGS`` dispatch correctly whenever ANY descendant
currently has focus, not just the widget itself — both are what make N
independent ``ClusterView`` instances (each with their own poll workers,
their own "f" focus-toggle, their own reconnect banner) safe to host
side-by-side without cross-cluster interference or ID-suffixing hacks.

Single-topology invocation is unchanged in behavior and DOM shape from the
caller's point of view: exactly one ``ClusterView`` is composed directly,
no ``TabbedContent`` wrapper — existing code/tests that ``query_one()`` for
``TopologyTree``/``AgentDetailPanel``/etc directly on the App still work,
since Textual's ``query_one()`` searches the whole subtree recursively, not
just direct children.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Header, TabbedContent, TabPane, Tree

from civitas.dashboard.client import DashboardConnectionError, fetch_json
from civitas.dashboard.widgets import (
    AgentDetailPanel,
    ProcessResourcePanel,
    ReconnectBanner,
    TopologyTree,
    flatten_topology,
)

logger = logging.getLogger(__name__)

_CSS_PATH = "app.tcss"


@dataclass(frozen=True)
class ClusterTarget:
    """One topology to attach to — a label (shown on its tab in multi-cluster
    mode; irrelevant in single-cluster mode) plus its introspection-endpoint
    address and, v0.9.6, the auth headers to send to it (empty = no auth)."""

    label: str
    host: str
    port: int
    headers: dict[str, str] = field(default_factory=dict)


class ClusterView(Vertical):
    """One cluster's live three-pane view, plus its own independent poll
    workers and its own reconnect banner (v0.9.4) — extracted from
    ``CivitasDashboardApp`` so the app can host N of these side-by-side in
    multi-cluster mode. See module docstring for why this is safe (query
    scoping + binding dispatch both confirmed to work correctly per-instance).
    """

    BINDINGS = [("f", "toggle_focus", "Focus detail")]

    def __init__(
        self, host: str, port: int, refresh: float, headers: dict[str, str] | None = None
    ) -> None:
        super().__init__()
        self._host = host
        self._port = port
        self._refresh = refresh
        # v0.9.6: auth headers sent on every poll, so the dashboard can attach
        # to an endpoint behind the control-plane auth seam. Empty = no auth.
        self._headers = headers or {}

        # Latest snapshot from each endpoint, updated independently by each
        # poll worker below. None means "never successfully fetched yet."
        self._topology: dict[str, Any] | None = None
        self._metrics: dict[str, Any] | None = None
        self._processes: list[dict[str, Any]] | None = None
        self._focused_name: str | None = None
        # v0.9.4: whether focus/expand mode is currently active (design §7.0).
        self._focus_mode = False
        # Which endpoint paths are CURRENTLY failing. A set, not a single
        # flag/banner-owned bool: three independent workers share one banner
        # widget, and a naive "clear on any success" would let a healthy
        # endpoint's poll mask a DIFFERENT endpoint's still-ongoing failure —
        # a real race, not a hypothetical one, given three concurrent workers.
        self._failing: set[str] = set()

    def compose(self) -> ComposeResult:
        yield ReconnectBanner()
        with Horizontal(id="main"):
            yield TopologyTree()
            yield AgentDetailPanel()
            yield ProcessResourcePanel()

    def on_mount(self) -> None:
        self._poll_topology()
        self._poll_metrics()
        self._poll_processes()

    # ------------------------------------------------------------------
    # Interaction
    # ------------------------------------------------------------------

    def on_tree_node_selected(self, event: Tree.NodeSelected[str]) -> None:
        name = event.node.data
        if not name:
            return
        self._focused_name = name
        self._refresh_detail_panel()
        event.stop()  # v0.9.4: don't let a sibling ClusterView's own Tree react too

    def action_toggle_focus(self) -> None:
        """v0.9.4: toggle focus/expand mode (design §7.0) -- widens
        AgentDetailPanel at the expense of TopologyTree/ProcessResourcePanel
        via a CSS class on #main, rather than replacing the three-pane
        layout entirely (Mockup B's "three equally first-class panels,
        always visible" philosophy holds even while focused -- this EXPANDS
        the detail pane, it doesn't hide the other two).

        No-ops with nothing selected yet -- expanding an empty placeholder
        has no real effect worth a keypress.
        """
        if self._focused_name is None:
            return
        self._focus_mode = not self._focus_mode
        self.query_one("#main").set_class(self._focus_mode, "focused")

    def _refresh_detail_panel(self) -> None:
        detail = self.query_one(AgentDetailPanel)
        nodes = flatten_topology(self._topology)
        node = nodes.get(self._focused_name) if self._focused_name else None
        agents = (self._metrics or {}).get("agents", {}) if self._metrics else {}
        agent_metrics = agents.get(self._focused_name) if self._focused_name else None
        detail.update_detail(self._focused_name, node, agent_metrics)

    # ------------------------------------------------------------------
    # Polling workers — one per endpoint, independently retried (design §7)
    # ------------------------------------------------------------------

    # v0.9.1 (Phase E): explicit distinct `group=` on each poller is load-
    # bearing, not decorative. @work's `group` defaults to "default" for
    # EVERY undecorated call — found by a real failing test: with
    # exclusive=True and no group=, all three pollers shared one group, and
    # starting each new one silently CANCELLED the previous one (that's what
    # exclusive=True does within a group) — only the last-called poller
    # (_poll_processes) ever actually ran; /topology and /snapshot never
    # polled at all, with no error, no exception, nothing to see except an
    # empty tree forever. (v0.9.3.1: this endpoint was /metrics at the time
    # -- renamed to /snapshot to make room for real Prometheus exposition at
    # the standard /metrics path; this comment's history is otherwise
    # unchanged. v0.9.4: these workers now run per-ClusterView instance, not
    # per-App — Textual's @work group isolation is per-DOMNode-method, so N
    # ClusterViews' identically-named groups don't collide with each other,
    # confirmed alongside this refactor's other Textual-mechanic checks.)
    @work(exclusive=True, group="topology-poll")
    async def _poll_topology(self) -> None:
        async for data in self._poll_forever("/topology"):
            self._topology = data
            if not self._touch_dom():
                return
            tree = self.query_one(TopologyTree)
            tree.update_topology(data)
            self._refresh_detail_panel()

    @work(exclusive=True, group="metrics-poll")
    async def _poll_metrics(self) -> None:
        async for data in self._poll_forever("/snapshot"):  # v0.9.3.1: was /metrics
            self._metrics = data
            if not self._touch_dom():
                return
            self._refresh_detail_panel()

    @work(exclusive=True, group="processes-poll")
    async def _poll_processes(self) -> None:
        async for data in self._poll_forever("/processes"):
            processes = data.get("processes") if isinstance(data, dict) else None
            self._processes = processes
            if not self._touch_dom():
                return
            self.query_one(ProcessResourcePanel).update_processes(processes)

    def _touch_dom(self) -> bool:
        """``True`` if it's currently safe to query/mutate this widget's DOM.

        Found via a real, reproducible failure (not review): Textual's own
        app-shutdown teardown (``run_test()``'s ``__aexit__`` in tests; the
        equivalent real-quit path in a live terminal) can tear down the
        Screen's widgets while an ``@work(exclusive=True)`` poll loop's `await
        asyncio.sleep(...)` is still pending — exclusive workers are
        cancelled on unmount, but there is a real window where a poll that
        was ALREADY in-flight resumes into a DOM that no longer has the
        widgets it expects, raising ``NoMatches`` and crashing the worker
        (which ``run_test()`` then re-raises). Checking ``self.is_running``
        before every DOM touch closes that window without an artificial
        try/except NoMatches at every call site. (v0.9.4: ``Widget.is_running``
        delegates to the owning App's own ``is_running`` — correct here too,
        not just for App subclasses.)
        """
        return self.is_running

    def _mark_ok(self, path: str) -> None:
        """Endpoint ``path`` just answered successfully.

        Only clears/hides this cluster's OWN banner when NO endpoint of ITS
        OWN is failing — see ``self._failing``'s docstring for why a naive
        per-worker clear() is a real race with three concurrent poll workers.
        """
        self._failing.discard(path)
        if not self._touch_dom():
            return
        banner = self.query_one(ReconnectBanner)
        if self._failing:
            banner.show_for(", ".join(sorted(self._failing)))
        else:
            banner.clear()

    def _mark_failed(self, path: str) -> None:
        """Endpoint ``path`` just failed — add it to the failing set and show
        (or update) this cluster's OWN banner naming every currently-failing
        endpoint (never a different cluster's — each ClusterView owns a
        distinct ReconnectBanner instance)."""
        self._failing.add(path)
        if not self._touch_dom():
            return
        self.query_one(ReconnectBanner).show_for(", ".join(sorted(self._failing)))

    async def _poll_forever(self, path: str) -> AsyncIterator[Any]:
        """Poll ``path`` forever at ``self._refresh`` interval.

        Yields the parsed JSON body on every successful fetch. On failure,
        marks this endpoint failing in this cluster's own banner and keeps
        retrying — never raises, never stops the loop; this is what makes
        each endpoint's failure independently visible from the others
        (design §7), without one endpoint's recovery silently masking a
        different endpoint's outage.
        """
        import asyncio

        while True:
            try:
                _status, data = await fetch_json(
                    self._host, self._port, path, headers=self._headers
                )
                self._mark_ok(path)
                yield data
            except DashboardConnectionError:
                logger.debug("poll failed for %s", path, exc_info=True)
                self._mark_failed(path)
            await asyncio.sleep(self._refresh)


class CivitasDashboardApp(App[None]):
    """``civitas top`` — live, mouse-clickable dashboard for one or more
    running topologies (v0.9.4: multi-cluster support)."""

    CSS_PATH = _CSS_PATH
    TITLE = "civitas top"
    BINDINGS = [("q", "quit", "Quit")]

    def __init__(
        self,
        clusters: list[ClusterTarget] | None = None,
        refresh: float = 1.0,
        *,
        host: str = "127.0.0.1",
        port: int = 6789,
        headers: dict[str, str] | None = None,
    ) -> None:
        """``clusters`` is the v0.9.4 multi-topology API. ``host``/``port``/
        ``headers`` are kept as a backward-compatible single-cluster convenience
        (the v0.9.1-v0.9.3 constructor shape) — used only when ``clusters`` is
        not given, so no existing direct-construction call site breaks.
        ``headers`` (v0.9.6) auth the single-cluster case; multi-cluster carries
        per-cluster headers on each ClusterTarget.
        """
        super().__init__()
        self._clusters = clusters or [
            ClusterTarget(label="default", host=host, port=port, headers=headers or {})
        ]
        self._refresh = refresh

    def compose(self) -> ComposeResult:
        yield Header()
        if len(self._clusters) == 1:
            # v0.9.1-v0.9.3 shape, unchanged: no tab bar at all for a single
            # topology — the common case shouldn't carry multi-cluster UI
            # chrome it has no use for.
            cluster = self._clusters[0]
            yield ClusterView(cluster.host, cluster.port, self._refresh, cluster.headers)
        else:
            with TabbedContent():
                for cluster in self._clusters:
                    with TabPane(cluster.label, id=f"cluster-{cluster.label}"):
                        yield ClusterView(
                            cluster.host, cluster.port, self._refresh, cluster.headers
                        )
        yield Footer()

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        """v0.9.4: move keyboard focus onto the newly-active tab's own
        TopologyTree -- found live (not assumed) that TabbedContent's own
        internal tab-selector bar (ContentTabs) grabs default focus instead
        of any tab's actual content, which would silently make ClusterView's
        "f" binding (bound via ancestor-of-focused-widget dispatch) never
        fire at all in multi-cluster mode -- confirmed by inspecting
        app.focused directly during a real headless run, not by review.
        Fires for the initially-active tab too, not just user-driven
        switches, so this closes the gap in both cases with one handler.
        """
        try:
            event.pane.query_one(TopologyTree).focus()
        except Exception:
            logger.debug("could not focus newly-active tab's tree", exc_info=True)


__all__ = ["CivitasDashboardApp", "ClusterTarget", "ClusterView"]
