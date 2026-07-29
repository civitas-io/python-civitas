"""``civitas top`` — the Textual dashboard app (v0.9.1, dashboard-v2 Phase E).

Attaches to an already-running ``TopologyServer`` over HTTP and polls three
endpoints independently and continuously: ``/topology``, ``/snapshot``,
``/processes``. Layout is Mockup B's dense three-pane grid (design §7.0):
tree | detail | resources, all three equally first-class, all visible at once.

Each endpoint has its OWN background poll worker (design §7: "each independently
retried on failure with a visible 'reconnecting…' banner instead of the whole app
dying") — a stalled `/processes` probe (e.g. a Worker mid-restart) must never
freeze the topology tree or vice versa.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Tree

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


class CivitasDashboardApp(App[None]):
    """``civitas top`` — live, mouse-clickable dashboard for a running topology."""

    CSS_PATH = _CSS_PATH
    TITLE = "civitas top"
    BINDINGS = [("q", "quit", "Quit")]

    def __init__(self, host: str = "127.0.0.1", port: int = 6789, refresh: float = 1.0) -> None:
        super().__init__()
        self._host = host
        self._port = port
        self._refresh = refresh

        # Latest snapshot from each endpoint, updated independently by each
        # poll worker below. None means "never successfully fetched yet."
        self._topology: dict[str, Any] | None = None
        self._metrics: dict[str, Any] | None = None
        self._processes: list[dict[str, Any]] | None = None
        self._focused_name: str | None = None
        # Which endpoint paths are CURRENTLY failing. A set, not a single
        # flag/banner-owned bool: three independent workers share one banner
        # widget, and a naive "clear on any success" would let a healthy
        # endpoint's poll mask a DIFFERENT endpoint's still-ongoing failure —
        # a real race, not a hypothetical one, given three concurrent workers.
        self._failing: set[str] = set()

    def compose(self) -> ComposeResult:
        yield Header()
        yield ReconnectBanner()
        with Horizontal(id="main"):
            yield TopologyTree()
            yield AgentDetailPanel()
            yield ProcessResourcePanel()
        yield Footer()

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
    # unchanged.)
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
        """``True`` if it's currently safe to query/mutate this app's DOM.

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
        try/except NoMatches at every call site.
        """
        return self.is_running

    def _mark_ok(self, path: str) -> None:
        """Endpoint ``path`` just answered successfully.

        Only clears/hides the shared banner when NO endpoint is failing — see
        ``self._failing``'s docstring for why a naive per-worker clear() is a
        real race with three concurrent poll workers.
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
        (or update) the shared banner naming every currently-failing endpoint."""
        self._failing.add(path)
        if not self._touch_dom():
            return
        self.query_one(ReconnectBanner).show_for(", ".join(sorted(self._failing)))

    async def _poll_forever(self, path: str) -> AsyncIterator[Any]:
        """Poll ``path`` forever at ``self._refresh`` interval.

        Yields the parsed JSON body on every successful fetch. On failure,
        marks this endpoint failing in the shared banner and keeps retrying —
        never raises, never stops the loop; this is what makes each endpoint's
        failure independently visible from the others (design §7), without one
        endpoint's recovery silently masking a different endpoint's outage.
        """
        import asyncio

        while True:
            try:
                _status, data = await fetch_json(self._host, self._port, path)
                self._mark_ok(path)
                yield data
            except DashboardConnectionError:
                logger.debug("poll failed for %s", path, exc_info=True)
                self._mark_failed(path)
            await asyncio.sleep(self._refresh)


__all__ = ["CivitasDashboardApp"]
