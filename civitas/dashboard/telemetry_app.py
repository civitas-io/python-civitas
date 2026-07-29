"""``civitas telemetry`` — the Textual TUI for B1/B2's native SQLite
telemetry store (v0.9.3.5, Track B, B3).

Deliberately a SEPARATE app from ``civitas top`` (civitas/dashboard/app.py),
not a new tab there: ``civitas top`` attaches to an already-running
``TopologyServer`` over HTTP and requires a live process; telemetry data is
historical and lives in a local SQLite directory readable even when nothing
is currently running at all (e.g. checking last week's cost after the app
has stopped) -- a genuinely different attach model.

Reuses ``civitas top``'s visual language (civitas/dashboard/palette.py) and
its periodic-poll-worker pattern (adapted here to re-query the local SQLite
store instead of polling HTTP endpoints) rather than inventing new
conventions for a second dashboard.
"""

from __future__ import annotations

import time

from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Header

from civitas.dashboard.telemetry_time import TimeRange
from civitas.dashboard.telemetry_widgets import (
    CostBreakdownTable,
    CostChart,
    MessageRateChart,
    StatPanel,
    TimeRangeBar,
)
from civitas.observability.sqlite_query import SQLiteQueryEngine

_CSS_PATH = "telemetry_app.tcss"


class CivitasTelemetryApp(App[None]):
    """``civitas telemetry`` — cost/rate charts and breakdowns over a local
    SQLite telemetry store. Refreshes periodically (a sliding time window,
    unless a fixed --since datetime was given) AND supports interactive
    time-range changes via keybindings -- both, per design conversation."""

    CSS_PATH = _CSS_PATH
    TITLE = "civitas telemetry"
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("h", "set_range('h')", "1h"),
        ("d", "set_range('d')", "24h"),
        ("w", "set_range('w')", "7d"),
        ("m", "set_range('m')", "30d"),
        ("r", "refresh_now", "Refresh"),
    ]

    def __init__(
        self,
        db_dir: str = "./civitas_telemetry",
        window_days: int = 30,
        time_range: TimeRange | None = None,
        refresh: float = 30.0,
    ) -> None:
        super().__init__()
        self._engine = SQLiteQueryEngine(db_dir=db_dir, window_days=window_days)
        self._time_range = time_range or TimeRange.default()
        self._refresh = refresh

    def compose(self) -> ComposeResult:
        yield Header()
        yield TimeRangeBar()
        with Vertical(id="main"):
            with Horizontal(id="charts"):
                yield CostChart()
                yield MessageRateChart()
            with Horizontal(id="lower"):
                yield StatPanel()
                yield CostBreakdownTable()
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(TimeRangeBar).range_label = self._time_range.label
        self._refresh_loop()

    # ------------------------------------------------------------------
    # Interaction
    # ------------------------------------------------------------------

    def action_set_range(self, code: str) -> None:
        self._time_range = TimeRange.preset(code)
        self.query_one(TimeRangeBar).range_label = self._time_range.label
        self._requery()

    def action_refresh_now(self) -> None:
        self._requery()

    # ------------------------------------------------------------------
    # Refresh loop — adapted from civitas top's _poll_forever (design
    # precedent), re-querying the local SQLite store instead of polling HTTP.
    # ------------------------------------------------------------------

    @work(exclusive=True, group="telemetry-refresh")
    async def _refresh_loop(self) -> None:
        import asyncio

        while True:
            await self._requery_once()
            await asyncio.sleep(self._refresh)

    def _requery(self) -> None:
        """Trigger an immediate re-query outside the refresh loop's own
        sleep cadence (a keybinding or manual refresh) -- fire-and-forget,
        the loop's own next tick will pick up the new time range regardless,
        this just avoids waiting up to `refresh` seconds for it."""
        self._requery_now_worker()

    @work(exclusive=True, group="telemetry-manual-refresh")
    async def _requery_now_worker(self) -> None:
        await self._requery_once()

    async def _requery_once(self) -> None:
        if not self.is_running:
            return
        now = time.time()
        since = self._time_range.since(now)

        cost_buckets = await self._engine.cost_over_time(since, now)
        rate_buckets = await self._engine.message_rate_over_time(since, now)
        by_agent = await self._engine.cost_by_agent(since, now)
        by_model = await self._engine.cost_by_model(since, now)

        if not self.is_running:
            return
        self.query_one(CostChart).update_data(cost_buckets)
        self.query_one(MessageRateChart).update_data(rate_buckets)
        total_messages = sum(b.message_count for b in rate_buckets)
        top_agent = max(by_agent.items(), key=lambda kv: kv[1]) if by_agent else None
        self.query_one(StatPanel).update_stats(sum(by_agent.values()), total_messages, top_agent)
        self.query_one(CostBreakdownTable).update_breakdown(by_agent, by_model)


__all__ = ["CivitasTelemetryApp"]
