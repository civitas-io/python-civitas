"""Widgets for ``civitas telemetry`` (v0.9.3.5, B3) — charts (via
textual-plotext), stat panels, and a per-agent/per-model cost breakdown
table. Reuses ``civitas top``'s established palette/style conventions
(``civitas/dashboard/palette.py``) rather than inventing a new visual
language for a second dashboard.
"""

from __future__ import annotations

import datetime as dt

from textual.reactive import reactive
from textual.widgets import DataTable, Static
from textual_plotext import PlotextPlot

from civitas.dashboard.palette import format_cost
from civitas.observability.sqlite_query import CostBucket, MessageRateBucket


class CostChart(PlotextPlot):
    """Cost-over-time line chart, one series per (agent, model) pair.

    Deliberately caps the number of plotted series (see MAX_SERIES) --
    plotext's terminal-rendered legend becomes unreadable well before a
    real multi-agent, multi-model deployment's full cardinality would fit;
    the top-N-by-total-cost series are plotted, the rest folded into a
    single "other" series rather than silently dropped.
    """

    MAX_SERIES = 6

    def on_mount(self) -> None:
        self.border_title = "Cost over time"

    def update_data(self, buckets: list[CostBucket]) -> None:
        plt = self.plt
        plt.clear_figure()
        plt.title("Cost over time (USD)")
        plt.date_form("Y-m-d H:M")
        if not buckets:
            self.refresh()
            return

        series: dict[str, dict[float, float]] = {}
        for b in buckets:
            key = f"{b.agent_name or '?'} ({b.model or '?'})"
            points = series.setdefault(key, {})
            points[b.bucket_start] = points.get(b.bucket_start, 0.0) + b.total_cost_usd

        totals = {key: sum(points.values()) for key, points in series.items()}
        top_keys = sorted(totals, key=lambda k: totals[k], reverse=True)[: self.MAX_SERIES]

        all_times = sorted({b.bucket_start for b in buckets})
        dates = [plt.datetime_to_string(dt.datetime.fromtimestamp(t)) for t in all_times]

        for key in top_keys:
            points = series[key]
            values = [points.get(t, 0.0) for t in all_times]
            plt.plot(dates, values, label=key)

        self.refresh()


class MessageRateChart(PlotextPlot):
    """Message-rate-over-time chart, one series per agent."""

    MAX_SERIES = 6

    def on_mount(self) -> None:
        self.border_title = "Message rate over time"

    def update_data(self, buckets: list[MessageRateBucket]) -> None:
        plt = self.plt
        plt.clear_figure()
        plt.title("Messages handled per bucket")
        plt.date_form("Y-m-d H:M")
        if not buckets:
            self.refresh()
            return

        series: dict[str, dict[float, int]] = {}
        for b in buckets:
            key = b.agent_name or "?"
            series.setdefault(key, {})[b.bucket_start] = b.message_count

        totals = {key: sum(points.values()) for key, points in series.items()}
        top_keys = sorted(totals, key=lambda k: totals[k], reverse=True)[: self.MAX_SERIES]

        all_times = sorted({b.bucket_start for b in buckets})
        dates = [plt.datetime_to_string(dt.datetime.fromtimestamp(t)) for t in all_times]

        for key in top_keys:
            points = series[key]
            values = [points.get(t, 0) for t in all_times]
            plt.plot(dates, values, label=key)

        self.refresh()


class StatPanel(Static):
    """A small set of at-a-glance totals -- total spend, total messages,
    top agent by cost. A "gauge/counter" panel, deliberately plain text
    (Static.update() resolves $tokens fine -- see palette.py's note on
    Tree/DataTable content NOT doing so) rather than a chart -- these are
    point-in-time numbers, not a trend.
    """

    def on_mount(self) -> None:
        self.border_title = "Totals"

    def update_stats(
        self, total_cost: float, total_messages: int, top_agent: tuple[str, float] | None
    ) -> None:
        top_line = f"{top_agent[0]} ({format_cost(top_agent[1])})" if top_agent else "-"
        self.update(
            f"[b]Total spend:[/b]    {format_cost(total_cost)}\n"
            f"[b]Total messages:[/b] {total_messages}\n"
            f"[b]Top agent:[/b]      {top_line}"
        )


class CostBreakdownTable(DataTable[str]):
    """Per-agent, per-model cost breakdown -- the two SQLiteQueryEngine
    whole-range totals (cost_by_agent/cost_by_model), side by side.
    """

    def on_mount(self) -> None:
        self.border_title = "Breakdown"
        self.add_columns("Kind", "Name", "Cost")
        self.cursor_type = "row"

    def update_breakdown(self, by_agent: dict[str, float], by_model: dict[str, float]) -> None:
        self.clear()
        for name, cost in sorted(by_agent.items(), key=lambda kv: kv[1], reverse=True):
            self.add_row("agent", name, format_cost(cost))
        for name, cost in sorted(by_model.items(), key=lambda kv: kv[1], reverse=True):
            self.add_row("model", name, format_cost(cost))
        # v0.10.1: the table (a ScrollView) already scrolls natively at any
        # cardinality -- the backlog's "would overflow" premise was inaccurate
        # (verified: 100 rows in a 20-row viewport scroll fine). The real
        # large-deployment gap was a *scroll affordance*: with dozens of
        # agents/models a user can't tell there's more below the fold. The
        # count in the title is that signal.
        self.border_title = f"Breakdown ({len(by_agent)} agents, {len(by_model)} models)"


class TimeRangeBar(Static):
    """Shows the currently-active time range and available preset keys --
    the interactive time-range switcher (v0.9.3.5, per-conversation: both a
    --since launch flag AND interactive in-TUI changes are supported).
    """

    range_label: reactive[str] = reactive("")

    def on_mount(self) -> None:
        self.border_title = "Time range"

    def watch_range_label(self, label: str) -> None:
        self.update(f"[b]{label}[/b]   [dim]([h]1h [d]24h [w]7d [m]30d  [r]efresh now)[/dim]")
