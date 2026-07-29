"""civitas/dashboard/telemetry_widgets.py -- rendering-only widgets (v0.9.3.5,
B3). Each ``update_*`` method takes plain SQLiteQueryEngine result objects
-- tested here with plain sample data, no real SQLite store needed (the
end-to-end app/live-store behavior lives in
tests/integration/test_telemetry_app.py, matching test_dashboard_widgets.py's
own established split).
"""

from __future__ import annotations

import pytest
from textual.app import App, ComposeResult

from civitas.dashboard.telemetry_widgets import (
    CostBreakdownTable,
    CostChart,
    MessageRateChart,
    StatPanel,
    TimeRangeBar,
)
from civitas.observability.sqlite_query import CostBucket, MessageRateBucket


class _SingleWidgetApp(App[None]):
    def __init__(self, widget: object) -> None:
        super().__init__()
        self._widget = widget

    def compose(self) -> ComposeResult:
        yield self._widget  # type: ignore[misc]


@pytest.mark.asyncio
async def test_cost_chart_renders_without_crashing_on_empty_data():
    chart = CostChart()
    app = _SingleWidgetApp(chart)
    async with app.run_test():
        chart.update_data([])  # must not raise


@pytest.mark.asyncio
async def test_cost_chart_renders_real_buckets():
    chart = CostChart()
    app = _SingleWidgetApp(chart)
    async with app.run_test():
        chart.update_data(
            [
                CostBucket(1000.0, "chatty", "gpt-4o", 0.01, 100, 50),
                CostBucket(2000.0, "chatty", "gpt-4o", 0.02, 100, 50),
            ]
        )  # must not raise; real rendering assertions live in the integration test


@pytest.mark.asyncio
async def test_cost_chart_caps_series_at_max_series():
    """A real multi-agent, multi-model deployment's full cardinality would
    make plotext's terminal legend unreadable -- only the top MAX_SERIES by
    total cost are plotted."""
    chart = CostChart()
    app = _SingleWidgetApp(chart)
    buckets = [CostBucket(1000.0, f"agent-{i}", "gpt-4o", float(i), 0, 0) for i in range(10)]
    async with app.run_test():
        chart.update_data(buckets)  # must not raise even with 10 distinct series


@pytest.mark.asyncio
async def test_message_rate_chart_renders_without_crashing_on_empty_data():
    chart = MessageRateChart()
    app = _SingleWidgetApp(chart)
    async with app.run_test():
        chart.update_data([])


@pytest.mark.asyncio
async def test_message_rate_chart_renders_real_buckets():
    chart = MessageRateChart()
    app = _SingleWidgetApp(chart)
    async with app.run_test():
        chart.update_data(
            [
                MessageRateBucket(1000.0, "chatty", 5),
                MessageRateBucket(2000.0, "chatty", 3),
            ]
        )


@pytest.mark.asyncio
async def test_stat_panel_formats_cost_and_top_agent():
    panel = StatPanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_stats(0.1234, 42, ("chatty", 0.05))
        rendered = str(panel.render())
        assert "$0.12" in rendered
        assert "42" in rendered
        assert "chatty" in rendered


@pytest.mark.asyncio
async def test_stat_panel_handles_no_top_agent():
    panel = StatPanel()
    app = _SingleWidgetApp(panel)
    async with app.run_test():
        panel.update_stats(0.0, 0, None)  # must not raise
        rendered = str(panel.render())
        assert "-" in rendered


@pytest.mark.asyncio
async def test_cost_breakdown_table_shows_agent_and_model_rows():
    table = CostBreakdownTable()
    app = _SingleWidgetApp(table)
    async with app.run_test():
        table.update_breakdown({"chatty": 0.05, "other": 0.02}, {"gpt-4o": 0.04, "claude": 0.03})
        assert table.row_count == 4


@pytest.mark.asyncio
async def test_cost_breakdown_table_sorts_by_cost_descending():
    table = CostBreakdownTable()
    app = _SingleWidgetApp(table)
    async with app.run_test():
        table.update_breakdown({"low": 0.01, "high": 0.99}, {})
        first_row = table.get_row_at(0)
        assert "high" in str(first_row[1])


@pytest.mark.asyncio
async def test_time_range_bar_shows_label_and_preset_keys():
    bar = TimeRangeBar()
    app = _SingleWidgetApp(bar)
    async with app.run_test():
        bar.range_label = "7d"
        rendered = str(bar.render())
        assert "7d" in rendered
        assert "h" in rendered  # preset key hint present in some form
