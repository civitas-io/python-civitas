"""CivitasTelemetryApp -- end-to-end Textual app tests (v0.9.3.5, B3).

Widget-level rendering is already covered by
tests/unit/test_telemetry_widgets.py; these prove the app wires a REAL
Runtime + SQLiteBackend's real output through SQLiteQueryEngine into the
actual mounted widgets, and that the interactive time-range keybindings
work -- matching this codebase's standing preference for real infra over
mocks wherever practical (test_dashboard_app.py's own established pattern).
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from civitas import AgentProcess, Supervisor
from civitas.dashboard.telemetry_app import CivitasTelemetryApp
from civitas.dashboard.telemetry_time import TimeRange
from civitas.dashboard.telemetry_widgets import (
    CostBreakdownTable,
    CostChart,
    EventLogTable,
    MessageRateChart,
    StatPanel,
    TimeRangeBar,
)
from civitas.messages import Message
from civitas.observability.sqlite_backend import SQLiteBackend
from civitas.runtime import Runtime


class _Chatty(AgentProcess):
    def __init__(self, name: str, cost_usd: float) -> None:
        super().__init__(name)
        self._cost_usd = cost_usd

    async def handle(self, message: Message) -> Message | None:
        with self.llm_span("gpt-4o") as span:
            span.set_attribute("civitas.llm.tokens_in", 100)
            span.set_attribute("civitas.llm.tokens_out", 50)
            span.set_attribute("civitas.llm.cost_usd", self._cost_usd)
        return self.reply({"ok": True})


async def _wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"condition never became true within {timeout}s")


async def _seed_real_store(db_dir: str) -> None:
    """Real Runtime + real SQLiteBackend -- not synthetic SpanData -- exactly
    what the actual data source will produce."""
    backend = SQLiteBackend(db_dir=db_dir)
    agent = _Chatty("chatty", cost_usd=0.05)
    runtime = Runtime(supervisor=Supervisor("root", children=[agent]), exporters=[backend])
    await runtime.start()
    try:
        await runtime.ask("chatty", {"q": 1})
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_telemetry_app_renders_real_runtime_output(tmp_path: Path) -> None:
    await _seed_real_store(str(tmp_path))

    app = CivitasTelemetryApp(db_dir=str(tmp_path), time_range=TimeRange.default(), refresh=60.0)
    async with app.run_test() as pilot:
        await pilot.pause()

        def _has_data() -> bool:
            table = app.query_one(CostBreakdownTable)
            return table.row_count > 0

        await _wait_until(_has_data)

        stat_text = str(app.query_one(StatPanel).render())
        assert "$0.05" in stat_text
        assert "chatty" in stat_text

        # v0.10.1 (B3.7): the event feed populated from the same real run.
        events = app.query_one(EventLogTable)
        assert events.row_count > 0


@pytest.mark.asyncio
async def test_telemetry_app_manual_refresh_binding(tmp_path: Path) -> None:
    """Pressing 'r' triggers an immediate re-query, not waiting for the next
    scheduled refresh tick."""
    app = CivitasTelemetryApp(db_dir=str(tmp_path), time_range=TimeRange.default(), refresh=300.0)
    async with app.run_test() as pilot:
        await pilot.pause()
        # Store starts empty -- write real data AFTER the app's first query.
        await _seed_real_store(str(tmp_path))
        await pilot.press("r")
        await pilot.pause()

        def _has_data() -> bool:
            table = app.query_one(CostBreakdownTable)
            return table.row_count > 0

        await _wait_until(_has_data)


@pytest.mark.asyncio
async def test_telemetry_app_time_range_keybindings_switch_and_requery(tmp_path: Path) -> None:
    await _seed_real_store(str(tmp_path))
    app = CivitasTelemetryApp(db_dir=str(tmp_path), time_range=TimeRange.default(), refresh=300.0)
    async with app.run_test() as pilot:
        await pilot.pause()

        def _has_data() -> bool:
            return app.query_one(CostBreakdownTable).row_count > 0

        await _wait_until(_has_data)

        await pilot.press("h")
        await pilot.pause()
        assert app.query_one(TimeRangeBar).range_label == "1h"

        await pilot.press("w")
        await pilot.pause()
        assert app.query_one(TimeRangeBar).range_label == "7d"


@pytest.mark.asyncio
async def test_telemetry_app_time_range_outside_data_shows_empty(tmp_path: Path) -> None:
    """A time range that genuinely has no data in it must render an empty
    breakdown, not stale data from a previous query or a crash.

    Note: TimeRange's "until" bound is always the query's own current
    time.time() (see _requery_once()) -- a fixed_since range only pins the
    START, not the end. So a genuinely-excluded-data test must seed data
    OUTSIDE the [since, now] window from the OTHER direction: write a span
    far in the FUTURE relative to "now", then query the (default, last-24h)
    window -- which cannot possibly include a future timestamp.
    """
    db_dir = str(tmp_path)
    backend = SQLiteBackend(db_dir=db_dir, retention_windows=100000)
    from civitas.observability.span_queue import SpanData

    far_future = time.time() + 400 * 86400
    await backend.export(
        [
            SpanData(
                name="civitas.llm.chat",
                trace_id="a" * 32,
                span_id="1" * 16,
                parent_span_id=None,
                start_time=far_future,
                end_time=far_future + 1,
                attributes={
                    "civitas.agent.name": "chatty",
                    "civitas.llm.model": "gpt-4o",
                    "civitas.llm.cost_usd": 0.05,
                },
                status="ok",
            )
        ]
    )
    await backend.shutdown()

    app = CivitasTelemetryApp(db_dir=db_dir, time_range=TimeRange.default(), refresh=300.0)
    async with app.run_test() as pilot:
        await pilot.pause()
        await asyncio.sleep(0.3)
        await pilot.pause()
        assert app.query_one(CostBreakdownTable).row_count == 0


@pytest.mark.asyncio
async def test_telemetry_app_charts_never_crash_on_launch_with_empty_store(tmp_path: Path) -> None:
    """No data written at all yet -- the app must still mount and render
    cleanly, not crash on an empty SQLite directory."""
    app = CivitasTelemetryApp(db_dir=str(tmp_path), time_range=TimeRange.default())
    async with app.run_test() as pilot:
        await pilot.pause()
        assert app.query_one(CostChart) is not None
        assert app.query_one(MessageRateChart) is not None
