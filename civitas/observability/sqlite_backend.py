"""SQLiteBackend — civitas-native, time-windowed persistent span store.

v0.9.3.x (Track B, B1). A real ``ExportBackend`` implementation (no protocol
changes needed — see ``docs/design/telemetry-native.md`` §2) for small/local
deployments that want to ask "what did my agents spend last week" without
already running Jaeger/Grafana/Tempo.

Design summary (full rationale in ``docs/design/telemetry-native.md``):

- **One SQLite file per fixed-size time window** (``window_days``, default
  30), not one growing file with row-level deletes. Retention removes whole
  files (§3) — simpler, no fragmentation, no risk of a bad ``WHERE`` clause
  corrupting live data.
- **Hot fields promoted to real, indexed columns** (``agent_name``,
  ``llm_model``, ``llm_tokens_in``/``_out``, ``llm_cost_usd``) so B2's
  cost-over-time/message-rate/per-agent queries are plain SQL, not
  per-row JSON parsing — while the full ``attributes`` dict is *also* kept
  (``attributes_json``) for drill-down fidelity (§4).
- **Single-process scope.** Multi-process aggregation is deliberately
  deferred (§7) — see the design doc for the concrete (not "TBD") answer.

Two further follow-ups, deferred and tracked (design doc §12, ``docs/milestones.md``
v0.9.3.6/B4) rather than refactored into this already-shipped implementation: whether
this class belongs in ``civitas-contrib`` instead of core (it durably persists data to
disk, matching where ``SQLiteStateStore`` already lives, unlike the zero-I/O
``ExportBackend``/``FanOutBackend``/``ConsoleBackend`` machinery that stays in core); and
separating the telemetry-specific normalization/schema logic from SQLite-specific
storage mechanics behind a future backend-agnostic interface, so a hypothetical
``PostgresBackend`` wouldn't have to reimplement ``normalize_span()`` from scratch.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from civitas.observability.span_queue import SpanData
from civitas.observability.span_store import (
    CostBucket,
    MessageRateBucket,
    SpanRecord,
    normalize_span,
)

logger = logging.getLogger(__name__)

__all__ = [
    "SQLiteSpanStore",
    "SQLiteBackend",
    "normalize_span",
    "window_index",
    "window_filename",
    "index_from_filename",
]

# Column list + order shared by recent_spans/spans_in_trace and _row_to_span --
# kept in one place so the SELECT and the tuple-unpacking can never drift.
_SPAN_COLS = (
    "name, trace_id, span_id, parent_span_id, start_time, end_time, status, "
    "error_message, agent_name, llm_model, llm_tokens_in, llm_tokens_out, llm_cost_usd"
)


def _row_to_span(row: tuple[Any, ...]) -> SpanRecord:
    return SpanRecord(*row)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS spans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    start_time REAL NOT NULL,
    end_time REAL NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    agent_name TEXT,
    llm_model TEXT,
    llm_tokens_in INTEGER,
    llm_tokens_out INTEGER,
    llm_cost_usd REAL,
    attributes_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spans_start_time ON spans(start_time);
CREATE INDEX IF NOT EXISTS idx_spans_trace_id ON spans(trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_agent_name ON spans(agent_name);
"""


def window_index(timestamp: float, window_days: int) -> int:
    """Which fixed-size window (anchored at the UNIX epoch) a timestamp falls
    into. Fixed-size, not calendar months — avoids variable-month-length
    edge cases for a v1 (design doc §3)."""
    window_seconds = window_days * 86400
    return int(timestamp // window_seconds)


def window_filename(index: int, window_days: int) -> str:
    """Human-discoverable filename: the window's START date, not its raw
    index — `ls` shows exactly what's there and when it started."""
    window_seconds = window_days * 86400
    start = datetime.fromtimestamp(index * window_seconds, tz=UTC)
    return f"civitas_spans_{start.strftime('%Y-%m-%d')}.db"


def index_from_filename(filename: str, window_days: int) -> int | None:
    """Recover a window's index from its own filename (round-trips through
    window_filename()'s date formatting). Module-level (not a SQLiteBackend
    method) so SQLiteQueryEngine (B2, sqlite_query.py) can enumerate window
    files without reaching into SQLiteBackend's internals.

    Known limitation, not engineered around (v1): assumes ``window_days``
    hasn't changed since the file was written. Changing the window size
    across restarts of the same db_dir would misjudge older files' ages --
    a real but narrow edge case for a knob nothing in this design expects to
    change often; revisit if it's ever a real complaint, not preemptively.
    """
    try:
        date_str = filename.removeprefix("civitas_spans_").removesuffix(".db")
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None
    return int(dt.timestamp() // (window_days * 86400))


class SQLiteSpanStore:
    """Time-windowed, civitas-native persistent span store (B4).

    Implements the full ``SpanStore`` protocol — the write side (``export``/
    ``shutdown``, from the ``ExportBackend`` contract) AND the read side
    (``cost_over_time``/``recent_spans``/... ) over one shared schema, so the
    two can never drift. Used like any other exporter via
    ``exporters=[SQLiteSpanStore(...)]`` (or inside a ``FanOutBackend``), and
    queried directly for the dashboard/CLI.

    ``SQLiteBackend`` (write-only name) and ``SQLiteQueryEngine`` (read-only
    name) remain as back-compat aliases — see the bottom of this module and
    ``sqlite_query.py``.
    """

    def __init__(
        self,
        db_dir: str = "./civitas_telemetry",
        window_days: int = 30,
        retention_windows: int = 6,
    ) -> None:
        self._db_dir = Path(db_dir)
        self._window_days = window_days
        self._retention_windows = retention_windows
        # LRU-ish cache of open connections keyed by window index — most
        # writes land in the CURRENT window; a stale entry for a just-rolled-
        # -over window stays briefly reachable for a rare late/out-of-order
        # span near a boundary (design doc §3).
        self._connections: dict[int, aiosqlite.Connection] = {}

    async def export(self, spans: list[SpanData]) -> None:
        if not spans:
            return
        self._db_dir.mkdir(parents=True, exist_ok=True)

        by_window: dict[int, list[SpanData]] = {}
        for span in spans:
            idx = window_index(span.start_time, self._window_days)
            by_window.setdefault(idx, []).append(span)

        for idx, window_spans in by_window.items():
            conn = await self._connection_for(idx)
            rows = []
            for span in window_spans:
                normalized = normalize_span(span)
                rows.append(
                    (
                        span.name,
                        span.trace_id,
                        span.span_id,
                        span.parent_span_id,
                        span.start_time,
                        span.end_time,
                        span.status,
                        span.error_message,
                        normalized["agent_name"],
                        normalized["llm_model"],
                        normalized["llm_tokens_in"],
                        normalized["llm_tokens_out"],
                        normalized["llm_cost_usd"],
                        json.dumps(span.attributes),
                    )
                )
            await conn.executemany(
                """
                INSERT INTO spans (
                    name, trace_id, span_id, parent_span_id, start_time,
                    end_time, status, error_message, agent_name, llm_model,
                    llm_tokens_in, llm_tokens_out, llm_cost_usd, attributes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            await conn.commit()

        await self._sweep_retention()

    async def shutdown(self) -> None:
        for conn in self._connections.values():
            await conn.close()
        self._connections.clear()

    # ------------------------------------------------------------------
    # Read side (SpanStore query surface) -- was SQLiteQueryEngine (B2)
    # ------------------------------------------------------------------

    async def cost_over_time(
        self, since: float, until: float, bucket_seconds: int = 86400
    ) -> list[CostBucket]:
        """LLM cost/tokens bucketed by time, broken down by agent + model.

        Double GROUP BY (once per attached window file, once in the outer
        merge) -- a bucket straddling a window-file boundary would otherwise
        be split into two separate rows instead of one correctly-merged one.
        """
        sql_per_window = f"""
            SELECT
                CAST(start_time / {bucket_seconds} AS INTEGER) * {bucket_seconds} AS bucket_start,
                agent_name,
                llm_model,
                SUM(llm_cost_usd) AS total_cost_usd,
                SUM(llm_tokens_in) AS total_tokens_in,
                SUM(llm_tokens_out) AS total_tokens_out
            FROM {{alias}}.spans
            WHERE llm_cost_usd IS NOT NULL AND start_time >= ? AND start_time <= ?
            GROUP BY bucket_start, agent_name, llm_model
        """
        outer_sql = """
            SELECT bucket_start, agent_name, llm_model,
                   SUM(total_cost_usd), SUM(total_tokens_in), SUM(total_tokens_out)
            FROM ({union})
            GROUP BY bucket_start, agent_name, llm_model
            ORDER BY bucket_start
        """
        rows = await self._query_across_windows(
            since, until, sql_per_window, outer_sql, (since, until)
        )
        return [
            CostBucket(
                bucket_start=row[0],
                agent_name=row[1],
                model=row[2],
                total_cost_usd=row[3] or 0.0,
                total_tokens_in=row[4] or 0,
                total_tokens_out=row[5] or 0,
            )
            for row in rows
        ]

    async def message_rate_over_time(
        self, since: float, until: float, bucket_seconds: int = 3600
    ) -> list[MessageRateBucket]:
        """Message-handling rate bucketed by time, per agent.

        Counts `civitas.agent.handle` spans specifically -- one per message an
        agent actually processed -- matching the same semantic Prometheus
        exposition and the dashboard use, not a raw count of every span kind.
        """
        sql_per_window = f"""
            SELECT
                CAST(start_time / {bucket_seconds} AS INTEGER) * {bucket_seconds} AS bucket_start,
                agent_name,
                COUNT(*) AS message_count
            FROM {{alias}}.spans
            WHERE name = 'civitas.agent.handle' AND start_time >= ? AND start_time <= ?
            GROUP BY bucket_start, agent_name
        """
        outer_sql = """
            SELECT bucket_start, agent_name, SUM(message_count)
            FROM ({union})
            GROUP BY bucket_start, agent_name
            ORDER BY bucket_start
        """
        rows = await self._query_across_windows(
            since, until, sql_per_window, outer_sql, (since, until)
        )
        return [
            MessageRateBucket(bucket_start=row[0], agent_name=row[1], message_count=row[2] or 0)
            for row in rows
        ]

    async def cost_by_agent(self, since: float, until: float) -> dict[str, float]:
        """Total LLM cost per agent over the whole range (no time bucketing).

        Rows with agent_name IS NULL are excluded -- a dict keyed by agent name
        can't meaningfully represent "no agent".
        """
        sql_per_window = """
            SELECT agent_name, SUM(llm_cost_usd) AS cost
            FROM {alias}.spans
            WHERE llm_cost_usd IS NOT NULL AND agent_name IS NOT NULL
              AND start_time >= ? AND start_time <= ?
            GROUP BY agent_name
        """
        outer_sql = """
            SELECT agent_name, SUM(cost)
            FROM ({union})
            GROUP BY agent_name
        """
        rows = await self._query_across_windows(
            since, until, sql_per_window, outer_sql, (since, until)
        )
        return {row[0]: row[1] or 0.0 for row in rows}

    async def cost_by_model(self, since: float, until: float) -> dict[str, float]:
        """Total LLM cost per model over the whole range. NULL model excluded."""
        sql_per_window = """
            SELECT llm_model, SUM(llm_cost_usd) AS cost
            FROM {alias}.spans
            WHERE llm_cost_usd IS NOT NULL AND llm_model IS NOT NULL
              AND start_time >= ? AND start_time <= ?
            GROUP BY llm_model
        """
        outer_sql = """
            SELECT llm_model, SUM(cost)
            FROM ({union})
            GROUP BY llm_model
        """
        rows = await self._query_across_windows(
            since, until, sql_per_window, outer_sql, (since, until)
        )
        return {row[0]: row[1] or 0.0 for row in rows}

    async def recent_spans(self, since: float, until: float, limit: int = 200) -> list[SpanRecord]:
        """The most recent spans in [since, until], newest first. ``limit`` is
        an int cast into the SQL (safe -- not user text) since the cross-window
        helper binds only the per-window range params."""
        sql_per_window = (
            f"SELECT {_SPAN_COLS} FROM {{alias}}.spans WHERE start_time >= ? AND start_time <= ?"
        )
        outer_sql = "SELECT * FROM ({union}) ORDER BY start_time DESC LIMIT " + str(int(limit))
        rows = await self._query_across_windows(
            since, until, sql_per_window, outer_sql, (since, until)
        )
        return [_row_to_span(row) for row in rows]

    async def spans_in_trace(self, trace_id: str, since: float, until: float) -> list[SpanRecord]:
        """Every span in one trace, oldest first (the per-trace timeline)."""
        sql_per_window = (
            f"SELECT {_SPAN_COLS} FROM {{alias}}.spans "
            "WHERE trace_id = ? AND start_time >= ? AND start_time <= ?"
        )
        outer_sql = "SELECT * FROM ({union}) ORDER BY start_time ASC"
        rows = await self._query_across_windows(
            since, until, sql_per_window, outer_sql, (trace_id, since, until)
        )
        return [_row_to_span(row) for row in rows]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _files_in_range(self, since: float, until: float) -> list[Path]:
        """Which window files (that exist on disk) fall within [since, until] --
        computed from the range's own window indices, not by opening files."""
        if not self._db_dir.exists():
            return []
        since_idx = window_index(since, self._window_days)
        until_idx = window_index(until, self._window_days)
        matches = []
        for path in self._db_dir.glob("civitas_spans_*.db"):
            idx = index_from_filename(path.name, self._window_days)
            if idx is not None and since_idx <= idx <= until_idx:
                matches.append((idx, path))
        return [path for _, path in sorted(matches)]

    async def _query_across_windows(
        self,
        since: float,
        until: float,
        sql_per_window: str,
        outer_sql: str,
        params: tuple[Any, ...],
    ) -> list[tuple[Any, ...]]:
        """ATTACH every window file in range to one connection, run one
        UNION ALL query across them wrapped in outer_sql's re-aggregation
        (a single time bucket can straddle a window-file boundary). Returns []
        with no connection opened at all if no window files fall in range.
        """
        files = self._files_in_range(since, until)
        if not files:
            return []
        conn = await aiosqlite.connect(":memory:")
        try:
            per_window_selects = []
            for i, path in enumerate(files):
                alias = f"w{i}"
                await conn.execute("ATTACH DATABASE ? AS " + alias, (str(path),))
                per_window_selects.append(sql_per_window.format(alias=alias))
            union_sql = " UNION ALL ".join(per_window_selects)
            full_sql = outer_sql.format(union=union_sql)
            cursor = await conn.execute(full_sql, params * len(files))
            return [tuple(row) for row in await cursor.fetchall()]
        finally:
            await conn.close()

    async def _connection_for(self, idx: int) -> aiosqlite.Connection:
        conn = self._connections.get(idx)
        if conn is not None:
            return conn
        path = self._db_dir / window_filename(idx, self._window_days)
        conn = await aiosqlite.connect(str(path))
        await conn.executescript(_SCHEMA)
        await conn.commit()
        self._connections[idx] = conn
        return conn

    async def _sweep_retention(self) -> None:
        """Delete whole window FILES older than retention_windows ago — not
        row-level deletes (design doc §3). Piggybacked on every export()
        call rather than a separate background task; cheap (a directory
        listing + integer comparisons) at the batch sizes OTELAgent uses."""
        current_idx = window_index(time.time(), self._window_days)
        cutoff_idx = current_idx - self._retention_windows
        if not self._db_dir.exists():
            return
        for path in self._db_dir.glob("civitas_spans_*.db"):
            file_idx = index_from_filename(path.name, self._window_days)
            if file_idx is not None and file_idx < cutoff_idx:
                cached = self._connections.pop(file_idx, None)
                if cached is not None:
                    await cached.close()
                try:
                    path.unlink()
                except OSError:
                    logger.warning("SQLiteBackend: failed to remove expired window file %s", path)

    def list_window_files(self) -> list[Path]:
        """Enumerate this backend's window files, oldest first — for a
        future query/aggregation layer (B2) to ATTACH DATABASE across
        several windows. Not used internally; a read-only convenience."""
        if not self._db_dir.exists():
            return []
        return sorted(self._db_dir.glob("civitas_spans_*.db"))


# Back-compat alias (B4): the write-only name predating the SpanStore merge.
# SQLiteSpanStore IS the ExportBackend; existing `exporters=[SQLiteBackend(...)]`
# and `from ...sqlite_backend import SQLiteBackend` keep working unchanged.
SQLiteBackend = SQLiteSpanStore
