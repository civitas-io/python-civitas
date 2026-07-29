"""SQLiteQueryEngine — read-only query/aggregation layer over B1's SQLite
telemetry store (v0.9.3.x, Track B, B2).

Cost-over-time, message-rate-over-time, per-agent/per-model breakdowns — a
pure query API, deliberately decoupled from any UI/CLI (that's B3's job,
still undecided). A future TUI tab, web page, or CLI command all build on
top of the same methods here.

Cross-window queries (a time range spanning more than one of SQLiteBackend's
window files) use SQLite's native ``ATTACH DATABASE`` — one real SQL query
across N attached files, not N round trips merged in Python (design doc §3
flagged this as the natural fit and explicitly left it to B2).

Same tracked placement caveat as SQLiteBackend itself (design doc §12,
``docs/milestones.md`` v0.9.3.6/B4): lives here, coupled to SQLiteBackend's
schema, for now — moves together if/when that refactor happens.

Note (v0.9.3.x, per-conversation): only four methods ship in this first cut
(cost/message-rate over time, cost by agent/model) — deliberately not
exhaustive. See ``docs/design/telemetry-native.md`` §13 for a tracked list
of further query method candidates evaluated for a future cut, not built
now.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import aiosqlite

from civitas.observability.sqlite_backend import index_from_filename, window_index


@dataclasses.dataclass
class CostBucket:
    """One time bucket's LLM cost/token totals, broken down by agent+model."""

    bucket_start: float
    agent_name: str | None
    model: str | None
    total_cost_usd: float
    total_tokens_in: int
    total_tokens_out: int


@dataclasses.dataclass
class MessageRateBucket:
    """One time bucket's message-handling count for one agent."""

    bucket_start: float
    agent_name: str | None
    message_count: int


class SQLiteQueryEngine:
    """Read-only queries over SQLiteBackend's time-windowed spans tables.

    Never writes — a separate connection/lifecycle from SQLiteBackend's own
    (which owns writing); this class only ever opens short-lived, read-only
    connections per query call.
    """

    def __init__(self, db_dir: str = "./civitas_telemetry", window_days: int = 30) -> None:
        self._db_dir = Path(db_dir)
        self._window_days = window_days

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

        Counts `civitas.agent.handle` spans specifically -- one per message
        an agent actually processed -- matching the SAME semantic Prometheus
        exposition (A2, `civitas_messages_handled_total`) and the dashboard
        already use, not a raw count of every span kind (send/recv spans
        would double-count the same logical message).
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

        Rows with agent_name IS NULL (the lower-level Tracer.start_llm_span()
        API has no agent identity to attach -- see sqlite_backend.py's
        normalize_span()) are excluded -- a dict keyed by agent name can't
        meaningfully represent "no agent".
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
        """Total LLM cost per model over the whole range (no time bucketing).

        Rows with llm_model IS NULL are excluded -- same reasoning as
        cost_by_agent() above.
        """
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

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _files_in_range(self, since: float, until: float) -> list[Path]:
        """Which of SQLiteBackend's window files (that actually exist on
        disk) fall within [since, until] -- computed from the range's own
        window indices, not by opening every file to check."""
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
        (needed because a single time bucket can straddle a window-file
        boundary -- see cost_over_time()'s docstring). Returns [] with no
        connection opened at all if no window files fall in range.
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
