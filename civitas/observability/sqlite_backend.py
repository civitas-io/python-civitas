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

logger = logging.getLogger(__name__)

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


def normalize_span(span: SpanData) -> dict[str, Any]:
    """Map a SpanData's attributes onto the promoted columns.

    Design doc §4's normalization table, checked in order (first match
    wins) — different span KINDS use different attribute keys for "which
    agent is this about"; there is no single universal key. A span that
    matches none of these patterns gets agent_name=None, not a guess.
    """
    attrs = span.attributes
    agent_name: str | None = None
    llm_model: str | None = None
    llm_tokens_in: int | None = None
    llm_tokens_out: int | None = None
    llm_cost_usd: float | None = None

    if span.name.startswith("civitas.agent."):
        agent_name = attrs.get("civitas.agent.name")
    elif span.name == "civitas.llm.chat":
        # AgentProcess.llm_span() -- the actually-used, ergonomic API real
        # agent code calls (confirmed via examples/dashboard_demo/agents.py).
        # civitas.agent.name was ADDED to this span's attributes as part of
        # this same change (civitas/process.py) after discovering, while
        # writing this normalization function, that no civitas.llm.chat span
        # had EVER carried any agent identity -- a real, silent gap, fixed at
        # the root rather than worked around here.
        agent_name = attrs.get("civitas.agent.name")
        llm_model = attrs.get("civitas.llm.model")
        llm_tokens_in = attrs.get("civitas.llm.tokens_in")
        llm_tokens_out = attrs.get("civitas.llm.tokens_out")
        llm_cost_usd = attrs.get("civitas.llm.cost_usd")
    elif span.name.startswith("llm.chat "):
        # Tracer.start_llm_span()/end_llm_span() -- a lower-level, standalone
        # API (examples/research_assistant.py, examples/observable_pipeline.py)
        # called directly on a bare Tracer with no AgentProcess/"self" context
        # at all -- architecturally, it has no agent identity to attach
        # (unlike AgentProcess.llm_span() above, which DOES have `self.name`
        # available and was missing it as a genuine oversight, now fixed).
        # agent_name stays None here deliberately -- not a guess, and not the
        # same class of bug as civitas.llm.chat's.
        llm_model = attrs.get("llm.model")
        llm_tokens_in = attrs.get("llm.tokens_in")
        llm_tokens_out = attrs.get("llm.tokens_out")
        llm_cost_usd = attrs.get("llm.cost_usd")
    elif span.name.startswith(("send ", "recv ")):
        agent_name = attrs.get("civitas.sender")
    elif span.name.startswith("tool.execute"):
        agent_name = attrs.get("civitas.sender")
    elif span.name == "supervisor.restart":
        agent_name = attrs.get("civitas.child")

    return {
        "agent_name": agent_name,
        "llm_model": llm_model,
        "llm_tokens_in": llm_tokens_in,
        "llm_tokens_out": llm_tokens_out,
        "llm_cost_usd": llm_cost_usd,
    }


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


class SQLiteBackend:
    """Time-windowed, civitas-native persistent span store.

    Conforms to the ExportBackend protocol (export/shutdown) — used exactly
    like any other exporter, via `exporters=[SQLiteBackend(...)]` or inside
    a FanOutBackend alongside others.
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
    # Internal
    # ------------------------------------------------------------------

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
            file_idx = self._index_from_filename(path.name)
            if file_idx is not None and file_idx < cutoff_idx:
                cached = self._connections.pop(file_idx, None)
                if cached is not None:
                    await cached.close()
                try:
                    path.unlink()
                except OSError:
                    logger.warning("SQLiteBackend: failed to remove expired window file %s", path)

    def _index_from_filename(self, filename: str) -> int | None:
        """Recover a window's index from its own filename (round-trips
        through window_filename()'s date formatting) — used by the
        retention sweep to identify expired files without keeping a
        separate side-table of index->filename mappings.

        Known limitation, not engineered around (v1): assumes ``window_days``
        hasn't changed since the file was written. Changing the window size
        across restarts of the same db_dir would misjudge older files' ages
        during retention — a real but narrow edge case for a knob nothing in
        this design expects to change often; revisit if it's ever a real
        complaint, not preemptively.
        """
        try:
            date_str = filename.removeprefix("civitas_spans_").removesuffix(".db")
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            return None
        return int(dt.timestamp() // (self._window_days * 86400))

    def list_window_files(self) -> list[Path]:
        """Enumerate this backend's window files, oldest first — for a
        future query/aggregation layer (B2) to ATTACH DATABASE across
        several windows. Not used internally; a read-only convenience."""
        if not self._db_dir.exists():
            return []
        return sorted(self._db_dir.glob("civitas_spans_*.db"))
