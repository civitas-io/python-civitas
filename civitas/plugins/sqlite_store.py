"""SQLiteStateStore — persistent agent-state store backed by SQLite (core).

Agent state survives process crashes and restarts. Each agent's state is stored
as a JSON blob keyed by agent name.

Moved from ``civitas-contrib`` to core in v0.11.0 (B4): SQLite is stdlib, so a
SQLite-backed store adds no third-party runtime dependency and belongs in core
under the contrib boundary rule (``docs/design/spanstore-and-contrib-boundary.md``
§3). Driver-backed stores (Postgres/MySQL — need ``asyncpg``/``aiomysql``) stay
in contrib. The old ``civitas_contrib.plugins.sqlite_store`` import path is kept
as a deprecating re-export shim.

Usage::

    store = SQLiteStateStore("civitas_state.db")
    runtime = Runtime(supervisor=..., state_store=store)

Cleanup: call ``await store.close()`` explicitly (or let ``Runtime.stop()`` do
it). ``__del__`` exists as a safety net but is non-deterministic — do not rely
on it.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from typing import Any


class SQLiteStateStore:
    """SQLite-backed StateStore implementing the StateStore protocol.

    State is scoped per-agent. Stateless agents (those that never call
    ``checkpoint()``) incur zero overhead — no rows are written.

    All I/O runs in a thread executor so SQLite operations do not block the
    asyncio event loop. ``close()`` is the authoritative cleanup path and is
    called by ``Runtime.stop()``. ``__del__`` is a safety net only.
    """

    def __init__(self, db_path: str = "civitas_state.db") -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS agent_state "
            "(agent_name TEXT PRIMARY KEY, state TEXT NOT NULL)"
        )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Sync helpers (run in executor — must not touch asyncio primitives)
    # ------------------------------------------------------------------

    def _sync_get(self, agent_name: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT state FROM agent_state WHERE agent_name = ?",
            (agent_name,),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])  # type: ignore[no-any-return]

    def _sync_set(self, agent_name: str, blob: str) -> None:
        self._conn.execute(
            "INSERT INTO agent_state (agent_name, state) VALUES (?, ?) "
            "ON CONFLICT(agent_name) DO UPDATE SET state = excluded.state",
            (agent_name, blob),
        )
        self._conn.commit()

    def _sync_delete(self, agent_name: str) -> None:
        self._conn.execute(
            "DELETE FROM agent_state WHERE agent_name = ?",
            (agent_name,),
        )
        self._conn.commit()

    def _sync_list(self) -> list[str]:
        rows = self._conn.execute(
            "SELECT agent_name FROM agent_state ORDER BY agent_name"
        ).fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Async interface
    # ------------------------------------------------------------------

    async def get(self, agent_name: str) -> dict[str, Any] | None:
        """Load agent state from SQLite (non-blocking)."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sync_get, agent_name)

    async def set(self, agent_name: str, state: dict[str, Any]) -> None:
        """Save agent state to SQLite (upsert, non-blocking)."""
        blob = json.dumps(state)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_set, agent_name, blob)

    async def delete(self, agent_name: str) -> None:
        """Remove agent state from SQLite (non-blocking)."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_delete, agent_name)

    async def list_agents(self) -> list[str]:
        """Return all agent names with persisted state (non-blocking)."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sync_list)

    async def close(self) -> None:
        """Close the database connection. Authoritative cleanup path."""
        self._conn.close()

    def __del__(self) -> None:
        """Safety-net close on GC (non-deterministic — use close())."""
        try:
            self._conn.close()
        except Exception:
            pass
