"""civitas state — manage persisted agent state."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import typer
from rich.table import Table

from civitas.cli.app import console, error, info, success, warn
from civitas.plugins.encrypted_store import ENVELOPE_KEY

try:
    from civitas_contrib.plugins.sqlite_store import SQLiteStateStore
except ImportError:
    SQLiteStateStore = None

state_app = typer.Typer(
    name="state",
    help="Manage persisted agent state.",
    no_args_is_help=True,
)


async def _load_all_states(
    db_path: str,
) -> tuple[list[str], dict[str, dict[str, Any] | None]]:
    """Fetch all agent names and their states in a single event loop."""
    store = SQLiteStateStore(db_path)
    try:
        agents = await store.list_agents()
        states = {name: await store.get(name) for name in agents}
        return agents, states
    finally:
        await store.close()


async def _delete_agents(db_path: str, names: list[str]) -> None:
    """Delete state for each name in a single event loop."""
    store = SQLiteStateStore(db_path)
    try:
        for name in names:
            await store.delete(name)
    finally:
        await store.close()


@state_app.command("list")
def state_list(
    db: str = typer.Option("agency_state.db", "--db", help="SQLite database path"),
) -> None:
    """Show all persisted agent states."""
    db_path = Path(db)
    if not db_path.exists():
        warn(f"No state database found at '{db}'.")
        raise typer.Exit(0)

    agents, states = asyncio.run(_load_all_states(str(db_path)))

    if not agents:
        warn("No persisted agent states found.")
        return

    table = Table(title="Persisted Agent States", show_lines=True)
    table.add_column("Agent", style="cyan")
    table.add_column("State", style="white", overflow="fold")

    for agent_name in agents:
        state = states.get(agent_name)
        if state and ENVELOPE_KEY in state:
            state_str = "<encrypted>"
        elif state:
            state_str = json.dumps(state, indent=2)
        else:
            state_str = "{}"
        table.add_row(agent_name, state_str)

    console.print(table)


@state_app.command("clear")
def state_clear(
    agent_name: str | None = typer.Argument(None, help="Agent name to clear (omit for all)"),
    db: str = typer.Option("agency_state.db", "--db", help="SQLite database path"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
) -> None:
    """Clear persisted agent state."""
    db_path = Path(db)
    if not db_path.exists():
        warn(f"No state database found at '{db}'.")
        raise typer.Exit(0)

    agents, states = asyncio.run(_load_all_states(str(db_path)))

    if agent_name:
        if agent_name not in states or states[agent_name] is None:
            warn(f"No state found for agent '{agent_name}'.")
            return
        if not force:
            confirm = typer.confirm(f"Clear state for agent '{agent_name}'?")
            if not confirm:
                raise typer.Abort()
        asyncio.run(_delete_agents(str(db_path), [agent_name]))
        success(f"Cleared state for agent '{agent_name}'.")
    else:
        if not agents:
            warn("No persisted agent states found.")
            return
        if not force:
            confirm = typer.confirm(f"Clear state for ALL {len(agents)} agents?")
            if not confirm:
                raise typer.Abort()
        asyncio.run(_delete_agents(str(db_path), agents))
        success(f"Cleared state for {len(agents)} agents.")


# ---------------------------------------------------------------------------
# migrate
# ---------------------------------------------------------------------------


def _parse_dsn(dsn: str) -> Any:
    """Return an appropriate StateStore for the given DSN string."""
    from civitas_contrib.plugins.postgres_store import PostgresStateStore

    if dsn.startswith("sqlite:"):
        path = dsn.removeprefix("sqlite:")
        return SQLiteStateStore(path)
    if dsn.startswith("postgresql://") or dsn.startswith("postgres://"):
        return PostgresStateStore(dsn)
    if dsn.endswith(".db") or dsn.endswith(".sqlite") or dsn.endswith(".sqlite3"):
        return SQLiteStateStore(dsn)
    raise typer.BadParameter(
        f"Cannot determine backend from DSN {dsn!r}. Use 'sqlite:<path>' or a postgresql:// URL."
    )


async def _do_migrate(
    src_dsn: str,
    dst_dsn: str,
    dry_run: bool,
) -> int:
    """Copy all agent state from src to dst. Returns the number of entries copied."""
    src = _parse_dsn(src_dsn)
    dst = _parse_dsn(dst_dsn)
    try:
        agents = await src.list_agents()
        if not agents:
            warn("Source store is empty — nothing to migrate.")
            return 0

        info(f"Found {len(agents)} agent(s) in source store.")
        count = 0
        for name in agents:
            state = await src.get(name)
            if state is None:
                continue
            if dry_run:
                console.print(f"  [dim]dry-run[/dim]  {name}: {json.dumps(state)[:80]}")
            else:
                await dst.set(name, state)
                console.print(f"  [green]copied[/green]  {name}")
            count += 1

        return count
    finally:
        await src.close()
        await dst.close()


@state_app.command("migrate")
def state_migrate(
    src: str = typer.Argument(..., help="Source DSN — 'sqlite:<path>' or postgresql://..."),
    dst: str = typer.Argument(..., help="Destination DSN — 'sqlite:<path>' or postgresql://..."),
    dry_run: bool = typer.Option(
        True,
        "--dry-run/--execute",
        help="Preview the migration without writing (default). Pass --execute to apply.",
    ),
) -> None:
    """Migrate agent state between backends (SQLite ↔ Postgres).

    Runs in dry-run mode by default — pass --execute to write to the destination.

    Note: migrate copies raw stored values between backends without an encryption
    wrapper, so it does NOT encrypt or decrypt. To re-encrypt state in place, use
    the dual-read sequence (design §7.5): deploy the encrypted store with
    allow_plaintext_read=true so legacy plaintext is read and re-written as
    ciphertext on the next checkpoint, then flip to strict.

    Examples:

        civitas state migrate sqlite:agency_state.db postgresql://user:pass@host/db
        civitas state migrate sqlite:agency_state.db postgresql://user:pass@host/db --execute
    """
    if dry_run:
        warn("Dry-run mode — no data will be written. Pass --execute to apply.")
    else:
        info(f"Migrating: {src} → {dst}")

    try:
        count = asyncio.run(_do_migrate(src, dst, dry_run))
    except Exception as exc:
        error(f"Migration failed: {exc}")
        raise typer.Exit(1) from exc

    if dry_run:
        success(f"Dry-run complete — {count} agent(s) would be migrated.")
    else:
        success(f"Migrated {count} agent(s).")
