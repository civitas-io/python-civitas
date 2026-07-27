"""civitas dashboard — live Textual dashboard attached to a running topology.

v0.9.1 (dashboard-v2 Phase F): YAML-driven discovery only (design §9, ratified) —
no ``--url`` flag, no "spawn my own runtime" mode. The topology YAML must declare
a ``topology_server`` node; this command finds it and attaches remotely, the same
way ``civitas topology show`` already does for its one-shot live snapshot. Keeping
both a spawn-my-own-runtime mode and a remote-attach mode would mean maintaining
two mental models for one command — the PRD's whole point is remote attach.
"""

from __future__ import annotations

from pathlib import Path

import typer
import yaml

from civitas.cli._topology_discovery import find_topology_server
from civitas.cli.app import app, err_console
from civitas.errors import ConfigurationError


@app.command()
def dashboard(
    topology: str = typer.Argument(help="Path to topology YAML file"),
    refresh: float = typer.Option(1.0, "--refresh", "-r", help="Poll interval in seconds"),
) -> None:
    """Launch the live Textual dashboard for an already-running topology.

    The topology YAML must declare a ``topology_server`` node — this command
    attaches to it remotely and polls; it does not start a runtime of its own.
    """
    try:
        from civitas.dashboard.app import CivitasDashboardApp
    except ImportError as exc:
        # Matches connect_mcp()'s pattern (civitas/process.py) for an optional
        # extra that's missing — fail fast with a clear install instruction,
        # not a raw ModuleNotFoundError traceback (design §8).
        raise ConfigurationError(
            "The dashboard requires the 'dashboard' extra. "
            "Install it with: pip install 'civitas[dashboard]'"
        ) from exc

    topology_path = Path(topology)
    if not topology_path.exists():
        err_console.print(f"[red]Error:[/red] Topology file '{topology}' not found.")
        raise typer.Exit(1)

    config = yaml.safe_load(topology_path.read_text())
    topo_server = find_topology_server(config)
    if topo_server is None:
        err_console.print(
            f"[red]Error:[/red] '{topology}' declares no 'topology_server' node — "
            "the dashboard needs a running one to attach to."
        )
        raise typer.Exit(1)

    host, port = topo_server
    CivitasDashboardApp(host=host, port=port, refresh=refresh).run()
