"""civitas telemetry — Textual TUI for B1/B2's native SQLite telemetry
store (v0.9.3.5, Track B, B3).
"""

from __future__ import annotations

import typer

from civitas.cli.app import app
from civitas.errors import ConfigurationError


@app.command()
def telemetry(
    db_dir: str = typer.Argument(
        "./civitas_telemetry", help="Path to the telemetry SQLite directory"
    ),
    window_days: int = typer.Option(
        30, "--window-days", help="Must match the SQLiteBackend's own window_days"
    ),
    since: str = typer.Option(
        "24h",
        "--since",
        help="Duration shorthand ('1h', '24h', '7d', '30d') or an ISO datetime. "
        "Also changeable interactively in the TUI (h/d/w/m keys).",
    ),
    refresh: float = typer.Option(30.0, "--refresh", "-r", help="Re-query interval in seconds"),
) -> None:
    """Launch the live Textual telemetry TUI over a local SQLite store.

    Reads directly from `db_dir` -- no running Runtime/TopologyServer
    required (unlike `civitas dashboard`, which attaches to a live process).
    """
    try:
        from civitas.dashboard.telemetry_app import CivitasTelemetryApp
    except ImportError as exc:
        # Matches civitas dashboard's own pattern (civitas/cli/dashboard.py)
        # for an optional extra that's missing -- fail fast with a clear
        # install instruction, not a raw ModuleNotFoundError traceback.
        raise ConfigurationError(
            "civitas telemetry requires the 'telemetry' extra. "
            "Install it with: pip install 'civitas[telemetry]'"
        ) from exc

    from civitas.dashboard.telemetry_time import parse_since

    try:
        time_range = parse_since(since)
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc

    app_instance = CivitasTelemetryApp(
        db_dir=db_dir, window_days=window_days, time_range=time_range, refresh=refresh
    )
    app_instance.run()
