"""Civitas CLI — command-line interface for the Civitas runtime.

Built with Typer + Rich (DR-011). See docs/08-CLI-Design.md for the
full design specification.

Package structure:
    app.py       — shared Typer app, consoles, output helpers
    init.py      — civitas init
    run.py       — civitas run
    state.py     — civitas state list|clear|migrate
    topology.py  — civitas topology validate|show|diff
    deploy.py    — civitas deploy
    dashboard.py — civitas dashboard ("civitas top", needs [dashboard] extra)
    telemetry.py — civitas telemetry (needs [telemetry] extra)
    security.py  — civitas security init zmq|nats
    version.py   — civitas version
    _templates/  — scaffolding templates
"""

from __future__ import annotations

# v0.9.1 (Phase F): civitas.cli.dashboard now defers its optional 'dashboard'
# extra (textual/psutil) import to inside the command function itself
# (ConfigurationError with install instructions on invoke, matching
# connect_mcp()'s pattern) — this top-level import always succeeds now, no
# guard needed; the command still appears in --help without the extra
# installed, only failing when actually run (a real UX improvement over the
# old guard, which hid the whole command from --help).
import civitas.cli.dashboard  # noqa: F401

# Register all subcommands by importing the modules that decorate them.
# Each module adds its commands to the shared `app` instance.
import civitas.cli.init  # noqa: F401
import civitas.cli.run  # noqa: F401

# v0.9.3.5 (B3): civitas.cli.telemetry follows the exact same pattern for
# its optional 'telemetry' extra (aiosqlite/textual/textual-plotext).
import civitas.cli.telemetry  # noqa: F401
import civitas.cli.version  # noqa: F401
from civitas.cli.app import app
from civitas.cli.deploy import deploy_app

# Register subcommand groups
from civitas.cli.security import security_app
from civitas.cli.state import state_app
from civitas.cli.topology import topology_app

app.add_typer(state_app, name="state")
app.add_typer(topology_app, name="topology")
app.add_typer(deploy_app, name="deploy")
app.add_typer(security_app, name="security")


def main() -> None:
    """CLI entry point — called by ``[project.scripts] civitas``."""
    app()
