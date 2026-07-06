"""Civitas CLI — command-line interface for the Civitas runtime.

Built with Typer + Rich (DR-011). See docs/08-CLI-Design.md for the
full design specification.

Package structure:
    app.py       — shared Typer app, consoles, output helpers
    init.py      — civitas init
    run.py       — civitas run
    state.py     — civitas state list|clear
    topology.py  — civitas topology validate|show|diff
    deploy.py    — civitas deploy (M2.7)
    version.py   — civitas version
    _templates/  — scaffolding templates
"""

from __future__ import annotations

# F09-8: guard dashboard import — it requires optional dependencies (rich Live).
# If the import fails, the rest of the CLI still loads normally.
try:
    import civitas.cli.dashboard  # noqa: F401
except ImportError:
    pass

# Register all subcommands by importing the modules that decorate them.
# Each module adds its commands to the shared `app` instance.
import civitas.cli.init  # noqa: F401
import civitas.cli.run  # noqa: F401
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
