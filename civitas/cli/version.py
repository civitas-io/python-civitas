"""civitas version — show the Civitas version."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from civitas.cli.app import app, console


@app.command()
def version() -> None:
    """Show the Civitas version."""
    # Read from package metadata — a hardcoded string shipped "0.1.0" in every
    # release up to v0.8.0 (caught by the first-ever CLI unit test, #42/V5).
    try:
        v = _pkg_version("civitas")
    except PackageNotFoundError:  # running from a source tree without install
        v = "unknown (not installed)"
    console.print(f"[cyan]civitas[/cyan] version [green]{v}[/green]")
