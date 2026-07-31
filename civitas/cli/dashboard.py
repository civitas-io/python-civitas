"""civitas dashboard — live Textual dashboard attached to one or more running
topologies.

v0.9.1 (dashboard-v2 Phase F): YAML-driven discovery only (design §9, ratified) —
no ``--url`` flag, no "spawn my own runtime" mode. Each topology YAML must declare
a ``topology_server`` node; this command finds it and attaches remotely, the same
way ``civitas topology show`` already does for its one-shot live snapshot. Keeping
both a spawn-my-own-runtime mode and a remote-attach mode would mean maintaining
two mental models for one command — the PRD's whole point is remote attach.

v0.9.4: accepts multiple topology files — one ``ClusterTarget`` per file,
attached to concurrently and switchable via tabs (design/dashboard-v2.md P2).
A single file behaves exactly as it did in v0.9.1-v0.9.3 (no tab bar at all).
"""

from __future__ import annotations

import re
from pathlib import Path

import typer
import yaml

from civitas.cli._topology_discovery import find_topology_server
from civitas.cli.app import app, err_console
from civitas.errors import ConfigurationError

_SAFE_LABEL_RE = re.compile(r"[^a-zA-Z0-9_-]+")


def _parse_headers(header_opts: list[str]) -> dict[str, str]:
    """Parse repeated ``--header 'Name: Value'`` options into a dict (v0.9.6).

    A general, scheme-agnostic auth mechanism matching the control-plane auth
    seam: any header the operator's middleware expects
    (``Authorization: Bearer ...``, ``X-API-Key: ...``, a custom one) goes
    through verbatim -- civitas privileges no scheme.
    """
    headers: dict[str, str] = {}
    for raw in header_opts:
        name, sep, value = raw.partition(":")
        if not sep or not name.strip():
            raise ConfigurationError(
                f"Invalid --header {raw!r}; expected 'Name: Value' (e.g. "
                "'Authorization: Bearer <token>')"
            )
        headers[name.strip()] = value.strip()
    return headers


def _label_for(topology_path: Path) -> str:
    """A tab label derived from the topology file's own name -- sanitized to
    a safe Textual widget-ID character set (Tab IDs must be valid CSS
    identifiers; a topology file could plausibly have dots/spaces in its
    stem that a raw ``Path.stem`` would carry through unsafely).
    """
    return _SAFE_LABEL_RE.sub("-", topology_path.stem) or "topology"


@app.command()
def dashboard(
    topologies: list[str] = typer.Argument(help="Path(s) to topology YAML file(s)"),
    refresh: float = typer.Option(1.0, "--refresh", "-r", help="Poll interval in seconds"),
    header: list[str] = typer.Option(
        [],
        "--header",
        "-H",
        help="Auth header to send, 'Name: Value' (repeatable). E.g. "
        "-H 'Authorization: Bearer <token>' or -H 'X-API-Key: <key>'. "
        "Applied to every topology.",
    ),
) -> None:
    """Launch the live Textual dashboard for one or more already-running topologies.

    Each topology YAML must declare a ``topology_server`` node — this command
    attaches to each remotely and polls; it does not start a runtime of its own.
    Given more than one topology, each gets its own tab (v0.9.4). Use ``--header``
    to authenticate to an endpoint behind the control-plane auth seam (v0.9.6).
    """
    headers = _parse_headers(header)
    try:
        from civitas.dashboard.app import CivitasDashboardApp, ClusterTarget
    except ImportError as exc:
        # Matches connect_mcp()'s pattern (civitas/process.py) for an optional
        # extra that's missing — fail fast with a clear install instruction,
        # not a raw ModuleNotFoundError traceback (design §8).
        raise ConfigurationError(
            "The dashboard requires the 'dashboard' extra. "
            "Install it with: pip install 'civitas[dashboard]'"
        ) from exc

    clusters: list[ClusterTarget] = []
    seen_labels: dict[str, int] = {}
    for topology in topologies:
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
        label = _label_for(topology_path)
        # Disambiguate two topology files that happen to share a stem (e.g.
        # two different directories' "topology.yaml") -- a real, if
        # unlikely, collision a naive one-shot label derivation would
        # otherwise produce two identically-labeled, indistinguishable tabs.
        seen_labels[label] = seen_labels.get(label, 0) + 1
        if seen_labels[label] > 1:
            label = f"{label}-{seen_labels[label]}"
        clusters.append(ClusterTarget(label=label, host=host, port=port, headers=headers))

    CivitasDashboardApp(clusters=clusters, refresh=refresh).run()
