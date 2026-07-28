"""Shared topology-server discovery for CLI commands (v0.9.1, Phase F).

Moved out of ``cli/topology.py`` because ``civitas dashboard`` (design
dashboard-v2.md §9: YAML-driven discovery only, no ``--url`` flag) now needs the
exact same "scan the YAML for a topology_server node" logic that
``civitas topology show`` already used — a private, single-purpose helper is
worth sharing rather than duplicating or (worse) importing one CLI module's
private helper from another.
"""

from __future__ import annotations

from typing import Any


def find_topology_server(config: dict[str, Any]) -> tuple[str, int] | None:
    """Scan a parsed topology YAML for a ``topology_server`` node.

    Returns ``(host, port)`` of the first one found (depth-first), or ``None``
    if the topology declares no ``topology_server`` node at all.
    """
    sup = config.get("supervision", config.get("supervisor", {}))

    def _scan(node: dict[str, Any]) -> tuple[str, int] | None:
        if node.get("type") == "topology_server":
            cfg = node.get("config", {})
            return cfg.get("host", "127.0.0.1"), cfg.get("port", 6789)
        if "supervisor" in node:
            for child in node["supervisor"].get("children", []):
                hit = _scan(child)
                if hit:
                    return hit
        return None

    for child in sup.get("children", []):
        hit = _scan(child)
        if hit:
            return hit
    return None
