"""Route table — maps (HTTP method, path) to (agent, mode) from YAML config.

YAML is the sole authoritative source for gateway routing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RouteEntry:
    """A single route mapping an HTTP method + path pattern to an agent."""

    method: str
    path_pattern: str
    agent: str
    mode: str = "call"
    middleware: list[str] = field(default_factory=list)
    # v0.9.5 (topology-gateway-merge.md D4): when True, a handle_call() reply
    # shaped as {"__raw_body__": str, "__content_type__": str} is sent verbatim
    # instead of JSON-encoded -- the Prometheus /metrics exposition case. Only
    # auto-registered topology routes set this; ordinary agent routes never do.
    raw_response: bool = False
    # v0.9.5 (topology-gateway-merge.md D2): fixed payload keys merged into the
    # dispatched request AFTER body+path_params (so a client cannot override
    # them). Auto-registered topology routes use this to carry {"__op__": ...}
    # to TopologyAgent.handle_call(); ordinary YAML routes never set it.
    payload_extra: dict[str, Any] = field(default_factory=dict, repr=False)
    # v0.9.6 (control-plane-writes.md D2): when True, the dispatch layer injects
    # the authenticated principal (request.auth["principal"], or the
    # {"id": "unauthenticated"} default) into the payload under the reserved key
    # "__principal__", merged LAST so a client body cannot spoof it. Only the
    # auto-registered control-plane WRITE routes set this; read/ordinary routes
    # never carry the principal into the dispatched payload (preserving the
    # original "auth is never merged into the payload" intent for everything
    # except the writes that genuinely need an honest actor in their audit).
    inject_principal: bool = False
    segments: list[tuple[bool, str]] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        self.method = self.method.upper()
        self.segments = _parse_pattern(self.path_pattern)


def _parse_pattern(pattern: str) -> list[tuple[bool, str]]:
    """Parse a path pattern into (is_param, name) segments.

    "/v1/sessions/{id}/history" →
        [(False, "v1"), (False, "sessions"), (True, "id"), (False, "history")]
    """
    result: list[tuple[bool, str]] = []
    for part in pattern.strip("/").split("/"):
        if part.startswith("{") and part.endswith("}"):
            result.append((True, part[1:-1]))
        else:
            result.append((False, part))
    return result


def _match_segments(
    entry_segs: list[tuple[bool, str]],
    path_segs: list[str],
) -> dict[str, str] | None:
    if len(entry_segs) != len(path_segs):
        return None
    params: dict[str, str] = {}
    for (is_param, name), seg in zip(entry_segs, path_segs, strict=False):
        if is_param:
            params[name] = seg
        elif name != seg:
            return None
    return params


class RouteTable:
    """Ordered route table. First match wins."""

    def __init__(self, entries: list[RouteEntry] | None = None) -> None:
        self._entries: list[RouteEntry] = entries or []

    @classmethod
    def from_config(cls, routes: list[dict[str, Any]]) -> RouteTable:
        """Build from the ``routes:`` list in a topology YAML config block."""
        entries = [
            RouteEntry(
                method=r["method"],
                path_pattern=r["path"],
                agent=r["agent"],
                mode=r.get("mode", "call"),
                middleware=r.get("middleware", []),
            )
            for r in routes
        ]
        return cls(entries)

    def match(self, method: str, path: str) -> tuple[RouteEntry, dict[str, str]] | None:
        """Return (entry, path_params) for the first matching route, or None."""
        path_segs = path.strip("/").split("/") if path.strip("/") else []
        for entry in self._entries:
            if entry.method != method.upper():
                continue
            params = _match_segments(entry.segments, path_segs)
            if params is not None:
                return entry, params
        return None

    def entries(self) -> list[RouteEntry]:
        return list(self._entries)

    def __len__(self) -> int:
        return len(self._entries)
