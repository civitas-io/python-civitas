"""Route table — maps (HTTP method, path) to (agent, mode) from YAML config.

YAML is the sole authoritative source for gateway routing.
RouteTable.from_class() is a validation-only helper used by
`civitas topology validate`, never by the gateway at runtime.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


def route(method: str, path: str, *, mode: str = "call") -> Callable[..., Any]:
    """Annotate a GenServer/AgentProcess method with HTTP route metadata.

    Stores ``fn._civitas_route`` for use by ``civitas topology validate``
    and ``RouteTable.merge_contracts_from()``.  The YAML ``routes:`` block
    is always the runtime-authoritative source; this decorator is documentation
    and opt-in validation only.
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        fn._civitas_route = {"method": method.upper(), "path": path, "mode": mode}  # type: ignore[attr-defined]
        return fn

    return decorator


@dataclass
class RouteEntry:
    """A single route mapping an HTTP method + path pattern to an agent."""

    method: str
    path_pattern: str
    agent: str
    mode: str = "call"
    middleware: list[str] = field(default_factory=list)
    # Optional Pydantic schemas — set via merge_contracts_from()
    request_schema: type[Any] | None = field(default=None, repr=False)
    response_schema: type[Any] | None = field(default=None, repr=False)
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

    @classmethod
    def from_class(cls, agent_cls: type) -> RouteTable:
        """Validation-only: scan an agent class for @route-decorated methods.

        Used exclusively by ``civitas topology validate`` to cross-check YAML
        routes against decorator annotations. Never called at gateway runtime.
        """
        entries: list[RouteEntry] = []
        for attr_name in dir(agent_cls):
            fn = getattr(agent_cls, attr_name, None)
            meta: dict[str, Any] | None = getattr(fn, "_civitas_route", None)
            if meta is not None:
                contract_meta: dict[str, Any] | None = getattr(fn, "_civitas_contract", None)
                entries.append(
                    RouteEntry(
                        method=meta["method"],
                        path_pattern=meta["path"],
                        agent="",
                        mode=meta.get("mode", "call"),
                        request_schema=(contract_meta["request"] if contract_meta else None),
                        response_schema=(contract_meta["response"] if contract_meta else None),
                    )
                )
        return cls(entries)

    def merge_contracts_from(self, agent_cls: type, agent_name: str = "") -> None:
        """Scan *agent_cls* for ``@route`` + ``@contract`` and update matching entries.

        Call this after building the table from YAML to attach Pydantic schemas
        to entries that match a decorator annotation. Matches by (method, path).
        """
        for attr_name in dir(agent_cls):
            fn = getattr(agent_cls, attr_name, None)
            route_meta: dict[str, Any] | None = getattr(fn, "_civitas_route", None)
            contract_meta: dict[str, Any] | None = getattr(fn, "_civitas_contract", None)
            if route_meta is None or contract_meta is None:
                continue
            method = route_meta["method"].upper()
            path = route_meta["path"]
            for entry in self._entries:
                if entry.method == method and entry.path_pattern == path:
                    entry.request_schema = contract_meta.get("request")
                    entry.response_schema = contract_meta.get("response")

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
