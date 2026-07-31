"""TopologyAgent — live topology introspection, served via HTTPGateway.

Declared as ``type: topology_server`` in topology YAML (the YAML node name is
unchanged for backward compatibility). The CLI's ``civitas topology show``
pings ``GET /topology`` and renders a live tree; it falls back to the static
YAML tree when the endpoint is not reachable.

v0.9.3.1: ``/metrics`` means real Prometheus text-format exposition (the
standard scrape path every Prometheus deployment defaults to) -- civitas's own
JSON metrics snapshot (used by ``civitas top``) lives at ``/snapshot``.

v0.9.5 (docs/design/topology-gateway-merge.md, migration phases 1-6):
the old ``TopologyServer`` -- a standalone, zero-auth ``asyncio.start_server``
HTTP server -- has been REMOVED (deliberate breaking change, D6). A
``type: topology_server`` YAML node now builds a ``TopologyAgent`` (this
file's data provider, privileged-injected by ``Runtime`` exactly as
``TopologyServer`` was) plus an internally-owned ``HTTPGateway`` that serves
the seven fixed introspection routes with the same already-audited AuthN
stack (API key / JWT / mTLS) as any other gateway. ``_TopologyIntrospection``
holds the introspection logic; ``TopologyAgent`` exposes it via
``handle_call()``, reached over the gateway's routes rather than its own
socket. Direct-construction (non-YAML) callers of the removed
``TopologyServer`` must migrate to constructing an ``HTTPGateway`` +
``TopologyAgent`` themselves (or just use the YAML node).
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from civitas.dashboard.resources import sample_process, try_start_process_sampler
from civitas.errors import MessageRoutingError
from civitas.genserver import GenServer
from civitas.messages import Message, _new_span_id, _uuid7
from civitas.observability.prometheus_export import (
    PROMETHEUS_CONTENT_TYPE,
    render_prometheus_metrics,
)
from civitas.supervisor import DynamicSupervisor, Supervisor

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass


class _TopologyIntrospection:
    """Introspection logic backing ``TopologyAgent`` (and, until v0.9.5's phase
    6 removal, the old ``TopologyServer``).

    Kept as a distinct base rather than folded into ``TopologyAgent`` so
    ``Runtime`` can dispatch its privileged injection on
    ``isinstance(agent, _TopologyIntrospection)`` -- one stable predicate for
    "this agent needs the topology references," independent of the concrete
    class. Pure functions over privileged, ``Runtime``-injected same-process
    state (``_root_supervisor``/``_agents``/``_metrics_collector``) -- a
    deliberate exemption from this codebase's own "route by name, never by
    object" rule. Expects the concrete class to also be a
    ``GenServer``/``AgentProcess`` (for ``self.name``, ``self._bus``,
    ``self._registry``) and to set ``_root_supervisor``/``_agents``/
    ``_metrics_collector``/``_runtime_resource_sampler`` itself (see
    ``TopologyAgent.__init__``).
    """

    name: str
    _root_supervisor: Supervisor | None
    _agents: dict[str, Any]
    _metrics_collector: Any
    _runtime_resource_sampler: Any
    # Set unconditionally by AgentProcess.__init__ (civitas/process.py) -- not
    # a new contract this mixin invents, just declaring the real type instead
    # of a defensive getattr() that would erase it into Any for mypy.
    _registry: Any
    _bus: Any

    def _build_topology(self) -> dict[str, Any]:
        if self._root_supervisor is None:
            return {"error": "runtime not available"}
        return self._serialize_node(self._root_supervisor)

    def _process_id_for(self, name: str) -> str:
        """Which `/processes` entry hosts ``name`` (v0.9.1, D-DASH addendum).

        Matches `/processes`' own "id" fields exactly, so a client can join the
        two endpoints directly: a remote agent's registry entry carries the
        Worker's health_channel (D5) — the same string `_build_processes()`
        uses as a worker entry's "id". Everything else (no registry, no
        channel — the common in-process case) runs in the SAME OS process as
        this introspection endpoint, whose "/processes" runtime entry's "id" is
        this server's own name — so that's the fallback, not a placeholder.
        """
        entry = self._registry.lookup(name) if self._registry is not None else None
        if entry is not None and entry.health_channel:
            return str(entry.health_channel)
        return self.name

    def _serialize_node(self, node: Any, restart_count: int = 0) -> dict[str, Any]:
        """Serialize one tree node. ``restart_count`` is supplied by the PARENT
        (v0.9.1, D-DASH-1) — a node cannot know its own restart count, only the
        supervisor tracking it can; the root (no parent) defaults to 0, matching
        §6.1's ``_status_snapshot()`` convention of tracking restarts per CHILD.
        """
        if isinstance(node, DynamicSupervisor):
            return {
                "name": node.name,
                "type": "dynamic_supervisor",
                "status": node.status.value,
                "restart_count": restart_count,
                "process_id": self._process_id_for(node.name),
                "max_children": node.max_children,
                "max_total_spawns": node.max_total_spawns,
                "live_count": len(node._dynamic_children),
                "children": [
                    {
                        "name": n,
                        "type": "agent",
                        "status": rec.agent.status.value,
                        "restart_count": node._child_restart_counts.get(n, 0),
                        "process_id": self._process_id_for(n),
                        "capabilities": list(rec.agent.capabilities),
                        "capability_metadata": dict(rec.agent.capability_metadata),
                        "uptime_seconds": rec.agent.uptime_seconds,
                        "session_turn_count": rec.agent.session_turn_count,
                        "session_duration_seconds": rec.agent.session_duration_seconds,
                        "suspend_category": rec.agent.suspend_category,
                    }
                    for n, rec in node._dynamic_children.items()
                ],
            }
        if isinstance(node, Supervisor):
            return {
                "name": node.name,
                "type": "supervisor",
                "strategy": node.strategy.value,
                "restart_count": restart_count,
                # v0.9.1 (D-DASH-1): this supervisor's OWN restart-window occupancy
                # — reuses the same v0.9.0 D6 introspection data
                # Supervisor.handle()'s civitas.supervision.status already computes
                # (_status_snapshot()) — same-process attribute read, no bus hop.
                "crashes_in_window": len(node._engine.window),
                # A Supervisor is never remote (only the agents it manages can be
                # Worker-hosted) — always the same process as this introspection agent.
                "process_id": self.name,
                "children": [
                    self._serialize_node(c, node._restart_counts.get(c.name, 0))
                    for c in node.children
                ],
            }
        # Generic AgentProcess
        return {
            "name": node.name,
            "type": "agent",
            "status": node.status.value,
            "restart_count": restart_count,
            "process_id": self._process_id_for(node.name),
            "capabilities": list(node.capabilities),
            "capability_metadata": dict(node.capability_metadata),
            "uptime_seconds": node.uptime_seconds,
            "session_turn_count": node.session_turn_count,
            "session_duration_seconds": node.session_duration_seconds,
            "suspend_category": node.suspend_category,
        }

    def _build_agents_list(self) -> list[dict[str, Any]]:
        # v0.9.1 (D-DASH-1): capabilities/capability_metadata/uptime_seconds for
        # every static agent — the "agent description" from the dashboard PRD.
        # restart_count for static agents isn't included here (it lives on the
        # /topology tree, attributed to the correct parent supervisor); dynamic
        # children DO carry it below, since their parent DynSup is directly at
        # hand while walking the tree.
        result: list[dict[str, Any]] = [
            {
                "name": name,
                "status": agent.status.value,
                "process_id": self._process_id_for(name),
                "capabilities": list(agent.capabilities),
                "capability_metadata": dict(agent.capability_metadata),
                "uptime_seconds": agent.uptime_seconds,
                "session_turn_count": agent.session_turn_count,
                "session_duration_seconds": agent.session_duration_seconds,
                "suspend_category": agent.suspend_category,
            }
            for name, agent in self._agents.items()
        ]
        # Include live dynamic children (not in the static _agents map)
        if self._root_supervisor is not None:
            self._collect_dynamic_children(self._root_supervisor, result)
        return result

    def _collect_dynamic_children(self, node: Any, result: list[dict[str, Any]]) -> None:
        if isinstance(node, DynamicSupervisor):
            for n, rec in node._dynamic_children.items():
                result.append(
                    {
                        "name": n,
                        "status": rec.agent.status.value,
                        "restart_count": node._child_restart_counts.get(n, 0),
                        "process_id": self._process_id_for(n),
                        "capabilities": list(rec.agent.capabilities),
                        "capability_metadata": dict(rec.agent.capability_metadata),
                        "uptime_seconds": rec.agent.uptime_seconds,
                        "session_turn_count": rec.agent.session_turn_count,
                        "session_duration_seconds": rec.agent.session_duration_seconds,
                        "suspend_category": rec.agent.suspend_category,
                    }
                )
        elif isinstance(node, Supervisor):
            for child in node.children:
                self._collect_dynamic_children(child, result)

    def _build_agent_detail(self, name: str) -> dict[str, Any] | None:
        agent = self._agents.get(name)
        if agent is None:
            agent = self._find_dynamic_agent(name)
        if agent is None:
            return None
        return {
            "name": name,
            "status": agent.status.value,
            "process_id": self._process_id_for(name),
            "capabilities": list(agent.capabilities),
            "capability_metadata": dict(agent.capability_metadata),
            "uptime_seconds": agent.uptime_seconds,
            "session_turn_count": agent.session_turn_count,
            "session_duration_seconds": agent.session_duration_seconds,
            "suspend_category": agent.suspend_category,
        }

    def _build_metrics(self) -> tuple[dict[str, Any], int]:
        """v0.9.1 (dashboard-v2, D-DASH-2): live MetricsCollector snapshot.

        Returns a documented "not available" body (not a silent empty
        snapshot) when no MetricsCollector was wired — e.g. a Runtime
        constructed with a custom, non-MetricsCollector metrics= sink.
        """
        if self._metrics_collector is None:
            return {"error": "metrics not available"}, 404
        snapshot = self._metrics_collector.snapshot
        return {
            "agents": {
                name: {
                    "messages_handled": m.messages_handled,
                    "messages_sent": m.messages_sent,
                    "avg_latency_ms": m.avg_latency_ms,
                    "restarts": m.restarts,
                    "errors": m.errors,
                    "tokens_in": m.tokens_in,
                    "tokens_out": m.tokens_out,
                    "cost_usd": m.cost_usd,
                    "last_model": m.last_model,
                }
                for name, m in snapshot.agents.items()
            },
            "total_messages": snapshot.total_messages,
            "total_cost_usd": snapshot.total_cost_usd,
            "uptime_seconds": snapshot.uptime_seconds,
            # v0.9.1 (dashboard-v2 addendum, 2026-07-26): already collected by
            # MetricsCollector since it was first written, never previously
            # exposed via this endpoint — a real, safe, read-only timeline of
            # every restart event (not just the current count), free to add.
            "restart_history": [
                {
                    "agent_name": e.agent_name,
                    "timestamp": e.timestamp,
                    "reason": e.reason,
                }
                for e in snapshot.restart_history
            ],
        }, 200

    def _build_prometheus_metrics(self) -> tuple[str, int, str]:
        """v0.9.3.1: real Prometheus text-format exposition at the standard
        ``/metrics`` scrape path. Same underlying data as /snapshot (JSON,
        v0.9.1) -- a different representation of the same MetricsCollector
        snapshot, not a second collection mechanism. Absent-collector shape
        mirrors /snapshot's: an explicit, documented empty body rather than
        pretending metrics exist when they don't (a Prometheus scrape of an
        empty 200 body is valid -- "no series right now" -- so no error
        status is needed here the way /snapshot's 404 is for JSON clients).
        """
        if self._metrics_collector is None:
            return "", 200, PROMETHEUS_CONTENT_TYPE
        return (
            render_prometheus_metrics(self._metrics_collector.snapshot),
            200,
            PROMETHEUS_CONTENT_TYPE,
        )

    async def _build_processes(self) -> dict[str, Any]:
        """v0.9.1 (dashboard-v2, D-DASH-3): one row per OS process (the
        Runtime itself + every distinct Worker health channel known to the
        registry) — reuses the D5 ``_agency.health_probe`` wire protocol
        rather than inventing a new one; Workers already answer these.
        """
        processes: list[dict[str, Any]] = []

        runtime_sample = sample_process(self._runtime_resource_sampler)
        if runtime_sample is not None:
            processes.append({"kind": "runtime", "id": self.name, **runtime_sample})

        for channel in self._distinct_health_channels():
            worker_sample = await self._probe_worker_process(channel)
            if worker_sample is not None:
                processes.append({"kind": "worker", "id": channel, **worker_sample})

        return {"processes": processes}

    def _distinct_health_channels(self) -> set[str]:
        """Every distinct Worker health channel currently known to the
        registry (v0.9.1, D-DASH-3). No ``_remote_children`` set of its own
        (unlike Supervisor's D5 probing) — scans every registered entry
        instead, since it needs every Worker in the whole tree, not just one
        supervisor's remote children.
        """
        if self._registry is None:
            return set()
        channels: set[str] = set()
        for name in self._registry.all_names():
            entry = self._registry.lookup(name)
            if entry is not None and entry.health_channel:
                channels.add(entry.health_channel)
        return channels

    async def _probe_worker_process(self, channel: str) -> dict[str, Any] | None:
        """Send one ``_agency.health_probe`` to a Worker's channel and read
        back its ``process`` field — the exact message shape
        ``Supervisor._probe_health_channel`` already sends (D5, v0.9.0);
        independently probed here rather than piggybacking on any one
        supervisor's own heartbeat loop, since it needs a live snapshot
        on-demand (one request), not a periodic background one.
        """
        if self._bus is None:
            return None
        probe = Message(
            type="_agency.health_probe",
            sender=self.name,
            recipient=channel,
            correlation_id=_uuid7(),
            span_id=_new_span_id(),
        )
        try:
            ack = await self._bus.request(probe, timeout=2.0)
        except (TimeoutError, MessageRoutingError):
            # Unreachable OR not-yet-routable (e.g. announced moments after the
            # agent it hosts — a real, observed startup race, not theoretical):
            # simply absent from /processes, never an exception that would
            # otherwise propagate through _build_processes() and silently kill
            # the WHOLE endpoint's response (F03-7-style containment — one bad
            # channel must not take down every other process's data).
            return None
        except Exception:
            logger.warning("[%s] health probe to %r failed unexpectedly", self.name, channel)
            return None
        result: dict[str, Any] | None = ack.payload.get("process")
        return result

    def _find_dynamic_agent(self, name: str) -> Any | None:
        if self._root_supervisor is None:
            return None
        return self._search_tree(self._root_supervisor, name)

    def _search_tree(self, node: Any, name: str) -> Any | None:
        if isinstance(node, DynamicSupervisor):
            rec = node._dynamic_children.get(name)
            return rec.agent if rec is not None else None
        if isinstance(node, Supervisor):
            for child in node.children:
                found = self._search_tree(child, name)
                if found is not None:
                    return found
        return None


class TopologyAgent(_TopologyIntrospection, GenServer):
    """v0.9.5 — reached via ``HTTPGateway``'s routes, not its own HTTP server.

    Same privileged injection contract as ``TopologyServer``
    (``_root_supervisor``/``_agents``/``_metrics_collector``, set by ``Runtime``
    exactly as today) — this is the ONLY thing that makes topology
    introspection possible, and does not change. What changes is how a
    request reaches ``handle_call()``: through ``HTTPGateway``'s ASGI
    transport + middleware chain (inheriting its already-audited API
    key/JWT/mTLS AuthN) instead of a hand-rolled, zero-auth
    ``asyncio.start_server``.

    ``handle_call()`` dispatches on ``payload["__op__"]`` and returns each
    response body pre-encoded as JSON (or, for ``"metrics"``, Prometheus
    text) via the ``{"__raw_body__", "__content_type__", "__status__"}``
    sentinel (``docs/design/topology-gateway-merge.md`` D4) — byte-for-byte
    identical wire bodies to ``TopologyServer``'s, including ``/agents``'
    bare-JSON-array shape, which a plain ``GenServer.handle_call()`` dict
    return could not produce directly (``handle_call()`` must return a
    dict; a bare list is not a dict). Every op is deliberately routed
    through the SAME sentinel, not just ``"metrics"``, specifically to
    guarantee this parity rather than mixing two response shapes.
    """

    def __init__(self, name: str = "topology_agent", **kwargs: Any) -> None:
        super().__init__(name, **kwargs)
        # Injected by Runtime before on_start() is called -- identical
        # contract to TopologyServer's own __init__ above.
        self._root_supervisor: Supervisor | None = None
        self._agents: dict[str, Any] = {}
        self._metrics_collector: Any = None
        self._runtime_resource_sampler = try_start_process_sampler()

    @staticmethod
    def _raw_json(data: Any, status: int = 200) -> dict[str, Any]:
        return {
            "__raw_body__": json.dumps(data),
            "__content_type__": "application/json",
            "__status__": status,
        }

    async def handle_call(self, payload: dict[str, Any], from_: str) -> dict[str, Any]:
        op = payload.get("__op__")
        if op == "health":
            return self._raw_json({"status": "ok"})
        if op == "topology":
            return self._raw_json(self._build_topology())
        if op == "agents":
            return self._raw_json(self._build_agents_list())
        if op == "agent_detail":
            name = str(payload.get("name", ""))
            data = self._build_agent_detail(name)
            if data is None:
                return self._raw_json({"error": f"agent '{name}' not found"}, status=404)
            return self._raw_json(data)
        if op == "snapshot":
            data, code = self._build_metrics()
            return self._raw_json(data, status=code)
        if op == "metrics":
            body, status, content_type = self._build_prometheus_metrics()
            return {"__raw_body__": body, "__content_type__": content_type, "__status__": status}
        if op == "processes":
            return self._raw_json(await self._build_processes())
        return self._raw_json({"error": "not found"}, status=404)
