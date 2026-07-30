"""TopologyServer — supervised JSON HTTP management endpoint for live topology queries.

Declared as ``type: topology_server`` in topology YAML. The CLI's
``civitas topology show`` pings ``GET /topology`` and renders a live tree;
it falls back to the static YAML tree when the server is not reachable.

v0.9.3.1: ``/metrics`` now means real Prometheus text-format exposition
(the standard scrape path every Prometheus deployment defaults to) --
civitas's own JSON metrics snapshot (used by ``civitas top``) moved to
``/snapshot`` to make room for it. Breaking change to a documented
endpoint, done deliberately rather than picking a non-standard Prometheus
path: "never wise to break standards in OSS projects" (2026-07-29).
"""

from __future__ import annotations

import asyncio
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


class TopologyServer(GenServer):
    """Supervised JSON HTTP server exposing live topology state.

    Endpoints:
        GET /health          → JSON {"status": "ok"}
        GET /topology        → JSON full supervision tree with live dynamic children
        GET /agents          → JSON flat list of all running agents + status
        GET /agents/{name}   → JSON single agent status or 404
        GET /snapshot        → JSON civitas's own metrics snapshot (v0.9.3.1: renamed
                               from /metrics -- see module docstring)
        GET /metrics         → Prometheus text-format exposition (v0.9.3.1) -- the
                               standard scrape path; point a Prometheus
                               scrape_config at this with no metrics_path override
        GET /processes       → JSON one row per OS process
    """

    def __init__(
        self,
        name: str = "topology_server",
        host: str = "127.0.0.1",
        port: int = 6789,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, **kwargs)
        self._host = host
        self._port = port
        self._server: asyncio.Server | None = None

        # Injected by Runtime before on_start() is called
        self._root_supervisor: Supervisor | None = None
        self._agents: dict[str, Any] = {}  # name → AgentProcess
        # v0.9.1 (dashboard-v2, D-DASH-2/D-DASH-4): injected by Runtime.start()
        # alongside _root_supervisor/_agents — None when the caller supplied a
        # non-MetricsCollector metrics= sink of their own (documented, not silently
        # empty; see /snapshot's "not available" response, v0.9.3.1: renamed from
        # /metrics to make room for real Prometheus exposition at that path).
        self._metrics_collector: Any = None
        # v0.9.1 (D-DASH-3): primed ONCE at construction, reused for every
        # /processes request — see try_start_process_sampler()'s docstring
        # for why a fresh handle per-request would be a real bug, not style.
        self._runtime_resource_sampler = try_start_process_sampler()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        try:
            self._server = await asyncio.start_server(
                self._handle_connection,
                self._host,
                self._port,
            )
            logger.info(
                "[%s] HTTP management endpoint on http://%s:%d",
                self.name,
                self._host,
                self._port,
            )
        except OSError as exc:
            logger.warning(
                "[%s] Failed to bind HTTP server on %s:%d: %s",
                self.name,
                self._host,
                self._port,
                exc,
            )

    async def on_stop(self) -> None:
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None
        await super().on_stop()

    # ------------------------------------------------------------------
    # HTTP connection handler
    # ------------------------------------------------------------------

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            parts = request_line.decode(errors="replace").split()
            path = parts[1] if len(parts) >= 2 else "/"

            # Drain remaining request headers
            while True:
                header_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                if header_line in (b"\r\n", b"\n", b""):
                    break

            body_str, status_code, content_type = await self._route_http(path)
            body_bytes = body_str.encode()
            status_text = "200 OK" if status_code == 200 else f"{status_code} Not Found"
            header = (
                f"HTTP/1.1 {status_text}\r\n"
                f"Content-Type: {content_type}\r\n"
                f"Content-Length: {len(body_bytes)}\r\n"
                f"Connection: close\r\n"
                f"\r\n"
            ).encode()
            writer.write(header + body_bytes)
            await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _route_http(self, path: str) -> tuple[str, int, str]:
        """Async (v0.9.1, D-DASH-3) — ``/processes`` is the first route that
        needs real I/O (probing remote Worker health channels over the bus);
        every other route below stays a plain synchronous call, awaiting
        nothing — this signature change doesn't alter their behavior.

        v0.9.3.1: returns a content-type alongside body/status now, since
        ``/metrics`` (Prometheus) is plain text, not JSON like every other
        route here.
        """
        json_type = "application/json"
        if path == "/health":
            return json.dumps({"status": "ok"}), 200, json_type
        if path == "/topology":
            return json.dumps(self._build_topology()), 200, json_type
        if path == "/agents":
            return json.dumps(self._build_agents_list()), 200, json_type
        if path.startswith("/agents/"):
            name = path[len("/agents/") :]
            data = self._build_agent_detail(name)
            if data is None:
                return json.dumps({"error": f"agent '{name}' not found"}), 404, json_type
            return json.dumps(data), 200, json_type
        if path == "/snapshot":
            data, code = self._build_metrics()
            return json.dumps(data), code, json_type
        if path == "/metrics":
            return self._build_prometheus_metrics()
        if path == "/processes":
            return json.dumps(await self._build_processes()), 200, json_type
        return json.dumps({"error": "not found"}), 404, json_type

    # ------------------------------------------------------------------
    # Serialisers
    # ------------------------------------------------------------------

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
        this TopologyServer, whose "/processes" runtime entry's "id" is this
        server's own name — so that's the fallback, not a placeholder.
        """
        entry = self._registry.lookup(name) if self._registry is not None else None
        if entry is not None and entry.health_channel:
            return entry.health_channel
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
                # Worker-hosted) — always the same process as this TopologyServer.
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
        registry (v0.9.1, D-DASH-3). TopologyServer has no ``_remote_children``
        set of its own (unlike Supervisor's D5 probing) — it scans every
        registered entry instead, since it needs every Worker in the whole
        tree, not just one supervisor's remote children.
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
        TopologyServer independently probes here rather than piggybacking on
        any one supervisor's own heartbeat loop, since it needs a live
        snapshot on-demand (one HTTP request), not a periodic background one.
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
            # the WHOLE endpoint's response via _handle_connection's catch-all
            # (F03-7-style containment — one bad channel must not take down
            # every other process's data in the same response).
            return None
        except Exception:
            logger.warning("[%s] health probe to %r failed unexpectedly", self.name, channel)
            return None
        return ack.payload.get("process")

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
