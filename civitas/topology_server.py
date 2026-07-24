"""TopologyServer — supervised JSON HTTP management endpoint for live topology queries.

Declared as ``type: topology_server`` in topology YAML. The CLI's
``civitas topology show`` pings ``GET /topology`` and renders a live tree;
it falls back to the static YAML tree when the server is not reachable.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any

from civitas.genserver import GenServer
from civitas.supervisor import DynamicSupervisor, Supervisor

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass


class TopologyServer(GenServer):
    """Supervised JSON HTTP server exposing live topology state.

    Endpoints (read-only, JSON):
        GET /health          → {"status": "ok"}
        GET /topology        → full supervision tree with live dynamic children
        GET /agents          → flat list of all running agents + status
        GET /agents/{name}   → single agent status or 404
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
        # empty; see /metrics's "not available" response).
        self._metrics_collector: Any = None

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

            body_str, status_code = self._route_http(path)
            body_bytes = body_str.encode()
            status_text = "200 OK" if status_code == 200 else f"{status_code} Not Found"
            header = (
                f"HTTP/1.1 {status_text}\r\n"
                f"Content-Type: application/json\r\n"
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

    def _route_http(self, path: str) -> tuple[str, int]:
        if path == "/health":
            return json.dumps({"status": "ok"}), 200
        if path == "/topology":
            return json.dumps(self._build_topology()), 200
        if path == "/agents":
            return json.dumps(self._build_agents_list()), 200
        if path.startswith("/agents/"):
            name = path[len("/agents/") :]
            data = self._build_agent_detail(name)
            if data is None:
                return json.dumps({"error": f"agent '{name}' not found"}), 404
            return json.dumps(data), 200
        if path == "/metrics":
            data, code = self._build_metrics()
            return json.dumps(data), code
        return json.dumps({"error": "not found"}), 404

    # ------------------------------------------------------------------
    # Serialisers
    # ------------------------------------------------------------------

    def _build_topology(self) -> dict[str, Any]:
        if self._root_supervisor is None:
            return {"error": "runtime not available"}
        return self._serialize_node(self._root_supervisor)

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
                "max_children": node.max_children,
                "max_total_spawns": node.max_total_spawns,
                "live_count": len(node._dynamic_children),
                "children": [
                    {
                        "name": n,
                        "type": "agent",
                        "status": rec.agent.status.value,
                        "restart_count": node._child_restart_counts.get(n, 0),
                        "capabilities": list(rec.agent.capabilities),
                        "capability_metadata": dict(rec.agent.capability_metadata),
                        "uptime_seconds": rec.agent.uptime_seconds,
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
            "capabilities": list(node.capabilities),
            "capability_metadata": dict(node.capability_metadata),
            "uptime_seconds": node.uptime_seconds,
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
                "capabilities": list(agent.capabilities),
                "capability_metadata": dict(agent.capability_metadata),
                "uptime_seconds": agent.uptime_seconds,
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
                        "capabilities": list(rec.agent.capabilities),
                        "capability_metadata": dict(rec.agent.capability_metadata),
                        "uptime_seconds": rec.agent.uptime_seconds,
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
            "capabilities": list(agent.capabilities),
            "capability_metadata": dict(agent.capability_metadata),
            "uptime_seconds": agent.uptime_seconds,
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
        }, 200

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
