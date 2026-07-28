"""MetricsCollector — tracks runtime metrics for dashboard display.

Hooks into the Tracer and Runtime to collect per-agent statistics:
message counts, latency, costs, restart events, and status changes.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class AgentMetrics:
    """Accumulated metrics for a single agent."""

    name: str
    status: str = "unknown"
    messages_handled: int = 0
    messages_sent: int = 0
    total_latency_ms: float = 0.0
    restarts: int = 0
    last_restart: float | None = None
    errors: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0
    last_active: float | None = None
    # v0.9.1 (dashboard-v2, D-DASH-5): written by llm_call() once llm_span()
    # feeds it (FD-01 close-out) — empty until an LLM call actually reports one.
    last_model: str = ""

    @property
    def avg_latency_ms(self) -> float:
        """Average message handling latency in milliseconds."""
        if self.messages_handled == 0:
            return 0.0
        return self.total_latency_ms / self.messages_handled


@dataclass
class RestartEvent:
    """A recorded restart event for history display."""

    agent_name: str
    timestamp: float
    reason: str = ""


@dataclass
class RuntimeSnapshot:
    """Point-in-time snapshot of runtime metrics."""

    agents: dict[str, AgentMetrics] = field(default_factory=dict)
    restart_history: list[RestartEvent] = field(default_factory=list)
    started_at: float | None = None
    total_messages: int = 0
    total_cost_usd: float = 0.0

    @property
    def uptime_seconds(self) -> float:
        """Seconds since runtime started."""
        if self.started_at is None:
            return 0.0
        return time.time() - self.started_at


class MetricsCollector:
    """Collects and aggregates runtime metrics.

    This collector is designed to be fed events from the Runtime, Tracer,
    and Supervisor. It maintains a running snapshot that the dashboard
    reads from.
    """

    def __init__(self) -> None:
        self._snapshot = RuntimeSnapshot()

    @property
    def snapshot(self) -> RuntimeSnapshot:
        """Return the current metrics snapshot."""
        return self._snapshot

    def runtime_started(self) -> None:
        """Record that the runtime has started."""
        self._snapshot.started_at = time.time()

    def register_agent(self, name: str) -> None:
        """Register an agent for metrics tracking up front.

        Not required before the ``*_handled``/``*_sent``/etc. methods below
        will record anything (v0.9.1, dashboard-v2 D-DASH addendum: they
        self-register lazily via ``_agent()``) — calling this explicitly is
        still worth doing for agents you want visible with all-zero metrics
        BEFORE their first activity (what ``Runtime.start()`` does for
        statically-declared agents). Dynamically-spawned children need no
        equivalent call — they register themselves the moment they do
        anything, with no coupling to how or when they were created.
        """
        self._agent(name)

    def _agent(self, name: str) -> AgentMetrics:
        """Return this agent's metrics, creating a fresh entry on first contact.

        v0.9.1 (dashboard-v2 D-DASH addendum): this is what makes
        pre-registration a nicety, not a requirement — a dynamically-spawned
        agent (never known to ``Runtime.all_agents()``'s static snapshot) is
        tracked correctly from its first reported event, no spawn-time hook
        needed anywhere.
        """
        return self._snapshot.agents.setdefault(name, AgentMetrics(name=name))

    def agent_status_changed(self, name: str, status: str) -> None:
        """Record an agent status change."""
        metrics = self._agent(name)
        metrics.status = status
        metrics.last_active = time.time()

    def message_handled(self, agent_name: str, latency_ms: float) -> None:
        """Record that an agent handled a message."""
        metrics = self._agent(agent_name)
        metrics.messages_handled += 1
        metrics.total_latency_ms += latency_ms
        metrics.last_active = time.time()
        self._snapshot.total_messages += 1

    def message_sent(self, agent_name: str) -> None:
        """Record that an agent sent a message."""
        self._agent(agent_name).messages_sent += 1

    def agent_restarted(self, name: str, reason: str = "") -> None:
        """Record an agent restart event."""
        metrics = self._agent(name)
        metrics.restarts += 1
        metrics.last_restart = time.time()
        event = RestartEvent(agent_name=name, timestamp=time.time(), reason=reason)
        self._snapshot.restart_history.append(event)

    def agent_error(self, name: str) -> None:
        """Record an agent error."""
        self._agent(name).errors += 1

    def llm_call(
        self,
        agent_name: str,
        tokens_in: int,
        tokens_out: int,
        cost_usd: float,
        model: str = "",
    ) -> None:
        """Record an LLM call with token usage, cost, and model (v0.9.1, FD-01)."""
        metrics = self._agent(agent_name)
        metrics.tokens_in += tokens_in
        metrics.tokens_out += tokens_out
        metrics.cost_usd += cost_usd
        if model:
            metrics.last_model = model
        self._snapshot.total_cost_usd += cost_usd

    def reset(self) -> None:
        """Reset all metrics."""
        self._snapshot = RuntimeSnapshot()
