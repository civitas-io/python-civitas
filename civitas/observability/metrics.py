"""MetricsSink — structural protocol for runtime metrics collection.

AgentProcess and Supervisor report metrics through this protocol so they
never depend on a concrete collector (e.g. the dashboard's MetricsCollector,
or a custom implementation). Any object with matching methods satisfies it.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class MetricsSink(Protocol):
    """Receives runtime events. All methods are synchronous and fire-and-forget."""

    def message_handled(self, agent_name: str, latency_ms: float) -> None:
        """Record that an agent finished handling a message."""
        ...

    def message_sent(self, agent_name: str) -> None:
        """Record that an agent sent a message."""
        ...

    def agent_error(self, agent_name: str) -> None:
        """Record that an agent's handle() raised."""
        ...

    def agent_restarted(self, agent_name: str, reason: str = "") -> None:
        """Record that a supervisor restarted an agent after a crash."""
        ...
