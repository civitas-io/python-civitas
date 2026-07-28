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

    def llm_call(
        self,
        agent_name: str,
        tokens_in: int,
        tokens_out: int,
        cost_usd: float,
        model: str = "",
    ) -> None:
        """Record one LLM call's token usage, cost, and model (v0.9.1, FD-01).

        Called from ``AgentProcess.llm_span()``'s teardown when the caller
        reported at least one of tokens_in/tokens_out/cost_usd via
        ``span.set_attribute("civitas.llm.tokens_in"/"tokens_out"/"cost_usd", ...)``
        — a span that never reports usage produces no call here.
        """
        ...
