"""Prometheus text-format exposition for civitas's own runtime metrics.

Hand-rolled (not the `prometheus_client` library) -- deliberately, after
weighing the trade-off: our data shape only ever needs counters and gauges,
never real Prometheus histograms/summaries (``AgentMetrics`` tracks a
running sum + count, not buckets, so raw ``_sum``/``_count`` pairs are
exposed as separate counter families rather than faking the special
quantile machinery real histograms/summaries need, which this data doesn't
support anyway). That keeps the format small enough to get fully correct
by hand -- matching ``TopologyServer``'s own existing hand-rolled-HTTP
style rather than adding a new dependency/extras group for something this
size.

Implements the older, universally-compatible ``text/plain; version=0.0.4``
exposition format (no OpenMetrics ``# EOF`` sentinel required for that
content type -- Prometheus's server has scraped it happily for years).
Label-value escaping and float formatting follow the spec exactly, since
those are precisely the edge cases a library would otherwise cover for
free: https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md

Verified against a REAL local Prometheus server actually scraping this
output successfully (not just eyeballed) -- see docs/milestones.md's
v0.9.3.1 entry.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from civitas.dashboard.collector import RuntimeSnapshot

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def _escape_label_value(value: str) -> str:
    """Escape a label value per the Prometheus text format spec.

    Order matters: backslash must be escaped FIRST, or the backslashes
    inserted for quote/newline escaping would themselves get re-escaped.
    """
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _format_value(value: float) -> str:
    """Format a metric value per spec: plain decimal, or +Inf/-Inf/NaN.

    civitas's own counters are ints or well-behaved floats in practice, but
    a cost/latency computation gone wrong upstream (division by zero, a bad
    provider response) could produce inf/nan -- guard defensively rather
    than emit a value Prometheus's parser would reject outright.
    """
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "+Inf" if value > 0 else "-Inf"
        return repr(value)
    return str(value)


def _sample(name: str, value: float, labels: dict[str, str] | None = None) -> str:
    if labels:
        label_str = ",".join(f'{k}="{_escape_label_value(v)}"' for k, v in labels.items())
        return f"{name}{{{label_str}}} {_format_value(value)}"
    return f"{name} {_format_value(value)}"


def render_prometheus_metrics(snapshot: RuntimeSnapshot) -> str:
    """Render a RuntimeSnapshot as Prometheus text-format exposition.

    Deliberately drops ``total_messages``/``total_cost_usd`` (redundant --
    Prometheus's own ``sum()`` over the per-agent series gives the same
    number) and never fabricates a real histogram/summary type (see module
    docstring). LLM-related families (tokens/cost) are only emitted for
    agents that have actually made at least one LLM call -- mirroring
    ``MetricsCollector.llm_call()``'s own established discipline (v0.9.1,
    FD-01 close: "a span that never reports usage produces ZERO
    llm_call()s") -- rather than emitting an all-zero, empty-model-label
    series for every non-LLM agent in the topology.
    """
    lines: list[str] = []

    def family(name: str, help_text: str, metric_type: str, samples: list[str]) -> None:
        if not samples:
            return
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")
        lines.extend(samples)

    agents = snapshot.agents
    llm_agents = {n: m for n, m in agents.items() if m.tokens_in or m.tokens_out or m.cost_usd}

    family(
        "civitas_messages_handled_total",
        "Total messages handled by this agent.",
        "counter",
        [
            _sample("civitas_messages_handled_total", m.messages_handled, {"agent": n})
            for n, m in agents.items()
        ],
    )
    family(
        "civitas_messages_sent_total",
        "Total messages sent by this agent.",
        "counter",
        [
            _sample("civitas_messages_sent_total", m.messages_sent, {"agent": n})
            for n, m in agents.items()
        ],
    )
    family(
        "civitas_message_latency_ms_sum",
        "Sum of message handling latency in milliseconds (divide by "
        "civitas_message_latency_ms_count for the average over any time window).",
        "counter",
        [
            _sample("civitas_message_latency_ms_sum", m.total_latency_ms, {"agent": n})
            for n, m in agents.items()
        ],
    )
    family(
        "civitas_message_latency_ms_count",
        "Count of messages contributing to civitas_message_latency_ms_sum.",
        "counter",
        [
            _sample("civitas_message_latency_ms_count", m.messages_handled, {"agent": n})
            for n, m in agents.items()
        ],
    )
    family(
        "civitas_agent_errors_total",
        "Total handle() errors for this agent.",
        "counter",
        [_sample("civitas_agent_errors_total", m.errors, {"agent": n}) for n, m in agents.items()],
    )
    family(
        "civitas_agent_restarts_total",
        "Total supervisor-initiated restarts for this agent.",
        "counter",
        [
            _sample("civitas_agent_restarts_total", m.restarts, {"agent": n})
            for n, m in agents.items()
        ],
    )
    family(
        "civitas_llm_tokens_in_total",
        "Total LLM input tokens consumed by this agent.",
        "counter",
        [
            _sample("civitas_llm_tokens_in_total", m.tokens_in, {"agent": n, "model": m.last_model})
            for n, m in llm_agents.items()
        ],
    )
    family(
        "civitas_llm_tokens_out_total",
        "Total LLM output tokens produced by this agent.",
        "counter",
        [
            _sample(
                "civitas_llm_tokens_out_total", m.tokens_out, {"agent": n, "model": m.last_model}
            )
            for n, m in llm_agents.items()
        ],
    )
    family(
        "civitas_llm_cost_usd_total",
        "Total LLM cost in USD attributed to this agent.",
        "counter",
        [
            _sample("civitas_llm_cost_usd_total", m.cost_usd, {"agent": n, "model": m.last_model})
            for n, m in llm_agents.items()
        ],
    )
    family(
        "civitas_agent_status",
        "1 for the agent's current status (standard Prometheus enum pattern -- "
        "other status values for this agent are simply absent, not asserted as 0).",
        "gauge",
        [
            _sample("civitas_agent_status", 1, {"agent": n, "status": m.status})
            for n, m in agents.items()
        ],
    )
    if snapshot.started_at is not None:
        family(
            "civitas_runtime_uptime_seconds",
            "Seconds since this runtime started.",
            "gauge",
            [_sample("civitas_runtime_uptime_seconds", snapshot.uptime_seconds)],
        )

    return "\n".join(lines) + "\n" if lines else ""
