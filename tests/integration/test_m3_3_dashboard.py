"""M3.3 — Managed Observability Dashboard (Beta) testable criteria.

Tests validate the metrics collector, renderer components, and CLI command
registration. The live dashboard itself requires a running runtime and is
tested via visual inspection.
"""

from rich.layout import Layout
from rich.table import Table
from rich.tree import Tree
from typer.testing import CliRunner

from civitas.cli import app
from civitas.dashboard.collector import MetricsCollector
from civitas.dashboard.renderer import (
    render_cost_attribution,
    render_dashboard,
    render_message_stats,
    render_restart_history,
    render_supervision_tree,
)

# ---------------------------------------------------------------------------
# MetricsCollector
# ---------------------------------------------------------------------------


def test_collector_register_agent():
    """Registering an agent creates its metrics entry."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    assert "agent_a" in collector.snapshot.agents
    assert collector.snapshot.agents["agent_a"].name == "agent_a"


def test_collector_agent_status():
    """Status changes are tracked."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    collector.agent_status_changed("agent_a", "running")
    assert collector.snapshot.agents["agent_a"].status == "running"


def test_collector_message_handled():
    """Message handling increments counts and accumulates latency."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    collector.message_handled("agent_a", 10.0)
    collector.message_handled("agent_a", 20.0)
    m = collector.snapshot.agents["agent_a"]
    assert m.messages_handled == 2
    assert m.avg_latency_ms == 15.0
    assert collector.snapshot.total_messages == 2


def test_collector_message_sent():
    """Sent message count is tracked separately."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    collector.message_sent("agent_a")
    collector.message_sent("agent_a")
    assert collector.snapshot.agents["agent_a"].messages_sent == 2


def test_collector_agent_restart():
    """Restart events are recorded in agent metrics and history."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    collector.agent_restarted("agent_a", reason="crash")
    m = collector.snapshot.agents["agent_a"]
    assert m.restarts == 1
    assert m.last_restart is not None
    assert len(collector.snapshot.restart_history) == 1
    assert collector.snapshot.restart_history[0].agent_name == "agent_a"
    assert collector.snapshot.restart_history[0].reason == "crash"


def test_collector_agent_error():
    """Error count is tracked."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    collector.agent_error("agent_a")
    collector.agent_error("agent_a")
    assert collector.snapshot.agents["agent_a"].errors == 2


def test_collector_llm_call():
    """LLM calls track tokens and cost per agent."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    collector.register_agent("agent_b")
    collector.llm_call("agent_a", tokens_in=100, tokens_out=50, cost_usd=0.01)
    collector.llm_call("agent_b", tokens_in=200, tokens_out=100, cost_usd=0.02)
    assert collector.snapshot.agents["agent_a"].tokens_in == 100
    assert collector.snapshot.agents["agent_a"].cost_usd == 0.01
    assert collector.snapshot.agents["agent_b"].tokens_out == 100
    assert collector.snapshot.total_cost_usd == 0.03


def test_collector_uptime():
    """Uptime is calculated from runtime start time."""
    collector = MetricsCollector()
    collector.runtime_started()
    assert collector.snapshot.uptime_seconds >= 0
    assert collector.snapshot.started_at is not None


def test_collector_reset():
    """Reset clears all metrics."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    collector.message_handled("agent_a", 10.0)
    collector.reset()
    assert len(collector.snapshot.agents) == 0
    assert collector.snapshot.total_messages == 0


def test_collector_never_registered_agent_self_registers_lazily():
    """v0.9.1 (dashboard-v2 D-DASH addendum): an agent that was NEVER
    explicitly register_agent()'d is tracked correctly from its first
    reported event — this is what makes dynamically-spawned children (never
    known to Runtime's static registration loop) show up in /metrics without
    any spawn-time hook. Deliberate behavior change from the pre-v0.9.1
    'operations on unregistered agents are silently ignored' contract —
    that silent-drop was itself the bug making dynamic children invisible.
    """
    collector = MetricsCollector()
    collector.message_handled("dyn-child", 10.0)
    collector.message_sent("dyn-child")
    collector.agent_error("dyn-child")
    collector.llm_call("dyn-child", 100, 50, 0.01)

    metrics = collector.snapshot.agents["dyn-child"]
    assert metrics.messages_handled == 1
    assert metrics.messages_sent == 1
    assert metrics.errors == 1
    assert metrics.tokens_in == 100
    assert metrics.tokens_out == 50
    assert metrics.cost_usd == 0.01
    assert collector.snapshot.total_messages == 1


# ---------------------------------------------------------------------------
# Renderer — supervision tree
# ---------------------------------------------------------------------------


def test_render_supervision_tree_with_agents():
    """Supervision tree renders all registered agents."""
    collector = MetricsCollector()
    collector.register_agent("worker_a")
    collector.register_agent("worker_b")
    collector.agent_status_changed("worker_a", "running")
    collector.agent_status_changed("worker_b", "running")
    collector.message_handled("worker_a", 5.0)

    tree = render_supervision_tree(collector.snapshot)
    assert isinstance(tree, Tree)


def test_render_supervision_tree_empty():
    """Empty snapshot shows 'no agents' message."""
    collector = MetricsCollector()
    tree = render_supervision_tree(collector.snapshot)
    assert isinstance(tree, Tree)


# ---------------------------------------------------------------------------
# Renderer — message stats
# ---------------------------------------------------------------------------


def test_render_message_stats():
    """Message stats table renders with correct columns."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    collector.message_handled("agent_a", 15.0)
    collector.message_sent("agent_a")
    collector.agent_error("agent_a")

    table = render_message_stats(collector.snapshot)
    assert isinstance(table, Table)
    assert table.title == "Message Flow"


# ---------------------------------------------------------------------------
# Renderer — cost attribution
# ---------------------------------------------------------------------------


def test_render_cost_attribution():
    """Cost attribution table shows per-agent costs."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    collector.llm_call("agent_a", tokens_in=500, tokens_out=200, cost_usd=0.05)

    table = render_cost_attribution(collector.snapshot)
    assert isinstance(table, Table)
    assert table.title == "Cost Attribution"


def test_render_cost_attribution_no_data():
    """Cost table shows placeholder when no LLM calls recorded."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")

    table = render_cost_attribution(collector.snapshot)
    assert isinstance(table, Table)


# ---------------------------------------------------------------------------
# Renderer — restart history
# ---------------------------------------------------------------------------


def test_render_restart_history():
    """Restart history table shows recent events."""
    collector = MetricsCollector()
    collector.register_agent("agent_a")
    collector.agent_restarted("agent_a", reason="unhandled exception")
    collector.agent_restarted("agent_a", reason="timeout")

    table = render_restart_history(collector.snapshot)
    assert isinstance(table, Table)
    assert table.title == "Restart History"


def test_render_restart_history_empty():
    """Empty restart history shows placeholder."""
    collector = MetricsCollector()
    table = render_restart_history(collector.snapshot)
    assert isinstance(table, Table)


# ---------------------------------------------------------------------------
# Renderer — full dashboard layout
# ---------------------------------------------------------------------------


def test_render_dashboard_layout():
    """Full dashboard renders as a Rich Layout with all panels."""
    collector = MetricsCollector()
    collector.runtime_started()
    collector.register_agent("agent_a")
    collector.register_agent("agent_b")
    collector.agent_status_changed("agent_a", "running")
    collector.agent_status_changed("agent_b", "running")
    collector.message_handled("agent_a", 10.0)
    collector.llm_call("agent_a", 100, 50, 0.01)

    layout = render_dashboard(collector)
    assert isinstance(layout, Layout)


# ---------------------------------------------------------------------------
# CLI command registration
# ---------------------------------------------------------------------------


def test_dashboard_command_registered():
    """Dashboard command is accessible via the CLI."""
    runner = CliRunner()
    result = runner.invoke(app, ["dashboard", "--help"])
    assert result.exit_code == 0
    assert "--topology" in result.output
    assert "--refresh" in result.output
