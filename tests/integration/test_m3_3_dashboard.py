"""M3.3 — Managed Observability Dashboard (Beta) testable criteria.

Tests validate the metrics collector and CLI command registration.
civitas/dashboard/renderer.py (Rich-based) was retired in v0.9.1's Phase E
rebuild -- see tests/integration/test_dashboard_app.py for the Textual app's
own tests, and docs/design/dashboard-v2.md §7 for why this is a rebuild,
not a patch.
"""

from typer.testing import CliRunner

from civitas.cli import app
from civitas.dashboard.collector import MetricsCollector

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
# CLI command registration
# ---------------------------------------------------------------------------


def test_dashboard_command_registered():
    """Dashboard command is accessible via the CLI.

    v0.9.1 (Phase F, design dashboard-v2.md §9): the CLI shape changed from a
    ``--topology`` flag to a required positional argument, and remote-attach
    is now the only mode (no spawn-own-runtime path) — a real, documented
    behavior change, not an accidental one; asserting the NEW shape here.
    """
    runner = CliRunner()
    # COLUMNS forces a wide terminal — without it, Click's help formatter
    # truncates long argument help text to "..." in narrower CI/container
    # environments (the same V1 class of bug as v0.8.1's Rich word-wrap
    # flake). Asserting on "topology" (the actual parameter name, lowercase)
    # rather than an uppercased "TOPOLOGY" metavar — found via a REAL
    # Docker/Linux run with an unpinned, newer typer than this repo's own
    # locked version (typer is ">=0.12" in pyproject.toml, genuinely
    # unpinned): newer typer renders the metavar as "<str>", not the
    # parameter name uppercased, so asserting on the uppercase form is
    # itself version-fragile, not just a display-width fragility.
    result = runner.invoke(app, ["dashboard", "--help"], env={"COLUMNS": "200"})
    assert result.exit_code == 0
    assert "topology" in result.output.lower()
    assert "--refresh" in result.output
