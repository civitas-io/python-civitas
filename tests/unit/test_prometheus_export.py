"""Unit tests for civitas/observability/prometheus_export.py (v0.9.3.1).

Pure-function formatter -- fully testable without a server, a Runtime, or
even a real TopologyServer. See tests/unit/test_topology_server.py's
test_topology_server_http_metrics_is_real_prometheus_text() for the
end-to-end HTTP-level proof, and this module's own docstring for the real
Prometheus-server scrape verification done during development.
"""

from __future__ import annotations

from civitas.dashboard.collector import AgentMetrics, RestartEvent, RuntimeSnapshot
from civitas.observability.prometheus_export import (
    PROMETHEUS_CONTENT_TYPE,
    _escape_label_value,
    _format_value,
    render_prometheus_metrics,
)


def test_prometheus_content_type_is_the_older_universally_compatible_format():
    """text/plain 0.0.4, not OpenMetrics -- no # EOF sentinel required."""
    assert PROMETHEUS_CONTENT_TYPE == "text/plain; version=0.0.4; charset=utf-8"


# ---------------------------------------------------------------------------
# _escape_label_value / _format_value
# ---------------------------------------------------------------------------


def test_escape_label_value_backslash_first():
    """Backslash must be escaped FIRST -- escaping quote/newline before
    backslash would double-escape the backslashes those insert."""
    assert _escape_label_value('a"b') == 'a\\"b'
    assert _escape_label_value("a\\b") == "a\\\\b"
    assert _escape_label_value("a\nb") == "a\\nb"
    assert _escape_label_value('a\\"b') == 'a\\\\\\"b'


def test_format_value_int_and_float():
    assert _format_value(5) == "5"
    assert _format_value(0.0089) == "0.0089"


def test_format_value_special_floats_never_crash_the_parser():
    """A cost/latency computation gone wrong upstream could produce inf/nan
    -- civitas must emit the exact spelling Prometheus's parser requires,
    not Python's repr() ('inf', 'nan') which it would reject."""
    assert _format_value(float("nan")) == "NaN"
    assert _format_value(float("inf")) == "+Inf"
    assert _format_value(float("-inf")) == "-Inf"


# ---------------------------------------------------------------------------
# render_prometheus_metrics
# ---------------------------------------------------------------------------


def _snapshot(**agents: AgentMetrics) -> RuntimeSnapshot:
    snap = RuntimeSnapshot()
    snap.agents.update(agents)
    return snap


def test_empty_snapshot_renders_empty_body():
    """No agents, no started_at -- a valid empty scrape, not an error."""
    assert render_prometheus_metrics(RuntimeSnapshot()) == ""


def test_renders_help_and_type_lines_for_each_family():
    snap = _snapshot(a=AgentMetrics(name="a", messages_handled=3))
    text = render_prometheus_metrics(snap)
    assert "# HELP civitas_messages_handled_total Total messages handled by this agent." in text
    assert "# TYPE civitas_messages_handled_total counter" in text
    assert 'civitas_messages_handled_total{agent="a"} 3' in text


def test_renders_one_sample_per_agent():
    snap = _snapshot(
        a=AgentMetrics(name="a", messages_handled=3),
        b=AgentMetrics(name="b", messages_handled=7),
    )
    text = render_prometheus_metrics(snap)
    assert 'civitas_messages_handled_total{agent="a"} 3' in text
    assert 'civitas_messages_handled_total{agent="b"} 7' in text


def test_latency_exposed_as_sum_and_count_not_a_fake_histogram():
    """AgentMetrics only tracks a running sum + count, not real buckets --
    exposing _sum/_count separately (not a Prometheus 'histogram'/'summary'
    type) is the honest representation of what this data actually supports."""
    snap = _snapshot(a=AgentMetrics(name="a", messages_handled=2, total_latency_ms=150.0))
    text = render_prometheus_metrics(snap)
    assert 'civitas_message_latency_ms_sum{agent="a"} 150.0' in text
    assert 'civitas_message_latency_ms_count{agent="a"} 2' in text
    assert "# TYPE civitas_message_latency_ms_sum counter" in text
    assert "histogram" not in text
    assert "summary" not in text


def test_llm_series_only_emitted_for_agents_that_actually_called_an_llm():
    """Mirrors MetricsCollector.llm_call()'s own established discipline
    (v0.9.1, FD-01 close): a non-LLM agent gets ZERO LLM-related series, not
    an all-zero, empty-model-label entry cluttering every scrape."""
    snap = _snapshot(
        chatty=AgentMetrics(
            name="chatty", tokens_in=100, tokens_out=50, cost_usd=0.01, last_model="gpt-5"
        ),
        silent=AgentMetrics(name="silent", messages_handled=5),  # never called an LLM
    )
    text = render_prometheus_metrics(snap)
    assert 'civitas_llm_cost_usd_total{agent="chatty",model="gpt-5"} 0.01' in text

    def _family_block(name: str) -> str:
        """Lines from '# TYPE {name} ...' up to (not including) the next
        '# HELP' line -- families aren't blank-line-separated in the real
        output, so bound each block by the next family header instead."""
        start = text.index(f"# TYPE {name}")
        rest = text[start:]
        next_help = rest.find("\n# HELP", 1)
        return rest if next_help == -1 else rest[:next_help]

    # silent's name never appears anywhere in an LLM-related family's block
    for family in (
        "civitas_llm_tokens_in_total",
        "civitas_llm_tokens_out_total",
        "civitas_llm_cost_usd_total",
    ):
        assert 'agent="silent"' not in _family_block(family)


def test_agent_status_gauge_shows_only_current_status():
    """1 for the CURRENT status only -- other possible status values are
    simply absent, not asserted as 0 (standard minimal enum pattern)."""
    snap = _snapshot(a=AgentMetrics(name="a", status="RUNNING"))
    text = render_prometheus_metrics(snap)
    assert 'civitas_agent_status{agent="a",status="RUNNING"} 1' in text
    assert "CRASHED" not in text


def test_uptime_only_emitted_when_runtime_actually_started():
    """started_at=None (RuntimeSnapshot's default) means 'never started' --
    must not emit a fake 0-second uptime as if it were real."""
    never_started = RuntimeSnapshot()
    assert "civitas_runtime_uptime_seconds" not in render_prometheus_metrics(never_started)

    started = RuntimeSnapshot(started_at=1000.0)
    assert "civitas_runtime_uptime_seconds" in render_prometheus_metrics(started)


def test_total_messages_and_total_cost_are_deliberately_not_duplicated():
    """Redundant with Prometheus's own sum() over the per-agent series --
    not exposed as a separate runtime-level metric family."""
    snap = _snapshot(a=AgentMetrics(name="a", messages_handled=1))
    snap.total_messages = 1
    snap.total_cost_usd = 0.5
    text = render_prometheus_metrics(snap)
    assert "civitas_total_messages" not in text
    assert "civitas_total_cost_usd" not in text


def test_agent_name_with_special_characters_is_escaped_not_corrupted():
    """A real (if unusual) agent name containing a quote/backslash must not
    corrupt the exposition format for every OTHER metric on the same line."""
    snap = _snapshot(**{'weird"name': AgentMetrics(name='weird"name', messages_handled=1)})
    text = render_prometheus_metrics(snap)
    assert 'civitas_messages_handled_total{agent="weird\\"name"} 1' in text


def test_restart_history_is_not_rendered_here():
    """restart_history is a JSON-only, timeline-shaped field (/snapshot) --
    Prometheus's per-scrape counter model has no equivalent representation
    for it; civitas_agent_restarts_total (a running count) is the
    Prometheus-appropriate analogue, already covered separately."""
    snap = _snapshot(a=AgentMetrics(name="a", restarts=2))
    snap.restart_history.append(RestartEvent(agent_name="a", timestamp=1.0, reason="crash"))
    text = render_prometheus_metrics(snap)
    assert 'civitas_agent_restarts_total{agent="a"} 2' in text
    assert "restart_history" not in text
    assert "crash" not in text


def test_output_is_well_formed_line_by_line():
    """Every non-comment, non-blank line must parse as `name{labels} value`
    or `name value` -- a coarse structural sanity check independent of the
    real-Prometheus-server scrape verification done during development."""
    snap = _snapshot(
        a=AgentMetrics(name="a", messages_handled=1, cost_usd=0.5, tokens_in=10, last_model="m")
    )
    text = render_prometheus_metrics(snap)
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        assert " " in line
        name_and_labels, _, value = line.rpartition(" ")
        assert name_and_labels
        float(value) if value not in ("NaN", "+Inf", "-Inf") else None


def test_ends_with_trailing_newline_when_non_empty():
    snap = _snapshot(a=AgentMetrics(name="a", messages_handled=1))
    text = render_prometheus_metrics(snap)
    assert text.endswith("\n")
    assert not text.endswith("\n\n")
