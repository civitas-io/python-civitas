"""civitas/dashboard/palette.py — status colors/glyphs + formatting helpers
(v0.9.1, dashboard-v2 Phase E). Mostly a straight port of the retired
renderer.py's helpers plus the new gauge_bar; testing the NEW logic
(gauge_bar's clamping/gradient, status lookups) rather than re-testing
formatting math the old renderer already exercised for years.
"""

from __future__ import annotations

from civitas.dashboard.palette import (
    TOPOLOGY_ACCENT,
    format_bytes,
    gauge_bar,
    status_color,
    status_dot,
    status_markup,
)


def test_status_color_known_and_unknown() -> None:
    # Plain Rich color names, NOT "$tokens" — these render inside Tree/DataTable
    # content, which does not understand Textual's theme-token syntax (see
    # palette.py's STATUS_COLORS docstring for the real bug this fixed).
    assert status_color("RUNNING") == "green"  # case-insensitive
    assert status_color("crashed") == "red"
    assert status_color("suspended") == "grey58"
    assert status_color("something-new") == status_color("unknown")


def test_status_dot_matches_retired_renderer_vocabulary() -> None:
    # §7.1: "extends... not reinvents" — same glyphs the old renderer.py used.
    assert status_dot("running") == "●"
    assert status_dot("crashed") == "✗"


def test_status_markup_combines_color_and_dot() -> None:
    markup = status_markup("running")
    assert "●" in markup
    assert "green" in markup
    assert "RUNNING" in markup


def test_status_color_hitl_approval_is_distinct_from_governance_pause() -> None:
    """v0.9.4 (dashboard-v2.md §6/§18): SUSPENDED alone is grey58 (governance
    pause, or the default/unknown category) -- passing suspend_category=
    "hitl_approval" renders a genuinely distinct color, not the same grey both
    used to share. Deliberately NOT cyan (reserved as TOPOLOGY_ACCENT per
    §7.1's category-color discipline) -- the original PRD note calling for
    cyan here was stale.
    """
    assert status_color("suspended") == "grey58"
    assert status_color("suspended", suspend_category="governance_pause") == "grey58"
    assert status_color("suspended", suspend_category="other") == "grey58"
    hitl_color = status_color("suspended", suspend_category="hitl_approval")
    assert hitl_color == "blue"
    assert hitl_color != "grey58"
    assert hitl_color != TOPOLOGY_ACCENT  # not the reserved topology cyan


def test_status_color_suspend_category_ignored_for_non_suspended_status() -> None:
    """suspend_category is meaningless (and harmless) for any other status --
    e.g. a stale/leftover category value on a RUNNING agent must not leak
    into its color."""
    assert status_color("running", suspend_category="hitl_approval") == "green"


def test_status_markup_hitl_approval_uses_distinct_color() -> None:
    markup = status_markup("suspended", suspend_category="hitl_approval")
    assert "blue" in markup
    assert "SUSPENDED" in markup


def test_gauge_bar_clamps_above_100() -> None:
    """A psutil cpu_percent briefly reporting >100% on a multi-core box is
    real (per-core cumulative), not a bug \u2014 must not crash or overflow the bar."""
    bar = gauge_bar(140.0, width=10)
    assert "█" * 10 in bar  # fully filled, not overflowing
    assert "100.0%" in bar  # displayed value is clamped too, not just the bar


def test_gauge_bar_clamps_below_zero() -> None:
    bar = gauge_bar(-5.0, width=10)
    assert "░" * 10 in bar  # fully empty


def test_gauge_bar_color_gradient() -> None:
    assert "green" in gauge_bar(10.0)
    assert "yellow" in gauge_bar(70.0)
    assert "red" in gauge_bar(95.0)


def test_format_bytes() -> None:
    assert format_bytes(512) == "512B"
    assert format_bytes(2048) == "2.0KB"
    assert format_bytes(5 * 1024 * 1024) == "5.0MB"
