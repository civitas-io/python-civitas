"""Unit tests for civitas/dashboard/telemetry_time.py (v0.9.3.5, B3)."""

from __future__ import annotations

import time
from datetime import datetime

import pytest

from civitas.dashboard.telemetry_time import DEFAULT_DURATION_SECONDS, TimeRange, parse_since


def test_default_time_range_is_24h():
    tr = TimeRange.default()
    assert tr.duration_seconds == DEFAULT_DURATION_SECONDS
    assert tr.label == "24h"


@pytest.mark.parametrize(
    "code,expected_seconds,expected_label",
    [
        ("h", 3600, "1h"),
        ("d", 86400, "24h"),
        ("w", 7 * 86400, "7d"),
        ("m", 30 * 86400, "30d"),
    ],
)
def test_preset_matches_tui_keybindings(code, expected_seconds, expected_label):
    tr = TimeRange.preset(code)
    assert tr.duration_seconds == expected_seconds
    assert tr.label == expected_label


def test_since_computes_sliding_window_from_now():
    tr = TimeRange(duration_seconds=3600, label="1h")
    now = 1_000_000.0
    assert tr.since(now) == now - 3600


def test_since_uses_fixed_start_when_set_ignoring_duration():
    """A fixed_since range does NOT slide -- since() returns the same
    timestamp regardless of what "now" is passed."""
    tr = TimeRange(fixed_since=500.0, label="2026-01-01")
    assert tr.since(1_000_000.0) == 500.0
    assert tr.since(2_000_000.0) == 500.0


@pytest.mark.parametrize(
    "value,expected_seconds",
    [
        ("1h", 3600),
        ("24h", 24 * 3600),
        ("7d", 7 * 86400),
        ("30d", 30 * 86400),
    ],
)
def test_parse_since_duration_shorthand(value, expected_seconds):
    tr = parse_since(value)
    assert tr.duration_seconds == expected_seconds
    assert tr.fixed_since is None
    assert tr.label == value


def test_parse_since_is_case_insensitive_and_strips_whitespace():
    tr = parse_since("  24H  ")
    assert tr.duration_seconds == 24 * 3600


def test_parse_since_absolute_iso_datetime_is_a_fixed_start():
    tr = parse_since("2026-07-01")
    assert tr.duration_seconds is None
    assert tr.fixed_since == datetime.fromisoformat("2026-07-01").timestamp()
    # Confirmed fixed, not sliding -- see test_since_uses_fixed_start_when_set above.
    assert tr.since(time.time()) == tr.fixed_since


def test_parse_since_rejects_garbage_with_a_clear_error_not_a_silent_fallback():
    with pytest.raises(ValueError, match="Could not parse --since"):
        parse_since("not-a-real-value")
