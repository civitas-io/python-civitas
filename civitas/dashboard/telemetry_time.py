"""Time-range parsing shared by ``civitas telemetry``'s CLI flag and its
interactive TUI keybindings (v0.9.3.5, B3).

Two shapes are supported, per-conversation ("support both"):

- A duration shorthand (``1h``, ``24h``, ``7d``, ``30d``) — a SLIDING window,
  recomputed against "now" on every refresh. This is the interactive-preset
  shape (the TUI's h/d/w/m keybindings use exactly this).
- An absolute ISO datetime (``2026-07-01`` or ``2026-07-01T00:00:00``) — a
  FIXED start point that does not slide as time passes; only the window's
  END keeps tracking "now" each refresh.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

_DURATION_RE = re.compile(r"^(\d+)([hdwm])$")
_UNIT_SECONDS = {"h": 3600, "d": 86400, "w": 7 * 86400, "m": 30 * 86400}

DEFAULT_DURATION_SECONDS = 24 * 3600  # "some reasonable default" -- 24h


@dataclass
class TimeRange:
    """Either a sliding duration (``duration_seconds`` set) or a fixed start
    point (``fixed_since`` set) -- never both. ``since(now)`` resolves
    either shape to a concrete timestamp for a given "now"."""

    duration_seconds: float | None = None
    fixed_since: float | None = None
    label: str = "24h"

    def since(self, now: float) -> float:
        if self.fixed_since is not None:
            return self.fixed_since
        return now - (self.duration_seconds or DEFAULT_DURATION_SECONDS)

    def bucket_seconds(self, now: float) -> int:
        """Chart bucket granularity that FOLLOWS the visible window (v0.10.1).

        Without this the queries default to a fixed bucket (1 day for cost),
        so a 1h view collapsed the whole hour into a single point and the
        chart looked dead. Here the bucket scales with the effective span so
        every view shows ~24-90 real aggregate points connected by straight
        lines -- honest data at a sensible density, NOT interpolated frames.

        Tiered to human-round buckets (1m/5m/1h/6h/1d) rather than a raw
        formula. Uses the effective span (``now - since(now)``) so a fixed
        ``--since`` start gets a sensible bucket as its window grows too.
        """
        span = now - self.since(now)
        if span <= 90 * 60:  # <= 90 min -> 1-minute buckets
            return 60
        if span <= 6 * 3600:  # <= 6 h    -> 5-minute buckets
            return 300
        if span <= 36 * 3600:  # <= 36 h  -> hourly buckets
            return 3600
        if span <= 10 * 86400:  # <= 10 d -> 6-hour buckets
            return 6 * 3600
        return 86400  # otherwise -> daily buckets

    @classmethod
    def preset(cls, code: str) -> TimeRange:
        """A named preset -- matches the TUI's h/d/w/m keybindings exactly."""
        seconds = {"h": 3600, "d": 86400, "w": 7 * 86400, "m": 30 * 86400}[code]
        labels = {"h": "1h", "d": "24h", "w": "7d", "m": "30d"}
        return cls(duration_seconds=seconds, label=labels[code])

    @classmethod
    def default(cls) -> TimeRange:
        return cls(duration_seconds=DEFAULT_DURATION_SECONDS, label="24h")


def parse_since(value: str) -> TimeRange:
    """Parse a ``--since`` CLI value into a TimeRange.

    Raises ValueError (not a silent fallback) on a genuinely unparseable
    value -- civitas/cli/telemetry.py turns this into a clear CLI error, not
    a confusing "why is my range wrong" surprise.
    """
    match = _DURATION_RE.match(value.strip().lower())
    if match:
        count, unit = match.groups()
        seconds = int(count) * _UNIT_SECONDS[unit]
        return TimeRange(duration_seconds=seconds, label=value)
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Could not parse --since value {value!r}. "
            "Use a duration shorthand (e.g. '24h', '7d', '30d') or an ISO "
            "datetime (e.g. '2026-07-01')."
        ) from exc
    return TimeRange(fixed_since=dt.timestamp(), label=value)
