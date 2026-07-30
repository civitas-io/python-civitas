"""Status colors, glyphs, and formatting helpers for the Textual dashboard.

Extends (not reinvents) the glyph vocabulary the old Rich ``renderer.py`` already
used (``_STATUS_DOTS``) — design doc §7.1's explicit rule: "one glyph vocabulary,
reused, not reinvented per-widget." Colors follow §6's ratified health model
(option A) and §7.1's category-accent rule: health always uses THIS palette,
never a category color, and vice versa.
"""

from __future__ import annotations

import time

# v0.9.1 (design §6, ratified option A): ProcessStatus.value (lowercase) -> color.
# Uses Textual's built-in semantic tokens where they exist so light/dark theme
# switching (Ctrl+P) recolors these automatically — never hardcoded hex.
# IMPORTANT (found while writing Phase E's widget tests, not by inspection):
# these feed Tree.add()/add_leaf() labels and DataTable cell content, BOTH of
# which render via plain Rich Text.from_markup() -- NOT Textual's own Content
# renderer -- so Textual's "$token" theme-variable syntax is NOT valid here
# and raises rich.errors.MarkupError at render time. Only Static.update()
# resolves "$tokens" (via Content); .tcss files always do. Plain, real Rich
# color names (exactly what the retired renderer.py already used) are the
# correct and only choice for tree/table content -- a real rendering bug
# that was caught by a failing test, not a stylistic downgrade.
STATUS_COLORS: dict[str, str] = {
    "running": "green",
    "initializing": "yellow",
    "stopping": "yellow",
    "restarting": "yellow",
    "crashed": "red",
    "suspended": "grey58",
    "stopped": "grey35",
    "unknown": "grey50",
}

# Same glyph set as the retired civitas/dashboard/renderer.py's _STATUS_DOTS —
# carried forward verbatim per §7.1, not redesigned.
STATUS_DOTS: dict[str, str] = {
    "running": "●",
    "initializing": "◐",
    "restarting": "○",
    "stopping": "◐",
    "stopped": "○",
    "crashed": "✗",
    "suspended": "○",
    "unknown": "?",
}

# §7.1 category accent colors — one per data category, used consistently
# everywhere that category appears. Never mixed with STATUS_COLORS above.
# Plain Rich color names, NOT "$tokens" -- same reason as STATUS_COLORS
# above. Chosen to match design §7.1's own category-color NAMES exactly
# (cyan/blue=topology, violet/magenta=LLM+cost, amber/gold=resources), so
# nothing is lost by not using the theme-token indirection for these.
TOPOLOGY_ACCENT = "cyan"
LLM_ACCENT = "magenta"
RESOURCE_ACCENT = "gold3"

# v0.9.4 (dashboard-v2.md §6/§18): the distinct HITL-wait signal the original
# §6 table deferred ("a distinct cyan HITL signal is explicit P1/v0.9.2, not
# built now"). Deliberately NOT cyan -- §7.1's later-ratified rule reserves
# cyan for TOPOLOGY_ACCENT ("health colors never mixed with category colors"),
# so the original PRD note is corrected here rather than followed as written.
HITL_ACCENT = "blue"


def status_color(status: str, suspend_category: str | None = None) -> str:
    """Color for a `ProcessStatus` value (case-insensitive).

    ``suspend_category`` (v0.9.4, additive/optional) distinguishes a HITL
    approval wait from an operational governance pause -- both are otherwise
    the identical SUSPENDED grey. Only takes effect when status is actually
    "suspended"; harmless to pass for any other status (ignored).
    """
    if status.lower() == "suspended" and suspend_category == "hitl_approval":
        return HITL_ACCENT
    return STATUS_COLORS.get(status.lower(), STATUS_COLORS["unknown"])


def status_dot(status: str) -> str:
    """Glyph for a `ProcessStatus` value (case-insensitive)."""
    return STATUS_DOTS.get(status.lower(), STATUS_DOTS["unknown"])


def status_markup(status: str, suspend_category: str | None = None) -> str:
    """Rich markup fragment for a status: colored dot + label, e.g. '[green]● RUNNING[/]'.

    ``suspend_category`` (v0.9.4, additive/optional): see `status_color()`.
    """
    color = status_color(status, suspend_category)
    return f"[{color}]{status_dot(status)} {status.upper()}[/]"


def format_uptime(seconds: float) -> str:
    """Human-readable uptime — same shape as the retired renderer's helper."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = int(minutes // 60)
    mins = minutes % 60
    return f"{hours}h {mins}m"


def format_cost(cost: float) -> str:
    """Format a USD cost, matching the retired renderer's helper."""
    if cost == 0:
        return "-"
    if cost < 0.01:
        return f"${cost:.4f}"
    return f"${cost:.2f}"


def format_timestamp(ts: float | None) -> str:
    """Relative "N ago" formatting, matching the retired renderer's helper."""
    if ts is None:
        return "-"
    ago = time.time() - ts
    if ago < 60:
        return f"{ago:.0f}s ago"
    if ago < 3600:
        return f"{ago / 60:.0f}m ago"
    return f"{ago / 3600:.1f}h ago"


def format_bytes(n: int) -> str:
    """Human-readable byte count (KB/MB/GB), used by the resource panel."""
    value = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.0f}{unit}" if unit == "B" else f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}GB"


def gauge_bar(percent: float, width: int = 10) -> str:
    """A proportional colored gauge bar (§7 resource panel — snapshot, not history).

    Gradient green -> amber -> red as the value fills, per §7.1's btop reference.
    ``percent`` is clamped to [0, 100] defensively (a psutil reading spiking
    briefly above 100% on a multi-core box is real and should not crash rendering).
    """
    pct = max(0.0, min(100.0, percent))
    filled = round((pct / 100.0) * width)
    filled = max(0, min(width, filled))
    bar = "█" * filled + "░" * (width - filled)
    # Plain Rich color names (see STATUS_COLORS' docstring above) -- this
    # renders inside a DataTable cell, which does not understand "$tokens".
    if pct >= 85:
        color = "red"
    elif pct >= 60:
        color = "yellow"
    else:
        color = "green"
    return f"[{color}]{bar}[/] {pct:5.1f}%"
