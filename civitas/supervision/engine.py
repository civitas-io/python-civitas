"""RestartEngine — restart budgets, intensity windows, and backoff verdicts.

Extracted in v0.9.0 E1 from the duplicated accounting in ``Supervisor`` and
``DynamicSupervisor`` (finding B1). One instance covers one budget scope: a
static ``Supervisor`` holds one engine (supervisor-wide intensity, the OTP
model); a ``DynamicSupervisor`` holds one engine per dynamic child (per-child
budgets, its pre-existing semantics — unchanged).

B3 (ratified, supervision-endgame.md §3): backoff is computed from the
**window occupancy at verdict time**, not from per-child lifetime counters.
Backoff therefore decays naturally once the window empties — previously a
child's 4th-ever crash earned ``base * 2**3`` forever, even weeks later.
Lifetime counters survive only as observability (logs/spans), never as a
backoff input.
"""

from __future__ import annotations

import random
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Literal


class BackoffPolicy(Enum):
    """Delay strategy applied between successive restart attempts."""

    CONSTANT = "CONSTANT"
    LINEAR = "LINEAR"
    EXPONENTIAL = "EXPONENTIAL"


@dataclass(frozen=True)
class RestartVerdict:
    """The engine's ruling on one crash event."""

    action: Literal["restart", "exhausted"]
    delay: float
    """Backoff seconds before restarting (0.0 when exhausted or no backoff)."""
    crashes_in_window: int
    """Window occupancy after recording this crash — the B3 backoff input."""


class RestartEngine:
    """Sliding-window restart budget + backoff calculator.

    ``backoff=None`` disables delays entirely (DynamicSupervisor mode — it has
    never applied backoff to dynamic children, and E1 is a refactor, not a
    behavior change beyond B3).
    """

    def __init__(
        self,
        max_restarts: int = 3,
        restart_window: float = 60.0,
        backoff: BackoffPolicy | None = None,
        backoff_base: float = 1.0,
        backoff_max: float = 60.0,
    ) -> None:
        self.max_restarts = max_restarts
        self.restart_window = restart_window
        self.backoff = backoff
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        # Sliding intensity window — deque for O(1) append/popleft (F03-10).
        self.window: deque[float] = deque()

    def record_crash(self, now: float | None = None) -> RestartVerdict:
        """Record one crash: prune the window, append, and rule.

        More than ``max_restarts`` crashes inside ``restart_window`` seconds
        ⇒ ``exhausted`` (caller escalates / removes). Otherwise ``restart``
        with the backoff delay for the current window occupancy (B3).
        """
        if now is None:
            now = time.time()
        cutoff = now - self.restart_window
        self.window.append(now)
        while self.window and self.window[0] <= cutoff:
            self.window.popleft()

        occupancy = len(self.window)
        if occupancy > self.max_restarts:
            return RestartVerdict(action="exhausted", delay=0.0, crashes_in_window=occupancy)
        return RestartVerdict(
            action="restart",
            delay=self.compute_backoff(occupancy),
            crashes_in_window=occupancy,
        )

    def compute_backoff(self, restart_count: int) -> float:
        """Backoff for the Nth crash (N = window occupancy under B3).

        The formula is unchanged from the pre-E1 implementation; only the
        *input* changed (B3): occupancy instead of a lifetime counter.
        """
        if self.backoff is None:
            return 0.0
        if self.backoff == BackoffPolicy.CONSTANT:
            delay = self.backoff_base
        elif self.backoff == BackoffPolicy.LINEAR:
            delay = self.backoff_base * restart_count
        elif self.backoff == BackoffPolicy.EXPONENTIAL:
            delay = self.backoff_base * (2 ** (restart_count - 1))
            # Add jitter (up to 25%)
            delay += delay * random.random() * 0.25
        else:  # pragma: no cover — enum is exhaustive; defensive default
            delay = self.backoff_base
        return min(delay, self.backoff_max)

    def reset(self) -> None:
        """Fresh incarnation ⇒ fresh budget (the H1 rule).

        Called when a parent restarts the subtree this engine belongs to —
        without it, an exhausted window would instantly re-escalate.
        """
        self.window.clear()
