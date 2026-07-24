"""Property tests for RestartEngine (v0.9.0 E1) — window math + B3 decay."""

from __future__ import annotations

from civitas.supervision.engine import BackoffPolicy, RestartEngine


def test_within_budget_restarts():
    e = RestartEngine(max_restarts=3, restart_window=60.0, backoff=BackoffPolicy.CONSTANT)
    for i in range(3):
        v = e.record_crash(now=100.0 + i)
        assert v.action == "restart"
        assert v.crashes_in_window == i + 1


def test_exceeding_budget_is_exhausted():
    e = RestartEngine(max_restarts=2, restart_window=60.0)
    e.record_crash(now=100.0)
    e.record_crash(now=101.0)
    v = e.record_crash(now=102.0)
    assert v.action == "exhausted"
    assert v.delay == 0.0


def test_window_pruning_forgives_old_crashes():
    e = RestartEngine(max_restarts=2, restart_window=10.0)
    e.record_crash(now=100.0)
    e.record_crash(now=101.0)
    # Both crashes now outside the window — budget is effectively fresh.
    v = e.record_crash(now=200.0)
    assert v.action == "restart"
    assert v.crashes_in_window == 1


def test_b3_backoff_derives_from_window_occupancy():
    """B3: the Nth crash IN THE WINDOW gets base*2^(N-1) — not lifetime count."""
    e = RestartEngine(
        max_restarts=10, restart_window=10.0, backoff=BackoffPolicy.EXPONENTIAL, backoff_base=1.0
    )
    v1 = e.record_crash(now=100.0)
    v2 = e.record_crash(now=101.0)
    v3 = e.record_crash(now=102.0)
    # jitter is up to +25%
    assert 1.0 <= v1.delay <= 1.25
    assert 2.0 <= v2.delay <= 2.5
    assert 4.0 <= v3.delay <= 5.0


def test_b3_backoff_decays_when_window_empties():
    """The headline B3 behavior change: after a quiet period, backoff returns
    to base — previously a 4th lifetime crash earned 8x base forever."""
    e = RestartEngine(
        max_restarts=10, restart_window=10.0, backoff=BackoffPolicy.EXPONENTIAL, backoff_base=1.0
    )
    for i in range(4):
        e.record_crash(now=100.0 + i)
    late = e.record_crash(now=500.0)  # window long empty
    assert late.crashes_in_window == 1
    assert 1.0 <= late.delay <= 1.25  # base again, not 16x


def test_linear_and_constant_policies():
    lin = RestartEngine(max_restarts=9, backoff=BackoffPolicy.LINEAR, backoff_base=2.0)
    assert lin.record_crash(now=1.0).delay == 2.0
    assert lin.record_crash(now=2.0).delay == 4.0
    const = RestartEngine(max_restarts=9, backoff=BackoffPolicy.CONSTANT, backoff_base=3.0)
    assert const.record_crash(now=1.0).delay == 3.0
    assert const.record_crash(now=2.0).delay == 3.0


def test_backoff_capped_at_max():
    e = RestartEngine(
        max_restarts=99,
        restart_window=1000.0,
        backoff=BackoffPolicy.EXPONENTIAL,
        backoff_base=1.0,
        backoff_max=5.0,
    )
    for i in range(10):
        v = e.record_crash(now=float(i))
    assert v.delay == 5.0


def test_no_backoff_mode_is_zero_delay():
    """DynamicSupervisor mode — budgets without delays (pre-E1 parity)."""
    e = RestartEngine(max_restarts=3, backoff=None)
    v = e.record_crash(now=1.0)
    assert v.action == "restart" and v.delay == 0.0


def test_reset_gives_fresh_incarnation_budget():
    """The H1 rule: a restarted subtree must not instantly re-escalate."""
    e = RestartEngine(max_restarts=1, restart_window=60.0)
    e.record_crash(now=100.0)
    assert e.record_crash(now=101.0).action == "exhausted"
    e.reset()
    assert e.record_crash(now=102.0).action == "restart"
