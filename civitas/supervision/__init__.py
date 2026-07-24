"""Internal supervision machinery shared by Supervisor and DynamicSupervisor.

v0.9.0 E1 (design: supervision-endgame.md §3): ONE restart-accounting
implementation behind both supervisor classes. Strategy policy (ONE_FOR_ONE /
ONE_FOR_ALL / REST_FOR_ONE vs. permanent / transient / never) stays with the
classes — this package owns budgets, windows, backoff, and verdicts only.
"""

from civitas.supervision.engine import BackoffPolicy, RestartEngine, RestartVerdict

__all__ = ["BackoffPolicy", "RestartEngine", "RestartVerdict"]
