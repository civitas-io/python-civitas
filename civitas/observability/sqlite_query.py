"""Back-compat shim (B4) — the read-side names now live on ``SQLiteSpanStore``.

Track B's read layer originally shipped as a standalone ``SQLiteQueryEngine``
(B2) plus the ``CostBucket``/``MessageRateBucket``/``SpanRecord`` dataclasses.
The B4 refactor merged read+write into one ``SpanStore`` (``SQLiteSpanStore`` in
``sqlite_backend.py``) so the two sides share one schema and can't drift, and
moved the dataclasses to the dependency-free ``span_store`` module.

This module preserves the original import paths:

    from civitas.observability.sqlite_query import (
        SQLiteQueryEngine, CostBucket, MessageRateBucket, SpanRecord,
    )

``SQLiteQueryEngine`` is an alias of ``SQLiteSpanStore`` — its constructor
(``db_dir``/``window_days``) is a subset of the store's, and the read methods
are identical. New code should import ``SQLiteSpanStore`` from
``sqlite_backend`` (or ``SpanStore``/``normalize_span`` from ``span_store``).
These aliases are removed in a later, explicitly-versioned major.
"""

from __future__ import annotations

from civitas.observability.span_store import CostBucket, MessageRateBucket, SpanRecord
from civitas.observability.sqlite_backend import SQLiteSpanStore

# Read-only name, kept for back-compat. SQLiteSpanStore's constructor accepts
# (db_dir, window_days) plus an optional retention_windows, so this aliases
# cleanly for every existing read-only call site.
SQLiteQueryEngine = SQLiteSpanStore

__all__ = ["SQLiteQueryEngine", "CostBucket", "MessageRateBucket", "SpanRecord"]
