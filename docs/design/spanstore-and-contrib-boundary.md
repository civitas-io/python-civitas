# Design: `SpanStore` protocol + the contrib boundary rule (B4)

**Status:** Draft — awaiting sign-off
**Supersedes:** `telemetry-native.md` §12 (B4 placement/pluggability addendum)
**Touches:** `python-civitas` (core) and `civitas-contrib`

---

## 1. Context & problem

`SQLiteBackend` (B1) + `SQLiteQueryEngine` (B2) shipped as two classes sharing an
*implicit* schema contract — they can silently drift. A hypothetical
`PostgresBackend` would have to reimplement all three of: `normalize_span()`
(pure telemetry semantics), the write side, and the read side. B4 was deferred
(`telemetry-native.md` §12) with two open questions:

1. **Placement** — does durable telemetry storage belong in core or contrib?
2. **Pluggability** — how do we separate telemetry logic from storage mechanics
   so alternative backends don't reimplement normalization?

This doc resolves both, plus a broader question the placement decision forced:
**what is the actual rule for what lives in `civitas-contrib`?**

## 2. Decisions (signed off)

| # | Decision |
|---|----------|
| D1 | `SpanStore` is a **read+write merged protocol** that **extends `ExportBackend`**. One implementation per backend, so read/write schema cannot drift. |
| D2 | Merge the concrete `SQLiteBackend` + `SQLiteQueryEngine` into one **`SQLiteSpanStore`**, with back-compat aliases for the old names. |
| D3 | Adopt **"needs a third-party runtime dependency"** as *the* contrib boundary rule (replacing the old "touches disk → contrib"). Under it, **`SQLiteSpanStore` stays in core**, and **`SQLiteStateStore` moves from contrib to core** for consistency. |
| D4 | `normalize_span()` becomes **public, stable, documented API** in core — every `SpanStore` impl imports it rather than reimplementing it. |

## 3. The contrib boundary rule (the principle)

> **Core ships mechanism, protocols, and implementations with no third-party
> runtime dependency. `civitas-contrib` ships anything that requires a vendor /
> driver / framework SDK, or that integrates with a specific external product.**

This replaces the old, incidental "does it durably persist to disk → contrib"
rule (which put `SQLiteStateStore` in contrib only because it writes files).

Why the new rule is better:
- **SQLite is stdlib.** `sqlite3` ships with CPython; `aiosqlite` is a tiny
  pure-Python wrapper. A SQLite backend adds no vendor dependency, so core can
  ship a batteries-included durable telemetry/state story out of the box.
- **Postgres/MySQL need drivers** (`asyncpg`/`aiomysql`), OTel needs its SDK,
  LLM plugins need vendor SDKs, framework adapters need the other framework —
  all genuinely third-party. Those stay in contrib.
- The rule is **testable and unambiguous**: "does this impl import a package
  that isn't stdlib or already a core dependency?" If yes → contrib.

Consequences applied in §7 / §8.

## 4. Architecture: `ExportBackend` → `SpanStore`

`ExportBackend` (unchanged) stays the general write contract for *anything that
receives spans* — including write-only exporters that are not queryable.
`SpanStore` extends it for *durable, queryable* stores:

```
ExportBackend (Protocol): export(spans), shutdown()
 ├─ ConsoleBackend            core    (write-only)
 ├─ FanOutBackend             core    (write-only, fan-out)
 ├─ OTelExportBackend         contrib (needs OTel SDK)
 └─ SpanStore (Protocol, extends ExportBackend):
        + cost_over_time(since, until, bucket_seconds)
        + message_rate_over_time(since, until, bucket_seconds)
        + cost_by_agent(since, until) / cost_by_model(since, until)
        + recent_spans(since, until, limit)
        + spans_in_trace(trace_id, since, until)
     ├─ SQLiteSpanStore        core    (stdlib/aiosqlite — via civitas[telemetry])
     ├─ InMemorySpanStore      core    (proof-of-seam + test double; no deps)
     ├─ PostgresSpanStore      contrib (needs asyncpg)   — future, demand-driven
     └─ MySQLSpanStore         contrib (needs aiomysql)  — future, demand-driven
```

`SpanStore` is `@runtime_checkable` (mirrors `StateStore`) so `isinstance()`
checks work for callers that want the query surface only when present.

**`InMemorySpanStore` is built now** (not deferred) precisely because it proves
the protocol is genuinely backend-agnostic *without* waiting for a driver-backed
impl, and it doubles as a fast, dependency-free test double — the same role
`InMemoryStateStore` plays for `StateStore`. The concrete `PostgresSpanStore`
is **deferred (demand-driven)**; the seam makes it a small, isolated add.

## 5. `normalize_span()` as public API

Promoted from an internal helper to blessed public API, re-exported from a
stable path (e.g. `civitas.observability.normalize_span`), documented, and
covered by its own tests as a contract. Its §4 normalization table (in
`telemetry-native.md`) is the spec. Every `SpanStore` impl — in core or contrib
— calls it; none reimplements the SpanData→columns mapping. The promoted-column
*set* it returns becomes part of the `SpanStore` contract.

## 6. Concrete `SQLiteSpanStore` (the merge)

- New `civitas/observability/span_store.py` (or keep `sqlite_backend.py`):
  `SQLiteSpanStore` implements the full `SpanStore` protocol — the current
  `SQLiteBackend` write path **and** the current `SQLiteQueryEngine` read
  methods, over the same window-file machinery, so schema is defined once.
- **Back-compat aliases (non-breaking):** `SQLiteBackend = SQLiteSpanStore` and
  a thin `SQLiteQueryEngine` shim (or alias) kept and marked deprecated, so
  existing `from civitas.observability.sqlite_backend import SQLiteBackend` and
  `... sqlite_query import SQLiteQueryEngine` keep working. Removal is a
  separate, later, explicitly-versioned breaking change — not part of this cut.
- The `SpanStore` protocol + `normalize_span` live in core regardless of
  extras; the concrete `SQLiteSpanStore` stays behind `civitas[telemetry]`
  (it imports `aiosqlite`), exactly as today.

## 7. `SQLiteStateStore` move to core (D3 consequence)

- **Add** `SQLiteStateStore` to core (e.g. `civitas/plugins/sqlite_store.py`).
  It uses stdlib `sqlite3` in a thread executor — **no new core dependency.**
- **Loader repoint:** `plugins/loader.py` `_STORE_TYPES["sqlite"]` →
  `"civitas.plugins.sqlite_store.SQLiteStateStore"` (was the contrib path). YAML
  `type: sqlite` now works **without contrib installed** — a strict improvement.
- **Contrib shim (non-breaking):** `civitas_contrib.plugins.sqlite_store`
  re-exports `SQLiteStateStore` from core with a `DeprecationWarning`, so
  `from civitas_contrib.plugins.sqlite_store import SQLiteStateStore` keeps
  working. Canonical import becomes the core path.
- `postgres_store` / future driver-backed stores **stay in contrib** (they need
  `asyncpg` etc.) — the loader keeps resolving `type: postgres` to contrib.

## 8. Contrib inventory (after this change)

| Category | Contrib now | Future candidates |
|---|---|---|
| LLM provider plugins | anthropic, openai, gemini, mistral, litellm, fiddler | bedrock, vertex, cohere, groq, ollama |
| Framework adapters | crewai, langgraph, openai-sdk | autogen, llamaindex, pydantic-ai, smolagents |
| StateStores (driver-backed) | postgres_store | redis, dynamodb, mongo |
| SpanStores (driver-backed) | — | postgres, mysql, clickhouse |
| External exporters | otel | datadog, honeycomb, prometheus-remote-write |
| Eval exporters | eval/exporters | eval-platform-specific |

**Moves out of contrib → core:** `sqlite_store` (`SQLiteStateStore`).
**Stays in core:** all protocols, `Console`/`FanOut`/`InMemoryStateStore`/
`InMemorySpanStore`/`SQLiteSpanStore`/`SQLiteStateStore`, and `prometheus_export`
(pure stdlib text exposition — no client SDK, consistent with the rule).

## 9. Back-compat & migration summary

Everything in this cut is **additive or aliased** — no breaking change:
- `SQLiteBackend` / `SQLiteQueryEngine` → aliases to the merged store.
- `civitas_contrib...sqlite_store.SQLiteStateStore` → re-export shim (warns).
- YAML `type: sqlite` → keeps working, now core-resolved.
Deprecated aliases/shims get removed in a later, explicitly-versioned major.

## 10. Phased plan & versioning (proposal — ship-as-one vs many is your call)

- **Phase 1 (core, telemetry):** `SpanStore` protocol + `normalize_span` public
  + `InMemorySpanStore` + merge into `SQLiteSpanStore` with aliases. Tests.
- **Phase 2 (core, state):** add `SQLiteStateStore` to core + loader repoint +
  tests (incl. YAML `type: sqlite` without contrib).
- **Phase 3 (contrib):** deprecation shim for `sqlite_store`; docs update; note
  Postgres/MySQL `SpanStore` as enabled-but-not-built.

All additive → a single **minor** release (`v0.11.0`) can carry Phases 1–3, or
they can ship as separate minors. Concrete `PostgresSpanStore` is out of scope
(demand-driven), enabled by the seam.

## 11. Verification plan

- `SpanStore` protocol conformance: run the **same** query test suite against
  both `SQLiteSpanStore` and `InMemorySpanStore` (parametrized) — proves the
  seam is real, not SQLite-shaped.
- `normalize_span` contract tests (each §4 span-kind → expected columns).
- Back-compat: old imports (`SQLiteBackend`, `SQLiteQueryEngine`, contrib
  `SQLiteStateStore`) still work; contrib shim emits `DeprecationWarning`.
- YAML `type: sqlite` state store loads with contrib **absent** (real loader).
- Full suite + ruff + ruff format + mypy; docs anchors; clean Docker install of
  the published package.

## 12. Risks / open questions

- **Cross-repo coordination:** the contrib shim must land *with or after* the
  core add so `civitas_contrib` never imports a symbol core hasn't shipped.
  Sequence: release core `v0.11.0` first, then the contrib release that adds the
  shim depends on `civitas>=0.11.0`.
- **`SQLiteQueryEngine` alias fidelity:** it takes `db_dir`/`window_days` and is
  read-only; the merged store must accept the same construction to alias
  cleanly, or the alias is a thin adapter. Confirm during Phase 1.
- **Two SQLite styles:** `SQLiteSpanStore` uses `aiosqlite`; `SQLiteStateStore`
  uses sync `sqlite3` in an executor. Left as-is (both work); not unified here.
