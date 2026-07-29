# Native telemetry storage — B1 (v0.9.3.x, Track B)

**Status: ✅ ACCEPTED and IMPLEMENTED (2026-07-29).** Scoped 2026-07-29 as part of the
v0.9.3.x telemetry split (`docs/milestones.md`): Track A (harden what already half-exists via
Jaeger/Grafana/OTLP, capabilities A1-A3) and Track B (B1-B3) were built as six sequential
capabilities and shipped together as a single release, v0.9.3. This is Track B, capability B1 — a civitas-native,
zero-dependency-beyond-`aiosqlite`, cost-focused persistent span store for small/local deployments
that don't want to stand up Jaeger/Tempo just to see "what did this run cost me."

## 1. Problem

Civitas already emits rich OTEL spans (cost, tokens, latency, per-agent/model — see
`docs/observability.md`) but has **no durable store of its own**. A span's only two fates today
are: printed to console and gone, or exported via real OTLP to an *external* backend
(`docs/observability.md` Mode 3, Track A). There is no way to ask "what did my agents spend last
week" without already running Jaeger/Grafana/Tempo — a real cost for the target audience Track B
exists for (small/local deployments).

## 2. Where this plugs in — no protocol changes needed

`civitas/components.py`'s `build_component_set()` already has two mutually exclusive tracing paths
(FD-09, confirmed while reading the code for this design):

- **Path A** (default, no `exporters=` configured): `Tracer` talks to the OpenTelemetry SDK
  directly — this is what Track A's OTLP-to-Jaeger/Grafana story and the A1 trace-linkage fix live
  on.
- **Path B** (`exporters=[...]` configured in topology YAML or passed directly): spans flow
  `SpanQueue → OTELAgent → ExportBackend.export()` instead, batched and async, never blocking the
  message loop (`civitas/observability/span_queue.py`'s own docstring).

**`SQLiteBackend` is a new `ExportBackend` implementation, nothing more** — it plugs into Path B
exactly the way a user's own custom exporter already can, composable with other exporters via the
*already-existing* `FanOutBackend` (e.g. SQLite + a custom OTLP-forwarding exporter, if someone
wants both). No changes to `Tracer`, `MessageBus`, or the wire protocol.

## 3. Time-windowed file sharding (not one growing file)

Decision (2026-07-29, in conversation): rather than one SQLite file with row-level retention
deletes, **one SQLite file per fixed-size time window** — e.g. one file per 30 days. Retention
becomes deleting whole files (`os.remove()`), not `DELETE FROM ... WHERE` + `VACUUM` — simpler,
no fragmentation, no risk of a bad `WHERE` clause corrupting live aggregations, and matches how
log rotation already solves this exact problem elsewhere.

- `window_days: int` (default `30`) — fixed-size windows anchored at the UNIX epoch, not calendar
  months (avoids variable-month-length edge cases for a v1).
- File naming: `civitas_spans_<window-start-date>.db` — the window's *start* date in `YYYY-MM-DD`
  form (human-discoverable — `ls` shows you exactly what's there and when it started), computed as
  `window_index = int(start_time // (window_days * 86400))`, filename derived from
  `datetime.fromtimestamp(window_index * window_days * 86400, tz=UTC)`.
- `SQLiteBackend` keeps an LRU-ish cache of open `aiosqlite` connections keyed by window index —
  most writes land in the current window; a connection to the previous window stays briefly
  reachable for the rare late/out-of-order span near a boundary, then gets closed once evicted.
- Retention: `retention_windows: int` (default `6`, i.e. ~180 days at the default window size) — a
  periodic sweep (piggybacked on `OTELAgent`'s own flush cadence, no new background task) lists
  `civitas_spans_*.db` files in `db_dir`, deletes any whose window is older than
  `retention_windows` windows ago.
- **Deliberately out of scope for B1**: querying *across* multiple window files. SQLite's
  `ATTACH DATABASE` mechanism handles this natively (`ATTACH 'civitas_spans_2026-06-01.db' AS w1;
  SELECT * FROM w1.spans UNION ALL SELECT * FROM main.spans`) and is the natural fit for a small
  number of windows — that's B2's job (the query/aggregation layer), not B1's. B1's job is writing
  correctly-windowed files and exposing a way to enumerate them (`list_window_files()`), not
  querying them.

## 4. Schema — hot fields promoted to real columns (decision: option 1)

Rejected the "dumb writer + JSON1 `json_extract()` at query time" alternative — promoting the
fields B2's stated queries actually need (cost-over-time, message-rate-over-time,
per-agent/per-model breakdowns) to real, indexed columns keeps those queries plain SQL
`GROUP BY`/`SUM()`, no per-row JSON parsing. The full `attributes` dict is *also* kept (as
`attributes_json`) for drill-down fidelity — this isn't an either/or.

```sql
CREATE TABLE spans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    start_time REAL NOT NULL,
    end_time REAL NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    agent_name TEXT,
    llm_model TEXT,
    llm_tokens_in INTEGER,
    llm_tokens_out INTEGER,
    llm_cost_usd REAL,
    attributes_json TEXT NOT NULL
);
CREATE INDEX idx_spans_start_time ON spans(start_time);
CREATE INDEX idx_spans_trace_id ON spans(trace_id);
CREATE INDEX idx_spans_agent_name ON spans(agent_name);
```

### Normalization: which attribute key becomes `agent_name`?

Different span *kinds* (`docs/observability.md`'s own table) use different attribute keys for
"which agent is this about" — there is no single universal key. Normalization rule, checked in
this order (first match wins):

| Span name pattern | `agent_name` source | `llm_*` source |
|---|---|---|
| `civitas.agent.*` (start/stop/handle/retry) | `attributes["civitas.agent.name"]` | — |
| `llm.chat *` | `attributes["civitas.sender"]` if present, else unset | `attributes["llm.model"/"llm.tokens_in"/"llm.tokens_out"/"llm.cost_usd"]` |
| `send *` / `recv *` | `attributes["civitas.sender"]` | — |
| `tool.execute *` | `attributes["civitas.sender"]` if present, else unset | — |
| `supervisor.restart` | `attributes["civitas.child"]` (the restarted child, not the supervisor) | — |
| anything else | unset (`NULL`) — never guessed | — |

A row with `agent_name IS NULL` is not an error — some spans (e.g. driver-issued or malformed)
genuinely have no clear "which agent" answer, and `NULL` (excluded from `GROUP BY agent_name`
naturally) is more honest than a wrong guess.

## 5. Async I/O — `aiosqlite` (decision: use the dependency, not `run_in_executor`)

Python's built-in `sqlite3` is synchronous; calling it directly inside `ExportBackend.export()`
(an `async def`) would block the event loop during disk I/O, defeating `OTELAgent`'s whole "async
export without blocking the message loop" design point. Decision: use `aiosqlite` rather than
hand-rolling `run_in_executor()` wrapping — genuinely a closer call than A2's hand-roll-vs-library
decision (this isn't a small, fully-bounded text-formatting task; it's real concurrent file I/O
with connection lifecycle management across window rollovers, where a well-tested async wrapper is
worth the dependency).

New extras group: `civitas[telemetry]` (matches the `dashboard`/`otel`/`zmq`/`nats` extras
pattern) — **must be added to all three `uv sync` calls in `.github/workflows/ci.yml`**, per the
exact gap that broke CI when the `dashboard` extra was first added (v0.9.1 release, PR #48).

## 6. Public API

```python
from civitas.observability.sqlite_backend import SQLiteBackend

backend = SQLiteBackend(
    db_dir="./civitas_telemetry",  # constructor arg, user-config-driven; reasonable default
    window_days=30,
    retention_windows=6,
)
```

Used exactly like any other `ExportBackend` — via `exporters=[backend]` (or inside a
`FanOutBackend` alongside others) passed to `Runtime`/`Worker`, or declared in topology YAML's
`plugins.exporters:` block.

## 7. Multi-process aggregation — explicitly deferred, not neglected

Each OS process (Runtime + every Worker) builds its own independent `Tracer`/`SpanQueue`/exporter
pipeline — a `SQLiteBackend` configured identically in each would produce **one separate file set
per process**, not a unified store. SQLite cannot safely support concurrent multi-writer access
across real separate OS processes/machines, so "one shared file" isn't a real option.

**B1's scope is single-process.** The sketched (not just "TBD") answer for a future
cross-process capability: reuse civitas's *own message bus*, following the exact precedent of
`_agency.health_probe` (`civitas/worker.py`/`civitas/topology_server.py`) — a well-known message
type any process already sends/receives cross-process, proven for exactly this "funnel data from
every Worker to one collector" shape (that's what `/processes` already does for health data).
A future capability would have every non-collector process forward its `SpanData` batches as a
normal civitas message to a well-known collector agent name; only that one process's
`SQLiteBackend` touches disk. Deliberately **not** OTLP-receiver-based (see decision log below) —
reusing the existing bus needs no new wire protocol, no OTLP-receiver implementation (which would
meaningfully duplicate what a real OTel Collector already does well), staying proportionate to
"one small capability" when it's eventually built.

## 8. Decision log

| # | Question | Decision |
|---|---|---|
| 1 | Schema: promote hot fields or dumb JSON writer? | **Promote** (`agent_name`, `llm_model`, `llm_tokens_in/out`, `llm_cost_usd` as real indexed columns) + keep full `attributes_json` for drill-down |
| 2 | Retention: row-level deletes or file sharding? | **File sharding** — one SQLite file per `window_days`-sized window, retention deletes whole files |
| 3 | Multi-process aggregation now or deferred? | **Deferred**, single-process only for B1; sketched answer (bus-based forwarding, `_agency.health_probe` precedent) recorded for a future capability, not silently dropped |
| 4 | Async I/O: `aiosqlite` or `run_in_executor`? | **`aiosqlite`** — real concurrent file I/O + connection lifecycle across window rollovers is enough complexity to justify the dependency |
| 5 | DB path configuration | **Constructor arg** (`db_dir=`), user-config-driven via topology YAML's `plugins.exporters:` or direct `exporters=[...]`, reasonable default |

## 9. Testing strategy

- Unit: schema creation, normalization table (every span-kind → correct `agent_name`/`llm_*`
  mapping, including the `NULL`-on-no-match case), window-index/filename computation, retention
  sweep (create fake old window files, confirm exactly the right ones get removed).
- Integration: a real `Runtime` with `exporters=[SQLiteBackend(...)]` running real agents,
  confirming rows land correctly in the actual `.db` file (opened and queried directly, not just
  asserting the writer didn't crash) — matching this whole project's "verify against the real
  thing" standard, not just mocking `aiosqlite`.
- A window-rollover test: spans on either side of a `window_days` boundary land in two distinct
  files with the expected names.

## 10. Non-goals for B1

- Cross-window querying (B2).
- Multi-process aggregation (deferred, §7).
- Rollup/pre-aggregation tables (hourly/daily summaries) — raw-row aggregation is adequate at the
  data volumes a "small/local deployment" actually produces; revisit only if real usage shows a
  need.
- A UI (B3) — this ticket produces queryable data, not a way to look at it.

## 11. Implementation notes (2026-07-29)

Shipped as designed (`civitas/observability/sqlite_backend.py`), with one unplanned but important
root-cause fix found while writing the normalization logic (§4): **`AgentProcess.llm_span()`'s
spans (`civitas.llm.chat`) had never carried any agent identity at all**, in either the existing
OTEL/Jaeger export path (Track A, already shipped) or this new storage backend — confirmed by
directly inspecting a real span's attributes, not assumed. Fixed at the root in
`civitas/process.py` by adding `civitas.agent.name` to that span's attributes (the ergonomic API
has `self.name` available; the separate, lower-level `Tracer.start_llm_span()` API does not, and
was left as-is — see the normalization table's updated notes in the module itself).

Also discovered while verifying `Tracer.start_llm_span()`'s real attribute shape: it uses
`llm.model`/`llm.tokens_in`/etc (no `civitas.` prefix, model in the span NAME) — a genuinely
different shape from `civitas.llm.chat`'s `civitas.llm.*` attributes. Both are real, both are used
(the former by examples calling the Tracer directly, the latter by all real agent code including
`examples/dashboard_demo/`) — `normalize_span()` handles both shapes explicitly, matched by exact
span name where the two could otherwise be ambiguous.

Verification: unit tests for every normalization case (all span kinds + the deliberate `NULL`-on-
no-match fallback), window-index/filename round-tripping, and the retention sweep (including the
real edge case of a span written directly into an already-expired window, immediately swept within
the same `export()` call). Integration test with a REAL `Runtime` running real agents
(`exporters=[SQLiteBackend(...)]`), verified by directly querying the actual `.db` file with a
fresh `aiosqlite` connection — not mocked, matching this project's "verify against the real thing"
standard. 1474/1474 unit+integration tests green (macOS); Linux (Docker, full extras including the
new `telemetry` extra) green except the one confirmed pre-existing `test_python_m_agency`
`.venv`-path environmental failure. mypy/ruff check/format clean.

## 12. Addendum (2026-07-29) — two real follow-up questions, deferred and tracked

Two legitimate design questions surfaced in conversation immediately after B1 shipped. Explicit
decision: **document both here and in `docs/milestones.md` (v0.9.3.6, B4) rather than refactor
already-working, already-tested code mid-flight.** Neither is a defect in what shipped — both are
real architectural improvements worth doing deliberately, later, not reactively.

### 12.1 Placement — should `SQLiteBackend` live in `civitas-contrib`, not core?

Two competing precedents exist in this codebase, pointing in different directions:

- **"Persistence backends live in `civitas-contrib`, even SQLite ones."** `SQLiteStateStore`
  (found mislocated in `civitas/plugins/sqlite_store` during v0.9.2's example fixes — the real
  path is `civitas_contrib.plugins.sqlite_store`) lives in contrib alongside `postgres`
  (`asyncpg`). Core `python-civitas` doesn't ship persistent `StateStore` backends at all.
- **"Batteries-included, zero-vendor-dependency exporters live in core."** `ConsoleBackend`/
  `FanOutBackend` already ship in `civitas/observability/export_backend.py` (core) — no vendor SDK
  involved. `civitas-contrib`'s actual `ExportBackend`-shaped extras (`arize`, `langfuse`,
  `braintrust`, `langsmith`, `fiddler`) are all third-party VENDOR eval/observability platforms;
  `civitas-contrib`'s role for exporters specifically appears to be "vendor SDK integration," not
  "a first-party local storage option."

`SQLiteBackend` is an `ExportBackend`, not a `StateStore` — but it durably persists data to disk,
which is the defining trait of the first precedent, not the second. Ratified principle (2026-07-29,
in conversation): **"common/persistence-related elements belong in `civitas-contrib`; feature-
related bits stay in `civitas` core."** Applying it: `SQLiteBackend`'s disk-writing, connection-
lifecycle, and window/retention mechanics are persistence — contrib. The `ExportBackend` protocol,
`FanOutBackend`, `ConsoleBackend`, `Tracer`/`SpanQueue`/`OTELAgent`, and the Prometheus text
formatter (`prometheus_export.py`, which touches no disk/db at all) are feature machinery — stay in
core. This means moving `SQLiteBackend` isn't a partial split of one class; since core can never
depend on contrib (only the reverse), the *entire* class (schema, normalization, and persistence
mechanics together) would need to relocate as one unit, mirroring exactly where `SQLiteStateStore`
already lives.

**Not executed now** — tracked as part of B4 (`docs/milestones.md`).

### 12.2 Pluggability — SQLite is one backend among several

SQLite is unlikely to be the only storage choice users eventually want (Postgres was named
explicitly in conversation). Today's `SQLiteBackend` bundles two genuinely separable concerns into
one class:

1. **Telemetry-specific logic** — `normalize_span()`'s attribute-mapping table, the `spans` schema
   shape (which columns, which types).
2. **Storage mechanics** — SQLite-specific connection handling, window-file sharding, the
   retention sweep.

A future refactor would separate these along a real seam — something like a `SpanStore` protocol
(`insert_spans(normalized_rows)`, backend-specific schema/connection management behind it) that
`SQLiteBackend` and a hypothetical future `PostgresBackend` both implement, with the normalization
logic (§4's table) shared, written once, and reused regardless of which storage backend a user
picks. Explicit intent stated in conversation: "build like a library so others can use the
capability" — civitas-contrib authors should be able to add a new storage backend without
reimplementing span normalization from scratch.

**Not executed now** — tracked as part of B4 (`docs/milestones.md`). The current v0.9.3
implementation remains the correct, working, fully-tested shape until this refactor actually
happens; this section exists so the seam is designed deliberately when it does, not discovered
under pressure from a real second-backend request.

## 13. B2 (shipped as part of v0.9.3) — 4 query methods; more candidates evaluated for later

`SQLiteQueryEngine` (`civitas/observability/sqlite_query.py`) shipped with four methods:
`cost_over_time`, `message_rate_over_time`, `cost_by_agent`, `cost_by_model`. Cross-window queries
(a time range spanning more than one of B1's window files) use SQLite's native `ATTACH DATABASE`
— confirmed working through `aiosqlite` directly, including the trickiest real case: a single time
bucket whose spans landed in two different window files, requiring a double `GROUP BY` (once per
attached file, once in an outer re-aggregation) to merge correctly into one row instead of two.

Explicit decision (2026-07-29): **ship these four now, evaluate more later** rather than try to be
exhaustive up front. Candidate methods identified for future evaluation, not built yet — tracked
here so the list isn't lost, not a commitment to build all of them:

- **Latency percentiles** (p50/p95/p99 message-handling latency, per agent) — `civitas_message_
  latency_ms_sum`/`_count` (A2's Prometheus metrics) only give an average; SQLite has no built-in
  percentile function, would need either an approximation (bucketed histogram counts) or pulling
  raw rows and computing in Python — a real design decision of its own, not a trivial addition.
- **Error rate over time** (bucketed, per agent) — mirrors `message_rate_over_time`'s shape exactly
  but filtered to `status = 'error'`; likely the cheapest of this whole list to add.
- **Restart/crash timeline query** — `civitas_agent_restarts_total` exists in Prometheus (A2), but
  no query method surfaces the underlying restart EVENTS (timestamp + reason) from the spans table
  the way `/snapshot`'s JSON `restart_history` already does for live data; a persisted equivalent
  would need `supervisor.restart` spans specifically.
- **Trace/span drill-down** ("show me every span in trace X") — a much simpler query than the
  aggregate ones above (just `SELECT * FROM spans WHERE trace_id = ?` per attached window), but
  needs its own decision about how much of `attributes_json` to surface and in what shape.
- **Top-N queries** ("top 5 most expensive agents this week", "top 5 slowest tool calls") —
  straightforward `ORDER BY ... LIMIT N` variants of the existing aggregates; low effort, deferred
  only because nothing has asked for them yet.
- **Model comparison over time** (cost/latency trend per model, not just a point-in-time total) —
  a bucketed variant of `cost_by_model`, symmetrical with how `cost_over_time` already buckets by
  agent+model together; could arguably have shipped in this same cut, held back to keep B2's first
  release small.

None of these block B3 (the UI) from starting once there's a reason to — B3 can be built against
today's four methods and grow as new query methods are added, matching this whole project's "small
capability at a time" cadence.

## 14. B3 (shipped as part of v0.9.3) — the Textual TUI

Confirmed empirically before committing to it (not assumed): real charts genuinely render inside a
Textual app, via `textual-plotext` (a real, installable, actively-maintained package wrapping
`plotext`) — verified with a real headless render (`app.export_screenshot()`) showing a correctly-
axis-labeled line chart before any of `civitas telemetry`'s own code was written.

**`civitas telemetry <db-dir>`** launches its own Textual app (`civitas/dashboard/telemetry_app.py`)
— deliberately separate from `civitas top`, not a new tab there: `civitas top` requires a live,
currently-running `TopologyServer` to attach to over HTTP; telemetry data is historical and lives in
a local SQLite directory, readable even when nothing is running at all. Reuses `civitas top`'s
established visual language (`palette.py`) and its periodic-poll-worker pattern (`@work(exclusive=
True, group=...)`), adapted to re-query the local SQLite store instead of polling HTTP endpoints.

**Panels**: `CostChart`/`MessageRateChart` (real `PlotextPlot` line charts, capped at the top 6
series by total value — a real multi-agent/multi-model deployment's full cardinality would make a
terminal legend unreadable well before that), `StatPanel` (total spend/messages/top-agent, plain
text — point-in-time numbers, not a trend, so a chart would be the wrong tool), `CostBreakdownTable`
(a `DataTable`, per-agent + per-model rows), `TimeRangeBar` (shows the active range + preset keys).

**Time range — both supported, per-conversation**: `--since` accepts either a duration shorthand
(`1h`/`24h`/`7d`/`30d`, a SLIDING window recomputed against "now" every refresh) or an absolute ISO
datetime (a FIXED start point; only the window's end keeps tracking "now"). Interactively
switchable in the TUI via the exact same h/d/w/m preset keys, plus `r` for an immediate manual
refresh outside the normal refresh cadence.

**Refresh**: periodic, `--refresh` (default 30s) — reusing `civitas top`'s own `@work`-based
polling precedent turned out NOT to be the hard path originally hedged against; period configurable
via CLI, no separate one-shot-only fallback needed for v1.

**New dependency**: `textual-plotext`, folded into the existing `civitas[telemetry]` extra (not a
separate one) — per-conversation, since the TUI is meaningless without the SQLite store it reads
from anyway.

**Deferred, tracked** (`docs/milestones.md`): the log/event viewer (browsing individual
spans/traces) — needs the "trace/span drill-down" query method from §13's candidate list, not yet
built. Also newly identified and tracked during B3's build, not built now: per-second/per-minute
live tick animation for the charts (today's refresh redraws the whole chart from scratch, which is
correct but not smoothly animated), and a scrollable/paginated view for `CostBreakdownTable` once a
real deployment has enough distinct agents/models to overflow one screen.
