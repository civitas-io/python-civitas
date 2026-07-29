# Native telemetry storage — B1 (v0.9.3.x, Track B)

**Status: ✅ ACCEPTED and IMPLEMENTED (2026-07-29).** Scoped 2026-07-29 as part of the
v0.9.3.x telemetry split (`docs/milestones.md`): Track A (harden what already half-exists via
Jaeger/Grafana/OTLP) shipped as v0.9.3–v0.9.3.2. This is Track B, capability B1 — a civitas-native,
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
