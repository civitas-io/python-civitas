# Dashboard v2 — "civitas top" (v0.9.1)

**Status: DRAFT** — pending sign-off before implementation.

## 1. Problem

`civitas dashboard` (M3.3) starts its own `Runtime` in-process, wires a `MetricsCollector`
directly, and renders with Rich's static `Live`. It cannot attach to a topology already running
elsewhere, has no resource visibility, and its LLM/cost metrics have been silently dead since
M3.3 shipped (FD-01, `docs/milestones.md`: *"`llm_call` is not auto-wired... a real follow-up, not
silently claimed done"*). This is that follow-up, plus a full rebuild on `TopologyServer` (the
remote-attach surface `civitas topology show` already uses) and a move to Textual for real
interactivity — mouse, scrolling, click-to-focus. PRD (agreed in conversation, not reproduced
in full here): P0 for v0.9.1 is topology + per-agent health/LLM-metrics + per-process resources;
P1 (network I/O, session length, history/sparklines, write actions) and P2 (log tail,
multi-cluster) are explicitly deferred to v0.9.2.

## 2. Architecture

```
┌─────────────────────┐        HTTP (poll, N Hz)        ┌──────────────────────────┐
│  civitas dashboard   │ ──────────────────────────────► │   TopologyServer         │
│  (Textual App,       │ ◄────────────────────────────── │   (inside Runtime's OS   │
│   separate process)  │   /topology /agents /metrics     │    process, supervised)  │
└─────────────────────┘        /processes                └──────────┬───────────────┘
                                                                     │ same-process reads
                                                        ┌────────────┼─────────────┐
                                                        │            │             │
                                                  Supervisor    MetricsCollector  psutil
                                                  tree (D6      (agent metrics)   (Runtime's
                                                  introspection)                  own PID)
                                                                     │
                                                          _agency.health_probe (D5, reused)
                                                                     │
                                                                     ▼
                                                        ┌──────────────────────────┐
                                                        │   Worker process(es)     │
                                                        │   psutil (own PID) in    │
                                                        │   the existing health-   │
                                                        │   ack reply             │
                                                        └──────────────────────────┘
```

Same discovery mechanism `civitas topology show` already uses (find the `topology_server` node
in the YAML → host:port), but the dashboard polls **continuously** instead of once, with a
visible "reconnecting…" state instead of one-shot fallback-to-static.

## 3. `TopologyServer` changes

### 3.1 Enrich `/topology` and `/agents` (D-DASH-1)
`_serialize_node`'s `Supervisor` branch and `_build_agents_list`/`_build_agent_detail` currently
emit only `name`/`type`/`status`. Add, for every node, straight off attributes `TopologyServer`
already has a reference to (no bus round-trip — same pattern as Phase C's `_status_snapshot()`):
- `restart_count` (per child, from `sup._restart_counts`)
- `crashes_in_window` (from `sup._engine.window` occupancy, per Supervisor node)
- `capabilities` / `capability_metadata` (from `AgentProcess.capabilities` — this is the "agent
  description" from the PRD; already exists, never surfaced)
- `uptime_seconds` (needs a new `AgentProcess._incarnation_started_at` timestamp, set in
  `_start_nowait()` — does not exist today; a restart's fresh incarnation naturally resets it,
  which is the correct semantic: "uptime" means *this* incarnation's age)

### 3.2 New `GET /metrics` endpoint (D-DASH-2)
Exposes a `MetricsCollector` snapshot. `TopologyServer` needs a reference to one — see §4.
Shape: `{"agents": {name: {messages_handled, messages_sent, avg_latency_ms, restarts, errors,
tokens_in, tokens_out, cost_usd, last_model}}, "total_messages", "total_cost_usd", "uptime_seconds"}`.

### 3.3 New `GET /processes` endpoint (D-DASH-3)
**Reuses the D5 (v0.9.0) per-process health-probe wire protocol rather than inventing a new
one.** Two sources:
- **The Runtime's own process**: `TopologyServer` runs inside it — a local `psutil.Process()`
  self-measurement, no message needed.
- **Each Worker process**: `TopologyServer` finds every distinct `health_channel` in the registry
  (the same field D5 added to `RoutingEntry`) and sends `_agency.health_probe` directly — the
  exact message a Supervisor already sends for liveness. The Worker's existing `_on_health_probe`
  handler gains `cpu_percent` / `rss_bytes` to its `_agency.health_ack` payload (its own
  `psutil.Process()` self-measurement), additive to the existing per-agent snapshot — Supervisors
  ignore fields they don't recognize, so this is a compatible extension, not a wire-format break.

Shape: `{"processes": [{"kind": "runtime"|"worker", "id": ..., "pid", "cpu_percent",
"rss_bytes", "uptime_seconds"}]}`.

## 4. `MetricsCollector` becomes remotely visible (D-DASH-4)

`Runtime.start()` auto-constructs a `MetricsCollector` (if the caller didn't already provide one
via the existing `metrics=` constructor kwarg) whenever the topology contains a
`topology_server` node, wires it via the existing `set_metrics()`/`on_crash()` path (unchanged),
and additionally hands the same reference to the `TopologyServer` instance
(`topology_server._metrics_collector = collector`) during the existing injection pass in
`start()` — the same place `_root_supervisor`/`_agents` already get set on it. Zero behavior
change for anyone not running a `TopologyServer`; existing `Runtime(metrics=my_own_sink)` callers
are unaffected (their sink is used as before; the dashboard-visible snapshot is simply unavailable
if their sink isn't a `MetricsCollector`, which is now explicitly documented rather than assumed).

## 5. Closing FD-01 — `llm_span()` actually feeds the collector (D-DASH-5)

`MetricsSink` (the formal protocol `AgentProcess`/`Supervisor` are written against) gains a new
method:

```python
def llm_call(self, agent_name: str, tokens_in: int, tokens_out: int, cost_usd: float, model: str = "") -> None:
    """Record one LLM call's usage, cost, and model."""
```

This is an **additive protocol change** — worth a CHANGELOG callout since any external
`MetricsSink` implementation (Presidium-side, for example) now needs this method to satisfy the
`@runtime_checkable` protocol at the type level, though `isinstance()` checks at call sites stay
defensive (`if self._metrics is not None: self._metrics.llm_call(...)`, same guard style already
used for `message_handled`).

`llm_span()`'s existing `finally:` block (after `span.end()`) reads back whatever the caller set
via `span.set_attribute("civitas.llm.tokens_in"/"tokens_out"/"cost_usd", ...)` — the convention
`docs/observability.md` already documents, previously read by nobody — and calls
`self._metrics.llm_call(self.name, tokens_in, tokens_out, cost_usd, model=model)` if a sink is
attached and at least one of the three attributes was actually set (an `llm_span()` that never
sets them costs nothing and reports nothing, rather than reporting a spurious all-zero call).
`MetricsCollector.llm_call()` gains the `model` parameter, storing `last_model` per agent.

## 6. Health color model (ratified: option A)

| `ProcessStatus` | Color | Notes |
|---|---|---|
| `RUNNING` | green | |
| `INITIALIZING` / `STOPPING` | yellow | transitional |
| `CRASHED` | red | |
| `SUSPENDED` | grey | covers both governance-pause and HITL-wait — they are the same mechanism today (see PRD discussion); a distinct cyan HITL signal is explicit P1/v0.9.2, not built now |
| `STOPPED` | grey (dim) | terminal, distinct dim shade from SUSPENDED in the actual TUI palette |

Elevated **restart rate** (`crashes_in_window` > 0 while status is `RUNNING`) renders as amber
text within an otherwise-green row — "recovering," not a separate `ProcessStatus`.

## 7. Textual TUI structure

New `civitas/dashboard/app.py` (Textual `App` subclass), replacing `renderer.py`'s Rich-based
rendering (the module is retired, not extended — this is a rebuild, not a patch, per the
milestones item's own framing).

### 7.0 Layout decision (ratified 2026-07): dense three-pane grid

Two layouts were prototyped as real, runnable Textual apps with sample data (not just described)
and compared as rendered screenshots before choosing — source scripts kept at
`.sisyphus/mockups/dashboard-mockup-{a-split,b-grid}.py` (untracked, per `.sisyphus/` convention);
rendered screenshots archived at `docs/assets/dashboard-mockup-{a-split,b-grid}.svg`.

- **[Mockup A — Horizontal split](../assets/dashboard-mockup-a-split.svg)** (tree left ~38%, wide
  detail panel right ~62%, resource stats as a thin footer strip). Pro: more horizontal room for
  the detail table. Con: the resource footer reads as an afterthought, not a first-class panel,
  and the wide detail panel leaves a lot of dead vertical space on a real terminal (height varies
  far more than the 120×40 mockup size did).
- **[Mockup B — Dense three-pane grid](../assets/dashboard-mockup-b-grid.svg)** (tree | detail |
  resources, roughly equal thirds, all visible simultaneously). Closer to btop/dolphie's density;
  treats topology, agent detail, and process resources as three EQUALLY first-class panels,
  matching the PRD's own framing rather than making one the "main" view and the others secondary.

**Ratified: Mockup B (dense three-pane grid) ships in v0.9.1.** Mockup A's core idea — a wider
detail view — is not discarded: deferred to v0.9.2 as an optional **focus/expand mode** (e.g.
pressing Enter on a tree node temporarily widens the detail pane), rather than the default
layout. Tracked in `docs/milestones.md` v0.9.2.

- **`TopologyTree`** (Textual `Tree` widget) — left pane, mouse-clickable nodes, click focuses
  the detail pane on that agent/supervisor.
- **`AgentDetailPanel`** (Textual `DataTable`/`Static` composite) — middle pane: status color,
  capabilities, restart count, crash-window occupancy, uptime, messages/tokens/cost/last-model
  for the focused node.
- **`ProcessResourcePanel`** — right pane (not a footer, per the layout decision above): one row
  per OS process (Runtime + Workers), each with a proportional **colored gauge bar** for CPU%
  and RSS% (single-sample meter, gradient green→amber→red as it fills) alongside the raw
  numbers — this is a *snapshot* visualization, not a history chart (multi-sample time-series
  graphs stay P1/v0.9.2 per the PRD; a proportional bar from one reading is in scope now).
- **Polling worker**: a Textual `@work` background task per endpoint (`/topology`, `/metrics`,
  `/processes`), interval from the existing `--refresh` flag, each independently retried on
  failure with a visible "reconnecting…" banner instead of the whole app dying — mirrors
  `topology show`'s graceful-unreachable framing, but persistent instead of one-shot.
- Mouse support and scrolling are Textual defaults, not custom code — the framework does this.

### 7.1 Visual design language (ratified: rich and colorful, not excessive)

Reference points: **btop** (gradient meter bars, confident color-per-category use),
**dolphie** (a real, widely-used Textual dashboard — proof this aesthetic works cleanly in a
terminal, not just in GUI toolkits), and Textual's own dark-theme design-token system
(`$primary`/`$secondary`/`$success`/`$warning`/`$error`/`$accent`), which the app defines against
rather than hardcoding ANSI colors — free light/dark theme support later, and consistent color
semantics across every widget.

**The guardrail for "not excessive": color always encodes meaning, never decoration.** Concretely:

- **One accent color per data category, used consistently everywhere that category appears:**
  cyan/blue for topology/structure, violet/magenta for LLM+cost metrics, amber/gold for resource
  gauges, and the five status colors from §6 for health — never mixed (an agent's cost figure is
  always violet, never colored by its status; its status dot is always the §6 palette, never
  themed by category).
- **Rounded panel borders** (Textual `border: round`) with a colored title matching that panel's
  category accent — this alone is most of what makes btop/dolphie read as "modern" rather than a
  1980s curses app, and it's near-zero extra code (a CSS property).
- **One glyph vocabulary, reused, not reinvented per-widget** — extends today's Rich renderer's
  existing `_STATUS_DOTS` (`● ◐ ○ ✗ ?`) with color from §6, plus `▲`/`▼` only where a real
  directional signal exists (e.g. cost trending up this session) — never decorative arrows with
  no underlying signal.
- **No blinking, no flashing, no more than the defined palette** — a crashed agent's red is
  attention-grabbing because it's the *only* red in a mostly green/cyan/violet screen, not because
  it animates.
- Textual's CSS is a separate `.tcss` file (`civitas/dashboard/app.tcss`), not inline styles —
  keeps the visual language auditable/editable in one place instead of scattered through widget code.


## 8. New dependencies

`textual` and `psutil`, both under a **new `civitas[dashboard]` extras group** (not core), matching
every other optional-surface precedent in this repo (`civitas[http]`, `civitas[grpc]`, etc.).
`civitas dashboard` fails fast with a `ConfigurationError` + install instructions if the extra
isn't installed, matching `connect_mcp()`'s pattern for `fabrica`.

## 9. CLI change

`civitas dashboard <topology.yaml>` — **YAML-driven discovery only** (ratified), no `--url` flag.
Reuses `_find_topology_server()` from `cli/topology.py` (moved to a shared location, e.g.
`cli/_topology_discovery.py`, since two commands now need it). Refuses with a clear error if the
YAML declares no `topology_server` node — a dashboard needs something to attach to. The
"spawn-my-own-runtime" code path in today's `dashboard.py` is **removed**, not kept as a second
mode — the PRD's whole point is remote attach; keeping both would mean maintaining two mental
models for one command. `--refresh` (seconds) is kept, now meaning "poll interval" instead of
"Rich `Live` refresh rate."

## 10. Testing strategy

- `TopologyServer` endpoint changes: plain unit tests (JSON shape assertions), no Textual needed
  — matches how `/topology`/`/agents` are presumably tested today.
- `MetricsCollector.llm_call()` + `llm_span()` wiring: unit tests asserting a span with
  `civitas.llm.tokens_in` etc. set produces exactly one `llm_call()` invocation with the right
  values; a span that never sets them produces zero calls (no spurious zero-cost entries).
- `psutil`-based process stats: unit tests with `psutil.Process` mocked (no real process
  introspection needed to prove the wiring is correct); one real (un-mocked) smoke test that
  `psutil.Process(os.getpid())` returns sane values, guarding against API drift.
- The `_agency.health_probe`/`_agency.health_ack` extension: extend the existing
  `tests/integration/test_process_liveness.py` real-ZMQ suite (same file D5 already lives in) with
  an assertion that `cpu_percent`/`rss_bytes` arrive in the ack.
- Textual App itself: `textual.testing.Pilot` (bundled test harness) drives simulated key/mouse
  events and asserts rendered state — used for the handful of interaction tests that matter
  (click-to-focus, reconnect-banner-on-failure), not exhaustively for every widget.

## 11. Compatibility & behavior-change ledger

| Change | Kind | Notes |
|---|---|---|
| `MetricsSink.llm_call()` | **Additive protocol change** | external sink implementers need this method now |
| `MetricsCollector.message_handled()`/`message_sent()`/`agent_error()`/`agent_restarted()`/`llm_call()` | **Behavior change** | previously silently ignored an unregistered agent name; now self-register lazily on first event — fixes dynamically-spawned children, has no effect on any existing caller that already registered first |
| `TopologyServer` `/topology`/`/agents` response shape | Additive | new fields, existing fields unchanged |
| New `/metrics`, `/processes` endpoints | Additive | |
| `_agency.health_ack` payload | Additive | new `cpu_percent`/`rss_bytes` fields, existing fields unchanged |
| `civitas dashboard` CLI | **Breaking (CLI only)** | no longer spawns its own Runtime; requires a `topology_server` node in the YAML and a separately-running process to attach to |
| `civitas/dashboard/renderer.py` | Removed | superseded by `civitas/dashboard/app.py` (Textual) |
| New extras: `civitas[dashboard]` | Additive | `textual`, `psutil` |

## 12. Open items carried to implementation (not blocking sign-off)

- ~~Whether `uptime_seconds` needs a new `_incarnation_started_at` field~~ **Resolved (Phase A):**
  confirmed no existing per-incarnation timestamp existed; added `AgentProcess._incarnation_started_at`
  (set fresh in `_start_nowait()`, so D1a's fresh-instance restart resets it automatically — zero
  special-casing needed) + a public `uptime_seconds` property.
- **Cross-platform scope, raised mid-Phase-D-planning (2026-07-24)**: the product targets macOS
  and Windows in addition to Linux, but CI has only ever run `ubuntu-latest` and this whole arc's
  manual verification has only ever covered macOS + Linux (Docker). Fixing full CI coverage is out
  of scope for this design (tracked as its own backlog item, `docs/milestones.md` v0.9.2). What IS
  in scope here: not making it WORSE — Phase D's new tests use `tcp://127.0.0.1:<port>` rather than
  the `ipc://` (Unix-only) pattern 4 existing test files use, and its `psutil` usage is written
  defensively rather than assuming Linux-only behavior. None of this is Windows-VERIFIED (no
  Windows runner available in this environment) — it's Windows-AWARE, which is a real, honest
  distinction worth keeping straight.

### Phase A implementation notes (D-DASH-1, done)

- **`restart_count` attribution is per-child, computed by the PARENT** — a node cannot know its own
  restart count, only the supervisor tracking it can. `_serialize_node` gained a `restart_count`
  parameter the parent supplies for each child it recurses into (root defaults to 0, no parent to
  track it). Caught via a dedicated test (`test_serialize_supervisor_children_get_own_restart_count`)
  proving two children's counts aren't conflated — the bug a naive "sum at the parent" version would
  have introduced (an early draft of this implementation did exactly that before catching it).
- **Found and fixed a pre-existing, unrelated test flake while in this file**: `TestTopologyShowCommand::
  test_show_fallback_when_runtime_not_running` asserted an un-normalized substring against Rich's
  word-wrapped CLI output — passed on macOS, failed on Linux/Docker (narrower default terminal
  width wraps the phrase mid-string). Same class of bug as V1 (v0.8.1, Rich help-text width).
  Confirmed pre-existing (reproduces identically without any Phase A changes) before fixing;
  normalized whitespace before the substring check.

### Phase B implementation notes (D-DASH-2/D-DASH-4, done)

- **`MetricsCollector.register_agent()` is required before any metric records anything** —
  `message_handled()`/`message_sent()`/etc. all silently no-op for an unregistered name (a
  pre-existing `MetricsCollector` behavior, not new). The old CLI `dashboard.py` called
  `register_agent()` manually for every static agent at startup; `Runtime`'s new auto-provisioning
  reproduces this loop. Caught by a real end-to-end test (not assumed) — the first draft of
  `test_topology_server_http_metrics_shape` failed with an empty `agents` dict until this was added.
- **Resolved, same session** (was briefly a documented gap): dynamically-spawned children (via
  `DynamicSupervisor`) aren't known to `all_agents()`'s static snapshot, so a spawn-time
  registration hook looked like the obvious fix — but that chases a moving target for every
  future spawn mechanism too. The actual fix is structural: `MetricsCollector`'s recording methods
  (`message_handled`/`message_sent`/`agent_error`/`agent_restarted`/`llm_call`) now self-register
  via a shared `_agent()` helper (`dict.setdefault`) instead of silently no-op'ing for an unknown
  name. A dynamically-spawned agent is tracked correctly from its FIRST reported event, no matter
  how or when it came to exist — zero new coupling between `DynamicSupervisor` and the collector.
  `register_agent()` is kept as a still-useful *explicit* call for agents you want visible with
  all-zero metrics before their first activity (what the static-registration loop does) — no
  longer a *requirement* for correctness. Deliberate behavior change from the pre-v0.9.1
  "operations on an unregistered agent are silently ignored" contract, verified end-to-end
  (`test_topology_server_http_metrics_includes_dynamically_spawned_agent`: real `DynamicSupervisor`,
  real spawn, real `/metrics` response) — not just at the unit level.
  **Status was never actually part of this gap** — confirmed by tracing every call site of
  `agent_status_changed()`: it was ONLY ever called from the old CLI `dashboard.py`'s manual
  polling loop, never from `Runtime`/`AgentProcess`/`Supervisor`. `TopologyServer`'s `/topology`
  and `/agents` (Phase A) read `agent.status.value` directly off the LIVE tree on every request —
  status for dynamic children was already correct with zero `MetricsCollector` involvement.
- `build_component_set()` captures `self._metrics` **by value**, so the auto-provisioning block
  must run strictly before it — placed at the very top of `start()`, before the `ComponentSet`
  branch, not alongside the later `TopologyServer` reference-injection block (which was the first,
  wrong, instinct — caught by re-reading the existing code path before writing code, not by a
  failing test).

### Phase C implementation notes (D-DASH-5, closes FD-01, done)

- **Metrics reporting had to move OUTSIDE the `if self._tracer is not None:` branch** — the
  original `llm_span()` returned early for the no-tracer case, meaning a dashboard-only setup
  (metrics attached, no OTEL tracer configured) would have silently gotten zero cost/token
  tracking even after this phase, an easy trap to fall into if the restructure had been a
  minimal patch rather than a real look at the control flow. Restructured so both branches
  (tracer / no-tracer) build a real `Span` (the class already "works with or without OTEL" per
  its own docstring) and share one `finally:` block that reports to the metrics sink regardless.
- `has_tracer: bool` is a separate local from `self._tracer is not None` specifically so mypy can
  be told, via one `assert`, that `self._tracer` is non-`None` inside that branch — checking a
  boolean alias doesn't narrow the original attribute's type on its own.
- Considered a defensive `getattr(self._metrics, "llm_call", None)` guard for callers with an old
  custom `MetricsSink` that predates this protocol addition, then rejected it — every other
  `MetricsSink` call site in this file (`message_handled`/`message_sent`/`agent_error`) calls
  directly, trusting the Protocol contract with no such guard. Matching that convention rather
  than introducing a one-off exception for this call site.
- `MetricsCollector.llm_call()`'s `model` parameter only overwrites `last_model` when non-empty —
  a later call that doesn't report a model (or an agent using multiple providers where one call
  doesn't tag it) shouldn't blank out the last known-real value.

### Phase D implementation notes (D-DASH-3, done)

- **`_route_http` had to become `async`** — it was a plain sync dispatch table (no route needed
  real I/O before this phase); `/processes` needs to `await` bus round-trips to remote Workers.
  Every other route stays a synchronous call within the now-async method — unaffected behavior,
  just a signature change (`tests/unit/test_topology_server.py`'s direct `_route_http(...)` calls
  needed `await` added, mechanical).
- **A real deadlock, caught by actually running the integration test, not by review**: the first
  draft of the real-ZMQ end-to-end test used `urllib.request.urlopen()` inside an `async def` test
  — a BLOCKING call that starves the very event loop the `TopologyServer` (client and server share
  one process/loop in this test) needs to run on to answer the request. The test hung until its
  own socket timeout fired, with no useful error pointing at the cause. Root-caused by testing
  `_build_processes()` directly first (proved it returns correctly in milliseconds, ruling out the
  new endpoint logic) before suspecting the HTTP client. Fixed by using the same async
  `asyncio.open_connection()`-based helper `test_topology_server.py`'s own tests already use —
  this codebase had already solved this exact problem once; the fix was to reuse it, not invent a
  second one.
- **A real routing race, also caught by the same test, not assumed away**: `_probe_worker_process`
  only caught `TimeoutError`, but a Worker's health channel can be announced moments AFTER the
  agent it hosts (same startup loop, separate messages) — probing in that narrow window raises
  `MessageRoutingError`, which propagated uncaught through `_build_processes()` and
  `_handle_connection`'s top-level `except Exception: pass`, silently killing the ENTIRE
  `/processes` response (not just the one racing Worker's entry). Fixed by catching
  `MessageRoutingError` alongside `TimeoutError` (both mean "not answerable right now, omit this
  one entry") plus a broad final `except Exception` so one bad channel can never take down every
  other process's data in the same response (F03-7 containment, matching this codebase's existing
  convention for background/reporting paths).
- `psutil` needed a `[[tool.mypy.overrides]] ignore_missing_imports` entry (no bundled type
  stubs), added to the same override list `zmq`/`nats`/etc. already share.

### Post-Phase-D addendum: process_id linkage + restart_history (2026-07-26, done)

Two small, safe, read-only additions agreed during the capability-scope discussion (kept out of
the control-plane/auth-gated group entirely — these add no write surface, no new risk tier):

- **`process_id` on every serialized agent/supervisor node** (`/topology`, `/agents`,
  `/agents/{name}`) — matches one of `/processes`' own `id` fields exactly (a Worker's
  `health_channel` for remote agents, this `TopologyServer`'s own name for everything else),
  so a client can join the two endpoints directly. Verified end-to-end (not just the
  `_process_id_for()` unit contract): a real agent's `/topology` `process_id` is asserted to be
  literally present in a real `/processes` response's set of `id`s.
- **`restart_history` in `/metrics`** — `MetricsCollector.restart_history` (a list of timestamped
  `RestartEvent`s) had existed since M3.3 and was never exposed via this endpoint. Free, already-
  collected data.
- **Found a second dead metrics hook while wiring the second one in** — `agent_restarted()`
  existed on `MetricsSink`/`MetricsCollector` and was fully unit-tested, but was **never called
  from anywhere in `civitas/`** — the exact same shape of gap FD-01 was for `llm_call()` (Phase
  C). The old CLI `dashboard.py` was the only caller, wired manually via `Runtime.on_crash()`.
  Reproduced that wiring in the same auto-provisioning block Phase B added, so `restart_history`
  and per-agent restart-count accuracy work for ANY `TopologyServer`-having `Runtime`, not only
  the (Phase F-removed) standalone CLI path. Caught by writing the real end-to-end test first
  (a genuine crash-restart, not a mock) and watching it fail with an empty list — not assumed
  correct because the field existed in the schema.

### Phase E implementation notes (Textual app itself, done)

`civitas/dashboard/{client.py,palette.py,widgets.py,app.py,app.tcss}`, replacing
`civitas/dashboard/renderer.py` (deleted) and `civitas/cli/dashboard.py` (rewritten to §9's
YAML-driven-discovery-only shape — folded into this phase rather than left broken, since deleting
`renderer.py` made the old CLI non-functional; `_find_topology_server` moved to the shared
`civitas/cli/_topology_discovery.py` §9 already specified). Mockup B's dense three-pane grid
shipped as designed. Six real bugs found by actually running the app (Pilot-driven integration
tests plus a manual smoke run against a live Runtime per §10's mandate), not by review:

- **`$token` markup fails inside `Tree`/`DataTable` content.** Textual's theme-variable syntax
  (`$accent`, `$success`, etc.) only resolves through Textual's own `Content` renderer, which
  `Static.update()` uses — `Tree.add()`/`add_leaf()` and `DataTable`'s cell formatter both use
  plain Rich `Text.from_markup()`, which raises `MarkupError` on an unrecognized `$name`. Fixed by
  reverting `STATUS_COLORS` and the category-accent constants to plain, real Rich color names
  (exactly what the retired `renderer.py` already used) for anything rendered into a Tree label or
  DataTable cell; `$tokens` remain valid and used in `.tcss` and the one `Static`-only banner.
- **`TopologyTree._add_node` silently shadowed `Tree`'s own private `_add_node`.** Same class of
  bug as v0.9.0's D-E4-6 — crashed on construction of every `TopologyTree` with a signature
  mismatch. Renamed to `_add_topology_node`.
- **`ReconnectBanner`'s hidden-by-default state depended entirely on `app.tcss`'s `display: none`**
  and had no Python-level default — correct in the full app, broken in an isolated widget test with
  no app CSS loaded. Set `self.display = False` explicitly in `__init__` too (defense-in-depth, not
  redundant: a widget's own default state shouldn't depend entirely on external CSS being present).
- **A naive "clear the shared reconnect banner on any successful poll" is a real race** with three
  concurrent pollers — a healthy `/metrics` fetch could silently mask an ongoing `/processes`
  outage. Fixed with a `self._failing: set[str]` tracked across all three pollers; the banner only
  clears when the set is empty, and names every currently-failing endpoint otherwise.
- **`@work(exclusive=True)` with no explicit `group=` defaults every decorated method to the SAME
  `"default"` group** — `exclusive=True` cancels the previous worker in a group when a new one
  starts, so all three pollers fought over one group and only the last-called one
  (`_poll_processes`) ever actually ran; `/topology` and `/metrics` silently never polled at all,
  with no exception, nothing visible except a permanently-empty tree. Found by directly inspecting
  `app.workers` in a live run, not by a test assertion (the failure produced no error to catch).
  Fixed with an explicit distinct `group=` per poller.
- **Polling `@work` tasks can outlive `run_test()`'s teardown** and resume into an already-unmounted
  Screen, raising `NoMatches` and crashing the worker (which `run_test()` re-raises, failing the
  test). Fixed with an `App.is_running` check (`_touch_dom()`) before every DOM query inside a
  poller callback or the `_mark_ok`/`_mark_failed` helpers, closing the window instead of adding a
  try/except at every call site.
- **Manual smoke run (§10's mandatory look-at-it pass) against a real live Runtime** found two
  more, non-crashing but real UX defects no automated test would catch: (1) `AgentDetailPanel` and
  `ProcessResourcePanel` (both `Static` subclasses, default `height: auto`) didn't fill their
  column the way `TopologyTree` (a `Tree`, which defaults to filling) did — fixed with explicit
  `height: 1fr` on all three panes in `app.tcss`; (2) a 4th "uptime" column on the resource table
  overflowed a 1/3-width pane at realistic sizes and truncated the mem column — dropped back to
  Mockup B's original 3 columns (process/cpu/mem); uptime remains visible per-agent in the detail
  panel, so no information is lost, just not duplicated.

`civitas/cli/dashboard.py`'s import of the optional `dashboard` extra was moved from
`civitas/cli/__init__.py`'s module-level guarded import into the command function itself, raising
`ConfigurationError` with install instructions on invoke (matching `connect_mcp()`'s established
pattern) — a real UX improvement over the old guard, which hid the whole `dashboard` command from
`--help` if the extra wasn't installed; now the command always appears, only failing when actually
run without the extra. `tests/integration/test_m3_3_dashboard.py::test_dashboard_command_registered`
was updated for the new positional-argument CLI shape (§9's documented, intentional behavior
change) and made robust to `typer`'s genuinely-unpinned (`>=0.12`) version range after a real
Docker/Linux run resolved a newer typer that renders argument metavars differently.

1373/1373 unit+integration green (macOS); all green on Linux (Docker, dashboard + full extras
installed) except the one confirmed pre-existing `.venv`-path environmental failure. mypy/ruff
check/format all clean. Manual smoke run against a real live Runtime confirmed correct rendering,
live data, and click-to-focus end-to-end (screenshot on file, not committed — ephemeral smoke
artifact, not a design record like the Mockup A/B comparison was).

Proceeding to Phase F (CLI — already substantially done as part of this phase's renderer.py
removal) and Phase G (final verification sweep + docs + CHANGELOG + release choreography).
