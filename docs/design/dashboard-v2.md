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

- **`TopologyTree`** (Textual `Tree` widget) — left pane, mouse-clickable nodes, click focuses
  the detail pane on that agent/supervisor.
- **`AgentDetailPanel`** (Textual `DataTable`/`Static` composite) — right pane: status color,
  capabilities, restart count, crash-window occupancy, uptime, messages/tokens/cost/last-model
  for the focused node.
- **`ProcessResourcePanel`** — footer or side panel: one row per OS process (Runtime + Workers),
  each with a proportional **colored gauge bar** for CPU% and RSS% (single-sample meter,
  gradient green→amber→red as it fills) alongside the raw numbers — this is a *snapshot*
  visualization, not a history chart (multi-sample time-series graphs stay P1/v0.9.2 per
  the PRD; a proportional bar from one reading is in scope now).
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
