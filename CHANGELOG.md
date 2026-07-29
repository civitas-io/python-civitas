# Changelog

> **Note:** This project was renamed from Agency to Civitas in April 2026.
> Historical entries below refer to the product as "Agency".

All notable changes to this project are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [0.9.3.1] — 2026-07-29

v0.9.3.x's Track A, capability A2: real Prometheus text-format metrics exposition at the
standard `/metrics` scrape path.

### Added

- **Real Prometheus text-format exposition** at `GET /metrics` on `TopologyServer` — the
  standard scrape path, no `metrics_path` override needed in a Prometheus `scrape_configs`
  entry. Hand-rolled (`civitas/observability/prometheus_export.py`), not the `prometheus_client`
  library — the data shape only needs counters and gauges, keeping full spec correctness
  (label-value escaping, `+Inf`/`-Inf`/`NaN` float formatting) achievable without a new
  dependency. Metrics: `civitas_messages_handled_total`/`_sent_total`,
  `civitas_message_latency_ms_sum`/`_count`, `civitas_agent_errors_total`/`_restarts_total`,
  `civitas_llm_tokens_in_total`/`_out_total`/`_cost_usd_total` (only for agents that actually
  called an LLM), `civitas_agent_status`, `civitas_runtime_uptime_seconds`. Verified against a
  real local Prometheus server actually scraping the endpoint and answering PromQL queries, not
  just eyeballed output. See `docs/observability.md`'s new "Prometheus metrics" section for the
  full reference and a Grafana recipe.

### Changed

- **Breaking**: `TopologyServer`'s existing JSON metrics-snapshot endpoint moved from
  `GET /metrics` to `GET /snapshot` to make room for the standard Prometheus path above ("never
  wise to break ecosystem standards in an OSS project"). If you were polling `/metrics` directly
  for civitas's own JSON shape (rather than through `civitas top`, which is updated already),
  update to `/snapshot`.

### Fixed

- **`MetricsCollector.agent_status_changed()` was never called from anywhere in the runtime**,
  present since v0.9.1 but dead on arrival — found live while verifying the new Prometheus route
  against a real scrape, when a plainly-running agent's exposed status came back `"unknown"`
  forever. Fixed by routing every `AgentProcess` status transition through one new choke point
  (`_set_status()` in `civitas/process.py`), guarded so a user-supplied custom `MetricsSink`
  implementing only the required Protocol methods keeps working unchanged.

## [0.9.3] — 2026-07-29

v0.9.3.x's Track A, capability A1 (see `docs/milestones.md` for the full telemetry roadmap
split). A live verification exercise ("does trace continuity survive a real ZMQ/NATS hop")
found something more fundamental than its original framing.

### Fixed

- **OTEL spans never linked to each other at all, even within a single process** — confirmed via
  direct instrumentation, not assumed from reading code. `Tracer._make_span()`
  (`civitas/observability/tracer.py`) called OpenTelemetry's `start_span()` with no `context=`
  parameter, so every span became its own isolated OTEL root trace with a random trace_id and
  `parent_id: null`, regardless of civitas's own correct `trace_id`/`span_id`/`parent_span_id`
  bookkeeping on `Span`/`Message`. A real Jaeger/Grafana/Datadog view (`docs/observability.md`
  Mode 3) would have shown every `send`/`recv`/`llm.chat`/`tool.execute`/etc. span as a
  disconnected single-span "trace" instead of a real request-flow tree — the one thing
  distributed tracing exists to do. Fixed with a new `_otel_parent_context()` helper (the
  standard "extracted remote context" pattern every OTEL propagator uses) plus making OTEL's own
  minted span IDs authoritative for civitas's bookkeeping, synced back onto the outgoing
  `Message` in `MessageBus.route()`/`request()` before serialization so cross-process/
  cross-transport linkage holds too. Verified with a real 2-OS-process ZMQ round trip (not a
  mock or a unit test alone) before landing the regression tests.

## [0.9.2.1] — 2026-07-28

Bugfix release: two real product bugs found while building v0.9.2's examples, both fully
root-caused and fixed, with real regression tests added for the exact gap that let each ship
silently.

### Fixed

- **Message signing + ZMQ/NATS transport: an agent-to-agent `ask()` silently timed out when
  `security.signing.enabled=True`**, even with `allow_unsigned=True`. Root cause: each transport
  (`ZMQTransport`, `NATSTransport`, `InProcessTransport`) held its own private serializer
  reference, captured once at construction; `Runtime.start()`'s signing-activation code only
  swapped the Runtime's and the `MessageBus`'s serializer references, never the transport's own.
  `request()`'s internal reply_to-injection round-trip then deserialized a signed v2 envelope with
  the STALE, non-signing serializer, silently reconstructing a blank message (empty
  `sender`/`correlation_id`) that the receiving agent's reply-routing check then silently
  dropped — no exception anywhere, just a generic 30s timeout. Fixed with a new
  `Transport.set_serializer()` method, called from `Runtime.start()`'s signing-wiring. New
  `tests/integration/test_signed_transport.py` proves a real signed `ask()` completes over both
  real ZMQ and real NATS — the exact gap that let this ship (no existing test had ever exercised
  signing over a real transport with an actual message round trip).
- **`Runtime.from_config()` / `civitas run --topology` (supervisor mode) did not filter
  `process:`-tagged nodes** — it built every node locally regardless, duplicating agents a real
  Worker process also builds for itself (confirmed on
  `deployment/level2_multi_process/run_supervisor.py`). Fixed with a new `process_filter` keyword
  argument on `Runtime.from_config()`/`from_config_dict()` (default `"*"` — build everything,
  completely unchanged behavior for every existing caller; `None` — build only untagged nodes, the
  new correct behavior for `civitas run --topology` without `--process`; a named string — build
  only nodes tagged for that process, matching `Worker`'s own filtering). `civitas/cli/run.py`'s
  supervisor role now uses `process_filter=None`.
- `examples/secured_messaging.py` gained a real, live, signed `ask()` demo (Part 3) now that it
  actually works; `examples/deployment/level2_multi_process/run_supervisor.py` now uses
  `process_filter=None`, matching the fix.

## [0.9.2] — 2026-07-28

Examples completeness: a smoke test proving every example actually runs, 8 new examples for
real, previously-undemonstrated features, and two real product bugs found and tracked (not
papered over) along the way.

### Added

- **Examples smoke test** (`tests/integration/test_examples_smoke.py`) — runs every example that
  needs no external service and asserts a clean exit, in three shapes: run-to-completion,
  long-running-then-signaled, and paired long-running processes. A self-checking test fails CI if
  a future example is added without being tracked. Exists because `examples/dynamic_spawning.py`
  (v0.9.1) shipped with three silently-broken API calls that nothing had ever caught.
- **8 new examples** for real, shipped features that previously had dedicated design docs but zero
  demonstrative code: `non_blocking_spawn.py` (`wait=False` / `spawn_nowait()`),
  `supervision_introspection.py` (`civitas.supervision.status`), `custom_plugin.py` (writing a
  `ModelProvider` from scratch), `streaming_response.py` (bus-native `stream_reply()`/`.stream()`),
  `secured_messaging.py` (Ed25519 message signing), `grpc_gateway.py` (the generic gRPC `Agent`
  service), `gateway_auth.py` (HTTP gateway JWT bearer auth), and `cross_process_spawn/` (a
  `DynamicSupervisor` hosted in a different OS process). Every one verified running end-to-end on
  macOS and Linux, not just written and assumed correct.
- **`examples/README.md`** — a full index of every example in the repo (existing and new), what
  each demonstrates, how to run it, and how to run the smoke test. Linked from the top-level
  `README.md`.

### Fixed

- **`examples/stateful_workflow.py`** imported `SQLiteStateStore` from a module that has never
  existed (`civitas.plugins.sqlite_store`); the real class lives in
  `civitas_contrib.plugins.sqlite_store`. Fixed the import path; the example is excluded from the
  default smoke run (needs `civitas-contrib`, not a core dependency) but is now correct if
  installed.
- **`examples/deployment/level2_multi_process/run_worker.py`** called `Worker.from_config(...)`, a
  classmethod that has never existed on `Worker` (only `Runtime` has one). Fixed to construct
  `Worker(agents=, transport=, zmq_pub_addr=, zmq_sub_addr=)` directly. Also fixed both Level 2
  scripts relying on `examples` being importable as a top-level package from the current working
  directory — true only by accident in an editable dev install, confirmed false in a real
  `pip install civitas[...]` via Docker.
- **`examples/frameworks/langgraph_on_civitas.py`** and **`openai_sdk_on_civitas.py`** imported
  from `civitas.adapters.*`, which does not exist in this repository — real framework adapters
  live in `civitas_contrib.adapters.*`. Fixed the import paths and docstrings.

### Known issues (found this release, tracked for future investigation, not fixed here)

- **`Runtime.from_config()` / `civitas run --topology` (supervisor mode) does not filter
  `process:`-tagged nodes** — it builds every node locally, including ones tagged for a different
  process, duplicating agents a real Worker process also builds for itself. Confirmed on the
  existing `deployment/level2_multi_process` example. `examples/cross_process_spawn/` works around
  this with the same pattern `tests/integration/test_cross_process_spawn.py` already proves
  correct, rather than building on top of the bug.
- **Message signing + ZMQ transport: an agent-to-agent `ask()` round trip silently times out when
  `security.signing.enabled=True`**, even with `allow_unsigned=True` — no existing test exercises
  signing over a real transport with an actual message round trip. `examples/secured_messaging.py`
  demonstrates the underlying `AgentIdentity`/`KeyRegistry`/`MessageSigner` primitives directly
  (proven correct by unit tests) instead of a live signed request/reply.

Both tracked in `docs/milestones.md`.

## [0.9.1] — 2026-07-28

Post-endgame polish: coverage top-ups, and a full Textual TUI rebuild of `civitas dashboard`
("civitas top") that attaches to an already-running topology instead of spawning its own runtime.

### Added

- **`civitas top` — the dashboard is a full Textual TUI rebuild, not a patch** ([design/dashboard-v2.md](docs/design/dashboard-v2.md)) — `civitas dashboard <topology.yaml>` now attaches remotely to an already-running topology's `topology_server` over HTTP and polls it live, instead of spawning its own runtime. Mouse-clickable three-pane layout (supervision tree | agent detail | per-process resources), built-in light/dark theme switching (`Ctrl+P`), and a persistent reconnect banner if the server becomes unreachable. New `GET /metrics` and `GET /processes` endpoints on `TopologyServer`; `/topology` and `/agents` gained `restart_count`, `crashes_in_window`, `capabilities`, `uptime_seconds`, and `process_id` (joins directly to `/processes`' own process IDs). `Runtime.start()` now auto-provisions a `MetricsCollector` whenever a `TopologyServer` is present and no metrics sink was already attached. New `civitas[dashboard]` extra (`textual`, `psutil`). A runnable demo topology ships at `examples/dashboard_demo/`.
- **Coverage top-ups: `process.py` (88%→92%) and `runtime.py` (87%→91%)** — deferred until after v0.9.0's D6/D1a rewiring settled in these exact regions (would have been wasted motion earlier). 22 new tests: `llm_span()`/`tool_span()`'s tracer-present path (span attributes, parent-span resolution, exception-sets-error — previously only the no-tracer branch was tested), `connect_mcp()`'s idempotent-reconnect and fabrica-absent `ConfigurationError` paths, `emit()`/`end_stream()`'s outside-handle guards, `spawn_into()`'s five validation/error paths (self-target, unknown supervisor, non-DynamicSupervisor target, routing/timeout wrapping, error-reply), `AgentProcess.stop()`'s error-reply path, four suspend/resume/despawn checkpoint-failure degraded-durability branches, and `Runtime.start()`'s message-signing wiring for non-InProcess transports (identity load, `KeyRegistry`, `MessageSigner`/`SigningSerializer` swap — previously only YAML parsing into `SecurityConfig` was tested, never the actual wiring). One block documented as an accepted, contrib-gated ceiling matching the `cli/state.py` precedent (v0.8.2 G3): `_build_exporters`' per-kind bodies (arize/langfuse/braintrust/langsmith/fiddler) require `civitas_contrib`, not a core dependency — only the `ConfigurationError` guard is testable in core CI.

### Changed

- **`civitas dashboard`'s CLI shape changed** — the topology YAML is now a required positional argument (was `--topology`, defaulting to `topology.yaml`), and the command only ever attaches remotely (the old spawn-your-own-runtime mode is removed). `--refresh` now means "poll interval" rather than "Rich `Live` refresh rate," but keeps the same flag and default.

### Fixed

- **`llm_span()` now actually feeds the dashboard's metrics collector, closing FD-01** — previously, token/cost/model usage reported via `span.set_attribute("civitas.llm.tokens_in"/...)` was only ever forwarded to a metrics sink when a tracer was also attached; a dashboard-only (no-tracer) setup silently recorded zero cost for every LLM call. Metrics reporting is now independent of tracing — both branches build a real span and share one `finally` that reports usage if the caller set at least one of tokens_in/tokens_out/cost_usd.
- **Two dead metrics hooks, found while wiring the dashboard's new endpoints** — `MetricsCollector.agent_restarted()` was fully implemented and unit-tested but never called from anywhere in `civitas/` (only the now-retired standalone dashboard CLI wired it manually); restart history and per-agent restart counts now populate for any `TopologyServer`-having `Runtime`. `MetricsCollector`'s recording methods also now self-register a name lazily instead of silently no-op'ing for it, fixing dynamically-spawned children (invisible to `Runtime.all_agents()`'s static snapshot) for any spawn mechanism.
- **`examples/dynamic_spawning.py` had three silently-broken API calls** — wrong `spawn()` argument order plus a nonexistent `init_kwargs` parameter, `Runtime(dict)` instead of `Runtime.from_config_dict(dict)`, and a `Message` object passed where a plain payload dict was required by `send()`. The example had zero test coverage (true of every file in `examples/` today — tracked in `docs/milestones.md`), so nothing caught it running broken. Verified end-to-end: exit code 0.

## [0.9.0] — 2026-07-24

Supervision Endgame — closes the entire 2026-07 architecture review (zero xfails remain), makes
remote liveness a per-process concern instead of a per-mailbox one, and makes every Supervisor a
first-class addressable actor.

### Added

- **Supervisors are now addressable actors (D6)** — every `Supervisor` (static or dynamic) is registered and reachable like any other agent, with its own mailbox and message loop. Query one with `runtime.ask("supervisor_name", {}, message_type="civitas.supervision.status")` for a live snapshot: children, their status, restart-window occupancy, and lifetime restart counts (observability only). Internally, crash processing moved off a bespoke queue onto this same mailbox — the restart strategies, backoff, and escalation semantics are unchanged, only how a crash reaches the supervisor changed.

### Changed

- **Suspending a supervisor is now rejected, not silently accepted** — a paused subtree manager is a footgun (it would stop reacting to crashes while still holding children hostage). `await supervisor.suspend(...)` now raises immediately; the `_agency.suspend` message is logged as a WARNING and dropped rather than being silently actioned. Suspend individual agents instead. Plain agents are completely unaffected.
- **`DynamicSupervisor` spawns with `wait=True` no longer block the supervisor's other traffic (B4)** — previously, a slow `on_start()` (a DB connection, a model warm-up) stalled every other spawn/despawn/status request the same `DynamicSupervisor` was asked to serve concurrently, because the wait was inline on its own message loop. The wait now happens off that loop; caller-visible behavior (the reply shape, the `_SPAWN_ASK_TIMEOUT` deadline) is identical — only the head-of-line blocking is gone.
- **Remote liveness is per-process, not per-mailbox (D5)** — supervisors now probe each Worker's transport-level health channel once per interval instead of pinging every agent's mailbox; the ack carries a per-agent snapshot (`status`, `task_alive`, `mailbox_depth` — depth is report-only). Consequences: **a remote agent legitimately busy in a long `handle()` is no longer falsely declared crashed** (the A6 false-positive, now a green regression test end-to-end over real ZMQ), and **a dead remote task is detected within one probe interval** instead of a full heartbeat-starvation cycle (~15 s at defaults). Workers advertise the channel in their announcements; children of pre-v0.9 workers fall back to the legacy per-agent pings (one-minor-version skew tolerance).
- **Restart builds a fresh incarnation (D1a)** — a restarted agent is a NEW object constructed from your original constructor call: `__init__` re-runs, instance variables reset (whatever in-memory corruption caused the crash dies with the old incarnation — "let it crash" now delivers a genuinely fresh heap), `self.state` restores from the last checkpoint, queued mailbox messages carry over in order, registration/capabilities/wiring (llm/tools/store/credentials) are reproduced exactly, and suspended agents restart into SUSPENDED. **Behavior change:** object references held across a restart go stale — route by name (`runtime.get_agent()` returns the current incarnation); holding direct references was already anti-pattern #6. Applies to static supervisors, `DynamicSupervisor` children (spawn-time `config` carries over), and Worker-hosted remote restarts. This closes the final finding (A1) of the 2026-07 architecture review — **the regression harness now has zero expected failures**.
- **Backoff now derives from the restart window, not lifetime counters (B3)** — both supervisor classes delegate restart accounting to one internal `RestartEngine` (previously two duplicated implementations with divergent semantics). Backoff for the Nth crash is computed from the intensity-window occupancy, so it **decays naturally once the window empties** — previously a child's 4th-ever crash earned `base × 2³` forever, even weeks later. Lifetime counters remain in logs/spans as observability only. `DynamicSupervisor` per-child budgets and its no-backoff behavior are unchanged.

## [0.8.2] — 2026-07-24

### Fixed

- **The 13 NATS integration tests now run in CI** — they spawn a real `nats-server` binary, which was never present on runners, so they skipped everywhere since the suite existed (the last remnant of the #39 fail-open class). The CI integration job installs a pinned, checksum-verified release; Docker-verified 14/14 on Linux. Also fixed the test fixture's hardcoded port 14222 (the comment claimed "random") which collided on parallel runs.
- **`civitas init` accepts paths** — `civitas init path/to/proj` previously failed with a confusing "not a valid Python identifier" error because the whole path was validated. Paths now auto-split (parents created, basename identifier-validated); absolute paths and `--dir` combos supported, fully backwards compatible.

### Added

- **CLI coverage top-ups** — deploy 57→88% (multi-process/NATS compose generation, env-file provider detection, process-affinity collection incl. nested + flat formats), topology 70→89% (all validate error classes, special node types, diff transport/plugins sections, dead-live-probe fallback), dashboard arg paths. `cli/state.py` stays at its documented ceiling (~49%) — its misses are contrib-gated store/migrate bodies.

## [0.8.1] — 2026-07-24

### Added

- **CLI unit test suite + honest coverage** ([#42](https://github.com/civitas-io/python-civitas/issues/42)) — the CLI (~1,800 LOC) and `ToolRegistry`/`ModelResponse` are now unit-tested and **measured**: 10 stale coverage-omit entries deleted, `plugins/tools.py` and `plugins/model.py` at 100% (were omitted / 0%), loader at 96%. The headline coverage moved from 91.2% to 87.6% — a *more honest* number over ~900 newly-measured statements, still above the 85% gate.

### Fixed

- **HTTP/3 event handling had never worked** ([#43](https://github.com/civitas-io/python-civitas/issues/43)) — `gateway/h3.py` imported `StreamReset` from `aioquic.h3.events`, where it does not exist in any aioquic within the package's own `>=1.0` requirement (it is a QUIC-layer event) — the handler raised `ImportError` on the first received event. The feature was advertised since M4.x but no test anywhere had ever driven a request through it (the #25 pattern, again caught only when a test finally existed). Fixed the imports, moved stream-reset handling to the QUIC layer where it belongs, removed an unreferenced duplicate protocol class carrying the same latent bug, and added the first-ever HTTP/3 tests: unit coverage of the stream↔ASGI adapter + a real QUIC loopback GET (`aioquic` is now a dev dependency; `h3.py` un-omitted from coverage).
- **`civitas version` reported "0.1.0" in every release** — the version string was hardcoded in `cli/version.py` since M3.1 and never updated; now read from package metadata. Caught by the first-ever CLI unit test (#42).
- **Integration tests now gate CI** ([#39](https://github.com/civitas-io/python-civitas/issues/39)) — the suite had never run in CI and rotted silently: 3 modules were uncollectable since the core/contrib split (~46 tests dead, [#40](https://github.com/civitas-io/python-civitas/issues/40) — revived with contrib `importorskip` guards / core-only fixtures) and the cross-process spawn E2E was failing everywhere unnoticed (#41). New required `Integration tests` job (~12 s); the gate's very first run caught a real environment bug (Rich help-text rendering width).

- **Cross-process spawn: first messages to a freshly-spawned remote child are no longer lost** ([#41](https://github.com/civitas-io/python-civitas/issues/41)) — two ZMQ subscription-propagation races, present since R6 (v0.7.0): the cluster-wide announcement systematically outran the child's topic-subscription propagation to peer PUB sockets (which silently drop unknown topics), so `spawn()`-then-`ask()` timed out — deterministically on macOS (~20 ms window), coin-flip on Linux (~5 ms); and per-request ephemeral reply topics raced their own first use, dropping fast responders' replies inside the responder's PUB socket. Fixed with (1) a **subscription-settle barrier**: `DynamicSupervisor` confirms the child's topic has propagated (probe loopback, `ZMQTransport.wait_subscribed()`) before announcing it — "announced" now means *routable*; and (2) a **stable per-transport reply-topic prefix** subscribed once at startup, eliminating per-request subscription churn and the reply race entirely. Verified 5/5 on macOS and Linux (was 0/5); E2E runtime dropped from 5.8 s timeout to 0.76 s. Root-cause analysis: `docs/design/cross-process-spawn.md` addendum.

## [0.8.0] — 2026-07-23

### Changed

- **Restart state contract enforced: only checkpointed state survives** — on every (re)start, `self.state` is reset before the checkpoint restore, so un-checkpointed in-memory state (including whatever corruption caused a crash) dies with the old incarnation instead of resurrecting into a restart loop. **Behavior change:** agents that relied on un-checkpointed `self.state` accidentally surviving restarts must call `checkpoint()` (the documented contract all along). Durable suspension is unaffected — the suspend marker rides in the checkpoint (restored agents still come up SUSPENDED). Note: durable suspension across restarts requires a StateStore; `Runtime` always injects one.

### Added

- **Docs: "Choosing Your Configuration" guide** ([recipes](https://civitas-io.github.io/python-civitas/recipes/)) — when to use which restart strategy, backoff, transport level, `handle_timeout`, state store, mailbox size, dynamic spawning, suspension, and security tier, with a worked production topology.
- **Docs: coding-agent quick reference + `llms.txt`** ([agents-guide](https://civitas-io.github.io/python-civitas/agents-guide/)) — dense normative rules, decision tables, copy-paste patterns, and debugging signals for AI coding agents building on Civitas; machine-readable index served at `/llms.txt`.
- **Docs: "Delivery semantics & hazards"** ([messaging](https://civitas-io.github.io/python-civitas/messaging/)) — the previously-unwritten contracts: at-most-once delivery, restart state contract, retry ordering, ask-cycle deadlock, backpressure deadlock, cooperative-scheduling bounds, `handle_timeout` limits.
- **Docs: supervision guide** now documents the restart contract table, subtree escalation + serialized crash handling, `handle_timeout`, suspension, and priority heartbeats.
- **`handle_timeout` — opt-in per-message watchdog** — `AgentProcess(..., handle_timeout=N)` (or `agent: {handle_timeout: N}` in topology YAML) bounds each `handle()` call; on expiry a `TimeoutError` flows through the normal `on_error()` path (default ESCALATE → visible crash → supervisor restart), so a hung *async* handler stops being invisible to its supervisor. Default `None` = disabled, zero behavior change. The handle span gains `civitas.handle.timeout=true` for hung-vs-buggy triage. Documented limits: cancellation lands at the current await point; blocking code (`time.sleep`, busy loops) is undetectable — async hangs only.

### Removed

- **`LocalRegistry.register_b64`** — dead API with zero callers (both apparent call sites target `KeyRegistry.register_b64`, the real home for verify keys, which is unchanged). It also silently dropped the key it was given while inserting a routable-but-keyless phantom entry into the routing table ([#34](https://github.com/civitas-io/python-civitas/issues/34)). Never documented, never exported.

### Fixed

- **Docs/site truth sweep** ([#35](https://github.com/civitas-io/python-civitas/issues/35)) — README, getting-started, index, and plugins pages advertised nonexistent `civitas[anthropic|openai|gemini|mistral|litellm]` extras and `civitas.adapters` / `civitas.plugins.anthropic` import paths (providers and adapters live in `civitas-contrib`); all corrected. Five written guide pages (gateway, GenServer, EvalLoop, MCP, streaming) and the security docs were missing from the site nav — now published.
- **`ErrorAction.RETRY` retries in place — FIFO preserved** ([#32](https://github.com/civitas-io/python-civitas/issues/32)) — RETRY used to re-enqueue the failed message at the *back* of the mailbox: per-sender ordering silently broke, retry latency scaled with queue depth, and an agent could block on its own full mailbox. RETRY now re-runs `handle()` immediately with the same message (fresh `handle_timeout` budget per attempt; `max_retries` → escalate unchanged; a STOP arriving mid-retry aborts the loop). **Behavior change:** other messages no longer interleave between attempts — that interleaving was the ordering bug. Backoff belongs in `on_error()` (`await asyncio.sleep(...)` before returning RETRY); for non-blocking deferral use SKIP + re-send.
- **Messages to `'_runtime'` no longer crash the sender** ([#33](https://github.com/civitas-io/python-civitas/issues/33)) — Runtime-initiated messages carry `sender="_runtime"`, so the natural `self.send(message.sender, ...)` follow-up raised `MessageRoutingError` and typically crashed the agent. A sink now absorbs them: WARNING-logged drop for `send()`, immediate error reply for `ask()` (fail-fast instead of a 30 s timeout). Relatedly, **glob patterns no longer match system names**: `broadcast("*")` skips `_runtime` / `_agency.*` endpoints; explicit underscore patterns (`"_agency.*"`) still match.
- **`on_stop()` exceptions during graceful shutdown are contained** ([#27](https://github.com/civitas-io/python-civitas/issues/27)) — a raising `on_stop()` propagated out of the message loop's `finally`, past `_stop()`'s narrow guard, and crashed whatever awaited it — in practice a supervisor stopping multiple children, taking the whole shutdown sequence down. Now logged at ERROR and contained; the agent still reaches STOPPED and shutdown proceeds (mirrors the existing failed-`on_start` guard).

- **Heartbeats now ride the priority channel** ([#31](https://github.com/civitas-io/python-civitas/issues/31)) — liveness probes were sent at priority 0, so a remote agent with a deep mailbox answered them only after its whole backlog (false-positive crash + forced restart under load — a restart-storm amplifier), and a SUSPENDED remote agent (which drains only its priority queue) never saw them at all — a deliberately-paused agent was unconditionally declared crashed after ~15 s at defaults. Heartbeats are now `priority=1`: busy agents ack between messages, suspended agents ack while staying suspended (suspension is a governance state, not a liveness failure). Threshold breaches are handed to the supervisor's serialized crash queue instead of restarting inline, so one child's restart backoff no longer stalls heartbeat monitoring of every other remote child. *Known limit (documented): a single long-running `handle()` still delays acks until the next loop boundary — bounded by the `handle_timeout` watchdog (upcoming), structurally fixed by per-process liveness in v0.9.*

- **Supervisor escalation now restarts the escalated subtree** ([#28](https://github.com/civitas-io/python-civitas/issues/28)) — under `ONE_FOR_ONE`, a child supervisor that exhausted its restart budget escalated to its parent, which then silently restarted **nothing**: the entire subtree stayed dead while the system reported normal operation. `_restart_child` now stops the escalated supervisor, clears its restart budget (a fresh incarnation gets a fresh intensity window — previously the exhausted window survived and any later crash instantly re-escalated), and starts it again.
- **Crash handling is serialized and failed restarts are loud** ([#30](https://github.com/civitas-io/python-civitas/issues/30)) — crash events now flow through one queue per supervisor, drained strictly sequentially (the OTP model). This removes the concurrent-restart races (double `ONE_FOR_ALL` cycles, registry collisions) and the window where crashes arriving during a nested stop/start were dropped. A restart that itself fails is no longer silently swallowed: it is logged at ERROR and escalated to the parent supervisor (or logged as terminal at the root). Stale crash events for an already-replaced child incarnation are skipped (the OTP EXIT-pid-matching analog). Escalation hands off to the parent's crash queue instead of calling into the parent inline.
- **Registration survives crash-restarts intact** ([#29](https://github.com/civitas-io/python-civitas/issues/29)) — every restart path re-registered agents bare, silently dropping `capabilities`, `capability_metadata`, and the address (including YAML `capabilities:` overrides). `send_capable()` / `find_by_capability()` stopped finding an agent after its first crash-restart. All Supervisor restart paths and `Worker._on_restart_command` now snapshot the full `RoutingEntry` and re-register from it.

## [0.7.4] — 2026-07-21

### Security

- **Dependency: `click` bumped 8.3.1 → 8.4.2** ([PYSEC-2026-2132](https://osv.dev/vulnerability/PYSEC-2026-2132)) — Click ≤ 8.3.2 contains a command-injection vulnerability in `click.edit()`. Civitas never calls `click.edit()` (`click` is a transitive dependency via `typer`), so the runtime was **not exploitable**; the bump keeps the zero-known-vulnerability dependency gate (pip-audit `--strict`) green.
- **CI: Semgrep SARIF pipeline restored** — `semgrep/semgrep-action@v1` dropped the `generateSarif` input, so `semgrep.sarif` was silently never produced and SAST findings stopped flowing to GitHub code scanning (a fail-open in the security pipeline; the job stayed green throughout). The Security workflow now invokes the semgrep CLI directly (`semgrep scan --sarif --output semgrep.sarif`) and fails on ERROR-severity findings.

## [0.7.3] — 2026-07-06

### Fixed

- **HTTP mTLS is now functional via a reverse proxy** ([#25](https://github.com/civitas-io/python-civitas/issues/25)) — `require_client_cert` always rejected requests on the uvicorn HTTP path with `401`, even with a valid client certificate, because uvicorn never exposes the certificate from its TLS handshake to the ASGI app, so the DN needed for authorization never arrived. A new opt-in `GatewayConfig.mtls_source="proxy_header"` instead trusts a TLS-terminating reverse proxy's IETF [RFC 9440](https://www.rfc-editor.org/rfc/rfc9440.html) `Client-Cert` header (a base64-DER leaf certificate), decoding it and feeding the subject DN into the **unchanged** `CIVITAS_GATEWAY_MTLS_ALLOWED_DNS` allowlist authorization.
  - **Trust check.** A new required `GatewayConfig.trusted_proxy_cidrs` gates which peer IPs may supply the header; the peer IP is checked before any header parsing, and civitas forces uvicorn's `proxy_headers=False` in this mode so a client-supplied `X-Forwarded-For` cannot spoof it. As a documented trade-off, `GatewayRequest.client_ip` (rate limiting, access logs) becomes the proxy's IP in this mode.
  - **Fail-closed guards.** `proxy_header` mode requires `client_cert_mode="none"`, a non-empty `trusted_proxy_cidrs`, `cryptography` installed, and `require_client_cert` present in `middleware` — each is a loud `ConfigurationError` at config or startup time, never a silently open gateway.
  - **`direct` mode is unchanged** — it remains the default and remains non-functional against uvicorn (a known limitation of that mode, not a regression); existing deployments are unaffected unless they opt into `proxy_header`.
  - Wired through topology YAML as `gateway.auth.mtls.mtls_source` / `trusted_proxy_cidrs`. See [`docs/gateway.md`](docs/gateway.md) for full nginx/Envoy/Traefik proxy examples and [`docs/design/gateway-http-mtls-proxy.md`](docs/design/gateway-http-mtls-proxy.md) for the rationale.

## [0.7.2] — 2026-07-06

### Security

- **Gateway auth now covers the WebSocket and gRPC surfaces** (#17) — JWT (`require_jwt`) and mTLS
  (`client_cert_mode`) auth configured on `HTTPGateway` is now **auto-inherited** by the WS and gRPC
  surfaces, closing a silent gap where those surfaces stayed unauthenticated regardless of the HTTP
  auth config. **This changes runtime behavior for existing deployments** that configure `require_jwt`
  / `client_cert_mode` *and* expose `ws_routes` or `grpc_enabled=True`: those surfaces go from
  silently-open to enforced.
  - **WebSocket (JWT only).** The bearer token is read from a pinned `civitas.bearer.<jwt>`
    `Sec-WebSocket-Protocol` subprotocol and verified **before** `websocket.accept`; a missing/invalid
    token closes the handshake with WS close code `4401`, and the negotiated subprotocol is echoed on a
    successful accept. WS mTLS remains a non-goal pending
    [#25](https://github.com/civitas-io/python-civitas/issues/25).
  - **gRPC (JWT + mTLS).** A `grpc.aio` server interceptor enforces the `authorization: Bearer`
    metadata JWT and, when `client_cert_mode="required"`, transport-level `require_client_auth=True`
    plus the existing `CIVITAS_GATEWAY_MTLS_ALLOWED_DNS` DN allowlist. The `Health` and
    `ServerReflection` services are exempt so probes and reflection clients keep working. Failures map
    to `UNAUTHENTICATED` (bad/missing JWT), `PERMISSION_DENIED` (unlisted DN), and `INTERNAL` (empty
    allowlist misconfig).
  - **New startup validations.** `client_cert_mode="optional"` is rejected when `grpc_enabled=True`
    (grpc.aio has no `CERT_OPTIONAL` equivalent — `ConfigurationError`); enforcing JWT over a plaintext
    (non-TLS) gRPC port is refused (`ConfigurationError`, since the token would travel in cleartext);
    and an mTLS-only gateway with `ws_routes` logs a startup warning that its WS routes are
    unauthenticated.
  See [`docs/design/gateway-ws-grpc-auth.md`](docs/design/gateway-ws-grpc-auth.md).

## [0.7.1] — 2026-07-05

### Added

- **Bus-native streaming** (v0.7.x · R7) — `AgentProcess.stream(recipient, payload, ...)` returns an
  async iterator over another agent's streamed chunks: the consumer counterpart to the existing
  `stream_reply()` / `emit()` producer API, working across in-process, ZMQ, and NATS with **no transport
  change**. Chunks are demultiplexed on the receive path (so consuming inside `handle()` never deadlocks
  and stream traffic bypasses the business mailbox). Adds a shared `civitas.streaming.StreamSink`, typed
  errors (`StreamError`, `SlowConsumerError`, `StreamInterrupted`, `StreamTimeout`), an optional
  `Message.seq` for ordering / gap detection, cooperative cancellation with a producer-side
  `max_frames` / `max_duration` cap, sender verification, and reserved `civitas.stream.*` message types.
  See [`docs/design/bus-native-streaming.md`](docs/design/bus-native-streaming.md).

## [0.7.0] — 2026-07-05

### Added

- **Per-agent spawn quotas** (v0.7.0 · R5) — `DynamicSupervisor` gains `max_children_per_spawner` and
  `max_total_spawns_per_spawner`, capping a single spawner's concurrent and lifetime children in addition
  to the supervisor-wide `max_children` / `max_total_spawns`. Quotas key on the spawner identity (each
  spawner has an independent budget; lifetime counts are never refunded), configurable per
  `dynamic_supervisor` topology node. Default `None` is unbounded per spawner.
- **Cross-process dynamic spawn** (v0.7.0 · R6) — `DynamicSupervisor` children can now be spawned in
  a *different* OS process over ZMQ/NATS, not just in the supervisor's own process. Cross-process spawn
  is simply `spawn_into(<supervisor hosted in a Worker>)`: a `type: dynamic_supervisor` topology node may
  carry `process: worker`, and the Worker hosts it with the full distributed `ComponentSet` so children it
  spawns are wired (`llm`/`tools`/`store`) and registered cluster-wide. No new public API and the
  in-process spawn path is unchanged (no announcement on `InProcessTransport`). Homogeneous deployments
  only — the spawned class must be importable on the Worker (no code distribution). Hardened per Oracle
  review (D8–D14):
  - **Cluster-wide announcement, after-start, signed, epoched** — once a child reaches RUNNING (never at
    request time), the supervisor publishes a signed `_agency.register` carrying `{name, capabilities,
    capability_metadata, pubkey, epoch}`; on termination it publishes `_agency.deregister {name, epoch}`.
    A monotonic incarnation `epoch` stops a reordered late announcement from resurrecting a dead name.
  - **Authenticated receive + name ownership** — `Runtime._on_remote_register` verifies the announcement
    signature against the trusted key set (dropping unsigned/unknown-signer announcements when signing is
    on) and `LocalRegistry.register_remote`/`deregister_remote` reject a name owned locally, by a different
    remote owner, or with a conflicting public key (no last-writer-wins takeover), and ignore stale epochs.
    A verified child public key is recorded in the `KeyRegistry` so peers can verify the child's messages.
  - **Per-incarnation child identity** — with signing enabled each spawn mints a fresh `AgentIdentity` for
    the child on the Worker and announces its public key; a compromised Worker can never impersonate a
    child elsewhere (no shared per-name private key).
  - **Spawn idempotency + termination authority** — `spawn_into` carries a unique `spawn_id`; a retried
    request for the same `(name, spawn_id)` returns the existing child instead of double-spawning. The
    Worker-side supervisor owns termination; the original spawner treats a child `_agency.deregister` as
    authoritative termination alongside `civitas.dynamic.terminated`.
  - **Per-spawn audit** — the `dynamic.spawn` audit event gains `distributed` and `pubkey` fields for
    cross-process spawns (in-process events are unchanged).
  See [`docs/design/cross-process-spawn.md`](docs/design/cross-process-spawn.md).
- **Encrypted StateStore at rest** (v0.7.0 · R4) — new `EncryptingStateStore`
  (`civitas.EncryptingStateStore`, opt-in `pip install 'civitas[encryption]'`) wraps any
  `StateStore` and transparently encrypts persisted *values* with ChaCha20-Poly1305 (AEAD), leaving
  agent *names* in the clear so `list_agents()` keeps working. The agent name is bound as AEAD
  associated data, so an envelope written for one agent fails to decrypt under another. Values are
  stored as a one-key envelope dict `{"__civitas_enc__": base64(version‖key_id‖nonce‖ciphertext)}`
  (`__civitas_enc__` is a reserved top-level key). Key is a base64 32-byte value from
  `CIVITAS_STATE_KEY` (via `SecretStr`); a missing/short key fails loud with `ConfigurationError`
  and `cryptography` is lazily imported. Supports **key rotation** (a `{key_id: key}` ring decrypts
  any past key while new writes use the current one) and fail-loud tamper/wrong-key detection (new
  `StateDecryptionError`, referencing only the agent name + `key_id`, never contents). Legacy
  plaintext is **strict by default** (raises) with an opt-in `allow_plaintext_read` dual-read for
  gradual migration. Wire it in topology YAML with `plugins.state: {type: encrypted, config: {store:
  {type: sqlite, config: {...}}, allow_plaintext_read: false}}`. `StateStore` is now also exported
  from `civitas`. `civitas state list` renders encrypted values as `<encrypted>`. See
  [`docs/design/encrypted-statestore.md`](docs/design/encrypted-statestore.md).
- **Gateway JWT + mTLS auth** (v0.7.0 · R3) — two first-party, opt-in, secure-by-default gateway
  middleware alongside the existing API-key auth:
  - **JWT bearer verification** (`civitas.gateway.jwt_auth.require_jwt`, opt-in `pip install
    'civitas[jwt]'`) — verifies `Authorization: Bearer` tokens against a `JwtVerifier` built once,
    eagerly, at gateway `on_start` (a missing PyJWT or misconfiguration crashes startup, not the first
    request). Secure by default: explicit `algorithms` (default `["RS256"]`), `exp`/`iss`/`aud`
    **required** (a token without `exp` never expires otherwise) and verified, `alg=none` and RS/HS
    algorithm confusion rejected, `jku`/`x5c`/`kid` header keys never fetched, tokens size-capped
    (~8 KB), bounded leeway, and the blocking JWKS lookup offloaded via `asyncio.to_thread`. Supports
    a JWKS URL (`https://` only) or a static key (exactly one source). Config via `CIVITAS_JWT_JWKS_URL`
    / `CIVITAS_JWT_AUDIENCE` / `CIVITAS_JWT_ISSUER` / `CIVITAS_JWT_ALGORITHMS` /
    `CIVITAS_JWT_PUBLIC_KEY` / `CIVITAS_JWT_SECRET`.
  - **mTLS client-cert authorization** (`civitas.gateway.mtls.require_client_cert`) — authorizes on
    the client certificate's **full subject DN, exact match** (never a CN substring) against
    `CIVITAS_GATEWAY_MTLS_ALLOWED_DNS` (semicolon-separated). Fail-closed: no cert → 401, unlisted DN →
    403, unconfigured allowlist → 500. New `GatewayConfig.tls_ca_cert` / `client_cert_mode`
    (`none`/`optional`/`required`) wire uvicorn's `ssl_ca_certs` + `ssl_cert_reqs`; the CA **must** be
    a dedicated private CA. `GatewayRequest.client_cert` is populated at the ASGI edge from the TLS
    extension, and verified identity from either middleware is attached to the new
    `GatewayRequest.auth` (authN feeding authZ) rather than the dispatched payload.
  - **Security boundary (documented, not covered):** R3 auth applies to HTTP request routes only —
    WebSocket routes, the gRPC surface, and `/docs` + `/openapi.json` bypass the middleware chain.
    `/docs` now defaults to **off** whenever any gateway auth (API-key/JWT/mTLS middleware or
    `client_cert_mode != "none"`) is configured, unless `docs_enabled` is explicitly set. mTLS over
    HTTP/3 is refused at config time (aioquic cannot enforce client certs). WS/gRPC auth is a
    follow-up. See [`docs/design/gateway-auth.md`](docs/design/gateway-auth.md).

### Fixed

- **Gateway middleware-load failures are now fatal (fail-open auth bypass fixed)** (v0.7.0 · R3, M1) —
  the ASGI edge previously caught any error while loading a global or route-scoped middleware, logged
  it, and **continued without that middleware** — so a security middleware that failed to import or
  construct was silently dropped and the gateway served **unauthenticated**. Middleware is now resolved
  eagerly at startup and a load failure raises `ConfigurationError` out of the gateway's `on_start`,
  crashing the supervised gateway instead of serving requests with a missing auth layer.

- **Cross-tree spawn** (v0.7.0 · R2) — `AgentProcess.spawn_into(supervisor_name, agent_class, name,
  config=None, *, wait=True)` spawns a child into any *named* `DynamicSupervisor` in the tree, not just
  the nearest ancestor. `spawn()` is now the nearest-ancestor special case of `spawn_into()` and delegates
  to it, so R1 semantics (`wait` / cleanup / `on_child_terminated`) are inherited unchanged. It fails fast
  with `SpawnError` instead of hanging: an unknown target, a non-`DynamicSupervisor` target, or a
  self-target are rejected before sending, and a valid but unresponsive target (e.g. `SUSPENDED`) surfaces
  as `SpawnError` via a bounded reply timeout. Every `DynamicSupervisor` now carries a reserved
  `_agency.dynamic_supervisor` capability, injected at registration (surviving a YAML `capabilities:`
  override and propagated to nested dynamically-spawned supervisors) so it appears in registry /
  `topology show` introspection. See [`docs/design/spawn-into.md`](docs/design/spawn-into.md).
- **Cross-tree spawn authorization** (v0.7.0 · R2) — because a cross-tree child runs the *spawner's* chosen
  code with the *target* supervisor's `llm` / `tools` / `store` (a confused-deputy surface), R2 ships the
  controls to let the target say no: a new `DynamicSupervisor(..., spawner_allowlist: set[str] | None =
  None)` rejects spawners not in the set *before* the governance hook (default `None` keeps today's open
  behavior); a read-only `DynamicSupervisor.current_spawner` property lets an `on_spawn_requested` override
  authorize by spawner without a signature change (valid only during the hook); and every admitted spawn
  emits a `dynamic.spawn` audit event (`spawner`, `child`, `class_path`, `supervisor`) via the wired audit
  sink.
- **Non-blocking dynamic spawn** (v0.7.0 · R1) — `DynamicSupervisor` spawning no longer forces the
  spawner to block on the child's `on_start()`. `spawn(..., wait=False)` — and the `spawn_nowait()`
  alias on both `AgentProcess` and `Runtime` — return as soon as the child's task exists; the child
  initialises in parallel and messages buffered in the meantime are dispatched FIFO once it is ready.
  `wait=True` remains the default and preserves the previous semantics, including synchronous
  start-failure reporting. Start failures after a non-blocking spawn are delivered to the spawner via
  `on_child_terminated`. See [`docs/design/non-blocking-spawn.md`](docs/design/non-blocking-spawn.md).
- `MessageBus.teardown_agent()` and `Transport.unsubscribe()` — unwire a terminated agent and fail any
  pending `ask()` bound to it (so callers fail fast instead of hanging).

### Changed

- `DynamicSupervisor` `max_total_spawns` now counts **every spawn attempt at admission** (previously
  only successful spawns); a failed spawn is no longer refunded, giving stricter spawn-storm protection.
- On `on_start()` failure, `on_stop()` is now called **best-effort** (a throwing `on_stop()` is logged
  and no longer masks the original start error).

### Fixed

- **Name collision no longer crashes the target `DynamicSupervisor`** (v0.7.0 · R2, P0) — spawning a child
  whose name already exists anywhere in the global registry previously let the `ValueError` from
  `registry.register()` escalate and crash the supervisor. `DynamicSupervisor._handle_spawn` now rejects a
  duplicate name with a `SpawnError` reply via a global pre-check and, for the cross-supervisor race, a
  wrapped `register()` — the supervisor stays running. Latent before R2 (ancestor-only spawn made it hard
  to reach); `spawn_into` turned it into a one-line cross-tree denial-of-service, so it is fixed here.
- Dynamically-spawned agents that fail `on_start()` no longer **leak** their registry entry or transport
  subscription — the supervisor runs a unified, idempotent terminal cleanup.

## [0.6.1] — 2026-07-04

### Fixed

- **Dynamically-spawned agents now inherit audit + metrics wiring** — agents created at runtime via
  `self.spawn()` / `DynamicSupervisor` were wired with the bus, tracer, registry, model provider, tools,
  and state store, but not the audit sink or metrics sink. Their audit events were silently dropped and
  their activity was invisible to metrics collectors. `DynamicSupervisor._handle_spawn` now also propagates
  `_audit_sink` and `_metrics`, matching `ComponentSet.inject` for statically-wired agents.

## [0.6.0] — 2026-07-04

### Added

- **Gateway streaming** (v0.6.0 / G2 + G3) — long-lived and incremental client connections built on one
  shared streaming core:
  - **WebSocket** (`GatewayConfig.ws_routes`) — bidirectional sessions: each inbound frame is `cast()` to
    an agent and the agent streams messages back over the same socket. No new dependency (uses
    `uvicorn[standard]`'s bundled `websockets`).
  - **Server-Sent Events / true streaming** — a route with `mode: "stream"` streams an agent's output
    incrementally as `text/event-stream`, replacing the old `{"chunks": [...]}` buffer-and-serialise
    workaround. Works over HTTP/1.1, HTTP/2, and HTTP/3.
  - **gRPC server-streaming** — the `Agent.Stream` RPC deferred in G1 is now implemented on the same core.
  - New agent API: `self.emit(chunk)` / `self.end_stream()` and the auto-terminating, error-safe
    `async with self.stream_reply() as stream:` context manager. Streams are bounded per-connection with a
    `slow_consumer` fail-fast plus idle/duration timeouts (`GatewayConfig.stream_queue_maxsize` /
    `stream_idle_timeout` / `max_stream_duration`).
- **Gateway middleware & uploads** (v0.6.0 / G4–G6) — first-party gateway building blocks:
  - **Rate limiting** (G4): a `RateLimiter` GenServer + `civitas.gateway.ratelimit.rate_limit`
    middleware (per-client sliding window; HTTP 429 + `Retry-After`).
  - **API-key auth** (G5): `civitas.gateway.auth.require_api_key` — fail-closed `X-API-Key` check
    against `CIVITAS_GATEWAY_API_KEY` (constant-time compare). JWT/mTLS remain integration points.
  - **File uploads** (G6): `multipart/form-data` is parsed at the ASGI edge; uploaded files reach the
    agent base64-encoded under `payload["__files__"]`, keeping payloads primitives-only.
- **Accelerated JSON serialization** (`civitas[fast]`) — installs Rust-backed `orjson`, which
  `JsonSerializer` uses automatically for a large encode/decode speedup, transparently falling back to
  the standard-library `json` module when it isn't installed. The wire format stays plain JSON, so the
  two backends interoperate. The `Message` payload primitives-only validation gate deliberately keeps
  using stdlib `json` — orjson natively accepts `datetime`/`UUID`/dataclasses and would weaken that
  enforcement.
- **gRPC gateway** (`civitas[grpc]`) — a generic `grpc.aio` surface on the gateway (v0.6.0 / G1). One
  `civitas.Agent` service proxies any agent by name, so callers need no per-agent `.proto` or civitas
  SDK: `Invoke` (unary → agent `call()`) and `Cast` (unary → `cast()`) carry a
  `google.protobuf.Struct` payload that maps to/from the JSON-ish dict a `Message` holds. Enabled with
  `GatewayConfig(grpc_enabled=True, grpc_port=…, grpc_reflection=True)` alongside the existing
  HTTP/1.1/2/3 surfaces; every transport shares one `GatewayDispatcher` so routing and error semantics
  stay identical. Ships the standard gRPC health service and optional server reflection (for
  `grpcurl`). Error mapping: no agent → `NOT_FOUND`, timeout → `DEADLINE_EXCEEDED`, unhandled error →
  `INTERNAL`; an agent's own business error is returned in-band on `AgentReply.error` with the payload
  preserved (mirrors the HTTP 400-with-body behaviour). `Stream` (server-streaming) is defined in the
  `.proto` but returns `UNIMPLEMENTED` until G3. Generated `_pb2` stubs are committed (no build-time
  `protoc` needed by consumers).

### Fixed

- **`DynamicSupervisor` spawn now applies `config` to the spawned agent** ([#8]) — the `config` passed
  to `spawn()` was parsed and governance-checked but never reached the agent. It is now available as
  `self.config` (readable in `on_start()` and `handle()`). Also documented that `on_start()` runs
  **synchronously inside the spawn call** — so `await self.spawn(...)` blocks until it returns; keep it
  fast and do slow/background work in `handle()`, kicked off by a post-spawn message (`on_start()`
  docstring + `docs/design/dynamic-spawning.md`).

[#8]: https://github.com/civitas-io/python-civitas/issues/8

## [0.5.0] — 2026-07-03

### Added

- **Durable suspension** (`agent.suspend()` / `agent.resume()`) — the Presidium human-in-the-loop
  approval primitive. `ProcessStatus.SUSPENDED` is re-introduced fully-wired (it was removed as dead
  API in **F02-6**): every transition, mailbox behaviour, supervisor interaction, and persistence
  path is defined and tested. Suspension pauses message *dispatch* only (Python cannot snapshot a
  running `handle()` coroutine). Key semantics: `suspend()` is a non-blocking flag actioned at the
  message-loop boundary; while suspended only the priority queue is drained so business messages stay
  buffered in FIFO order with backpressure preserved; the durable marker rides inside `self.state`
  under `_civitas.suspended` (one atomic checkpoint, durable only with a persistent store); suspend is
  write-ahead (pause in-memory first, then persist — never falls back to RUNNING on persist failure);
  `resume()` requires a non-empty `approver`; on permanent removal (despawn / restarts exhausted) the
  marker is cleared to prevent zombie-suspension, while graceful shutdown and crash-restart keep it.
  External entry points `runtime.suspend(name, reason="")` / `runtime.resume(name, approver)` deliver
  priority `_agency.suspend` / `_agency.resume` control messages. Each suspend/resume emits a
  `civitas.agent.suspend` / `civitas.agent.resume` span and an `AuditEvent` (resume records the
  approver). `ask()` into a suspended agent times out (documented; fail-fast deferred).

### Security

- Bumped `msgpack` floor to `>=1.2.1` (was `>=1.1`, resolved to 1.1.2) to clear
  **GHSA-6v7p-g79w-8964** (SEGV / DoS when a streaming `Unpacker` is reused after an error).
  Civitas's `MsgpackSerializer` uses one-shot `msgpack.unpackb()`, not the reused-`Unpacker`
  pattern, so core was not directly exposed — but the fix clears the `pip-audit --strict` CI gate
  and hardens the untrusted-input path over ZMQ/NATS transports regardless.

### Fixed

- **FD-01/FD-03** — `MetricsCollector` (dashboard) had no working way to receive restart, message,
  or error events; `civitas/cli/dashboard.py` worked around it by monkey-patching
  `runtime._root_supervisor._handle_crash` directly. Added `civitas.observability.metrics.MetricsSink`
  protocol, injected via `ComponentSet`/`Runtime.set_metrics()`; `Supervisor.add_crash_callback()` +
  `Runtime.on_crash()` public hook replace the monkeypatch. `message_handled` and `agent_error` are
  wired from `AgentProcess._dispatch()`, `message_sent` from `send()`/`ask()`. `llm_call` remains
  unwired — no clean interception point exists for `ModelResponse` token/cost data without a larger
  redesign of how `self.llm` is wrapped; flagged as a known follow-up, not silently claimed done.
- **F01-3** — `Message.ttl` was declared and documented (`AGENTS.md`) but never enforced. Added the
  `ttl` field to `Message` and enforcement in `Mailbox.get()`: expired messages are discarded with a
  warning instead of being delivered.
- **F11-5** — `on_stop()` was not called when `on_start()` raised, leaking any resources partially
  acquired in `on_start()` and any open `civitas.agent.start` tracer span. `AgentProcess._start()`
  now runs the equivalent cleanup (close span with error, mark `CRASHED`, run `on_stop()` + MCP
  client disconnect) before re-raising the original exception.
- **FD-07/FD-09** — `plugins.exporters` in topology YAML was parsed but never wired to anything in
  `Runtime` or `Worker`; `Worker` in particular dropped exporters silently in `cli/run.py`'s
  `_run_worker()`. Fixed both together: `Tracer` now skips its direct `TracerProvider` path
  whenever a `SpanQueue` is supplied (previously both could run simultaneously with no
  coordination), and `build_component_set()` assembles a `SpanQueue` + `FanOutBackend` from
  configured exporters, with `Runtime`/`Worker` owning the `OTELAgent` background task's lifecycle.
  `plugins.exporters: [{type: console}]` now actually works. The existing `OTEL_EXPORTER_OTLP_ENDPOINT`
  auto-detect path is unchanged when no `plugins.exporters` are configured.

## [0.4.0] — 2026-07-03

### Fixed

- **[#6](https://github.com/civitas-io/python-civitas/issues/6)** — Route-scoped gateway
  middleware (`RouteEntry.middleware` / a route's `middleware:` list in topology YAML) was
  parsed but never invoked by `GatewayASGI`. The dispatch layer built its middleware chain once
  at construction time from global `config.middleware` only; a matched route's own `.middleware`
  was never read. `GatewayASGI` now matches the route before building the chain and appends the
  route's resolved middleware after global middleware, restoring the documented execution order:
  global → route-scoped → contract validation → bus dispatch. Route middleware is resolved once
  and cached per route.
- **[#7](https://github.com/civitas-io/python-civitas/issues/7)** — Added the missing
  `civitas/py.typed` marker file. The package declared the `"Typing :: Typed"` classifier and
  ran `mypy --strict` internally, but shipped no marker, causing downstream `mypy --strict`
  users to get `import-untyped` errors on every `civitas` import.

### Changed

- **BREAKING CHANGE:** Model provider plugins (`civitas.plugins.{anthropic,openai,gemini,mistral,litellm}`), `civitas.plugins.sqlite_store`, `civitas.plugins.fiddler`, and `civitas.plugins.otel` have moved to `civitas-contrib` (`civitas_contrib.plugins.*`). Update imports, e.g. `from civitas.plugins.anthropic import AnthropicProvider` → `from civitas_contrib.plugins.anthropic import AnthropicProvider` (`pip install civitas-contrib[anthropic]`). `civitas.plugins.model`, `civitas.plugins.tools`, and `civitas.plugins.state` (protocols + `InMemoryStateStore`) remain in core.
- **BREAKING CHANGE:** `MCPClient` and `MCPTool` have moved to `fabrica` (`fabrica.mcp.client`, `fabrica.mcp.tool` — `pip install fabrica[mcp]`); there is no `civitas[mcp]` extra. `civitas.mcp.types` (`MCPServerConfig`, `MCPToolSchema`, `MCPToolError`) remains in core with no `mcp` SDK dependency at import time. `AgentProcess.connect_mcp()` and the `mcp.servers` topology YAML block remain in core and lazily import `fabrica.mcp.client.MCPClient` at call time, raising a `ConfigurationError` with install instructions if `fabrica` is not installed. `CivitasMCPServer` (exposing an agent tree as an MCP server) was never implemented in core — it is a Fabrica-scope feature, not a civitas regression.

### Added

#### M4.1b — Dynamic Agent Spawning

- `DynamicSupervisor` — starts empty, `ONE_FOR_ONE` only, `max_children` + `max_total_spawns` limits
- `self.spawn(AgentClass, name, config)` — routes to the nearest ancestor `DynamicSupervisor`
- `self.despawn(name)` (hard stop) / `self.stop(name, drain, timeout)` (soft stop, awaitable)
- `on_spawn_requested()` governance veto hook on `DynamicSupervisor`; `on_child_terminated()` notification hook on the spawning agent
- `Runtime.spawn()` / `Runtime.despawn()` / `Runtime.stop_agent()` — external entry points; `SpawnError` added to the error hierarchy
- `TopologyServer` — supervised JSON HTTP endpoint (`/topology`, `/agents`, `/agents/{name}`, `/health`); `civitas topology show` uses it for live state, falling back to static YAML

#### M4.2 — Security Hardening

- `civitas/security/` — `AgentIdentity` (Ed25519 keypairs), `KeyRegistry`, `MessageSigner` (v=2 wire format), `NonceCache` (replay protection), `SignatureError`, `SigningSerializer`; `security:` YAML block; InProcess transport bypasses signing entirely
- ZMQ CURVE and NATS TLS + nkeys transport-level encryption/auth; `civitas security init` CLI to scaffold keys
- `civitas.secrets` — `SecretsProvider` protocol, env/file implementations, `${VAR_NAME}` substitution in topology YAML (raises `ConfigurationError` on unset vars), per-agent `credentials:` block
- Bubblewrap-based tool sandbox for MCP subprocess execution on Linux; `sandbox:` YAML block; refuses to start when `sandbox.enabled: true` and `bwrap` is unavailable
- `civitas.audit` — `AuditEvent`, `AuditSink` protocol, `JsonlFileSink` (batched fsync, SIGHUP rotation), `NullSink`, `SyslogSink`, `OtlpSink`; emission at `MessageBus.route()`, tool execution, sandbox violations, secret access

#### M4.4 — Capability-Aware Registry

- `RoutingEntry.capabilities` / `capability_metadata`; `LocalRegistry.find_by_capability()` / `find_by_capabilities(tags, match="any"|"all")`
- `AgentProcess.capabilities` / `capability_metadata` class-level declarations; `AgentProcess.send_capable(capability, payload)`
- `CapabilityNotFoundError`; YAML `capabilities:` / `capability_metadata:` overrides; distributed propagation via Worker announcements
- `RegistryListener` hook — async callbacks on every register/deregister (Presidium integration point); `add_listener()` / `remove_listener()`
- `RoutingEntry`, `RegistryListener`, `CapabilityNotFoundError` exported from `civitas` top-level package

#### Gateway API Surface

- `@route(method, path, mode=)` and `@contract(request=Model, response=Model)` decorators; YAML remains the sole runtime-authoritative source, decorators are documentation + `civitas topology validate` cross-checks
- Global + route-scoped middleware chain (`config.middleware`, `RouteEntry.middleware`); short-circuit by returning a `GatewayResponse` without calling `next_fn`
- Auto-generated OpenAPI 3.1 spec (`GET /openapi.json`), Swagger UI (`GET /docs`), ReDoc (`GET /redoc`); `docs.enabled: false` disables all three

#### Postgres StateStore + Migration

- `PostgresStateStore` — `asyncpg` backend, connection pool, `civitas_agent_state` JSONB table with upsert; `civitas[postgres]` extra
- `StateStore` protocol extended with `list_agents()` / `close()`; `@runtime_checkable` for `isinstance()` checks
- `civitas state migrate <src> <dst>` — dry-run by default, `--execute` to apply; `_parse_dsn()` supports `sqlite:`, `.db`/`.sqlite`, and `postgresql://`

#### M4.1 — HTTP Gateway

- `civitas.gateway.HTTPGateway` — supervised `AgentProcess` that translates HTTP ↔ Civitas messages; external clients never touch the bus directly
- `civitas.gateway.GatewayConfig` — dataclass covering host, port, TLS, HTTP/3 (QUIC), routes, middleware, OpenAPI docs, and request timeout
- `civitas.gateway.RouteTable` — ordered route matching table; path parameters extracted and merged into `message.payload`; YAML is the authoritative source
- `civitas.gateway.route` — `@route(method, path, mode=)` decorator to co-locate route metadata on agent methods; used by `civitas topology validate`, never read at runtime
- `civitas.gateway.contract` — `@contract(request=Model, response=Model)` decorator; wired to `RouteTable.merge_contracts_from()` for automatic 422/500 validation
- `civitas.gateway.GatewayRequest` / `GatewayResponse` — thin middleware types; middleware receives `(request, next_fn)` and can short-circuit or pass through
- Middleware chain: global middleware loaded from dotted import paths in `config.middleware`; per-route middleware supported in `RouteEntry`
- OpenAPI 3.1 spec auto-generated from route table; served at `GET /docs/openapi.json`; Swagger UI served at `GET /docs` (CDN-hosted, zero bundling)
- Default URL conventions: `POST /agents/{name}` → call, `POST /agents/{name}/cast` → cast, `GET /agents/{name}/state` → call with `{"__op__": "state"}`
- HTTP/3 / QUIC via `civitas[http3]` (`aioquic`): `H3Server` runs alongside uvicorn; `Alt-Svc` header injected automatically when `enable_http3: true`
- W3C `traceparent` header parsed and propagated to trace context; `X-Civitas-Type` header overrides the Civitas message type
- Topology YAML support: `type: http_gateway` node type wired into `Runtime._build_node()`; `civitas topology show` renders `[http]` prefix
- `examples/http_gateway.py` — end-to-end example with `EchoAgent` and Swagger UI
- `pyproject.toml` — `civitas[http]` (uvicorn + pydantic) and `civitas[http3]` (+ aioquic) optional extras

#### M4.3 — Codebase Security & Enterprise Posture

- `.github/workflows/security.yml` — three-job security CI workflow running on every PR and weekly:
  - SAST: Bandit (`-lll -iii`, HIGH+ severity) + Semgrep (`p/python`, `p/secrets`, `p/owasp-top-ten`); SARIF uploaded to GitHub Security tab
  - Dependency audit: `pip-audit --strict` against PyPI Advisory Database; fails on any fixable vulnerability
  - Secret scan: `gitleaks` on full git history (`fetch-depth: 0`)
- `.github/dependabot.yml` — weekly Dependabot scans for pip and GitHub Actions dependencies; dev-tools grouped to reduce PR noise
- `publish.yml` — CycloneDX SBOM generated (JSON + XML) on every release tag; attached as GitHub release assets
- `.pre-commit-config.yaml` — `gitleaks` pre-commit hook added; blocks secret commits before they reach the remote
- `SECURITY.md` — responsible disclosure policy: email contact, response SLAs (2 days ack, 14 days for CRITICAL/HIGH), 90-day coordinated disclosure window, CVE process, supported versions
- `docs/security/threat-model.md` — STRIDE analysis for all runtime components: `AgentProcess`, `Supervisor`, `MessageBus`, `ZMQTransport`, `NATSTransport`, `HTTPGateway`, `StateStore`, plugin system, `EvalAgent`; risk summary with 21 itemised threats
- `docs/security/architecture.md` — four-zone trust boundary model (runtime process → Worker processes → remote machines → external clients), transport security posture per level, credential handling patterns, planned M4.2 hardening roadmap
- `docs/security/enterprise-checklist.md` — tiered adoption checklist (Level 1–4 by deployment complexity) + compliance guidance for SOC 2, GDPR, and HIPAA

---

## [0.3.0] — 2026-04-22

### Added

#### M2.5 — EvalLoop

- `EvalAgent` — supervised process that monitors agent behaviour and sends correction signals; sits alongside regular agents in the supervision tree
- `EvalEvent` — observable event emitted by agents; schema aligned with OTEL GenAI Semantic Conventions for remote exporter compatibility
- `CorrectionSignal` — three severity levels: `nudge` (soft guidance), `redirect` (change course), `halt` (stop agent cleanly)
- `EvalExporter` protocol — interface for remote eval engine adapters (Arize, Fiddler, Langfuse, etc.); implementations in M2.6
- `AgentProcess.emit_eval(event_type, payload, eval_agent)` — emit an observable event; no-op when bus not wired (safe in tests)
- `AgentProcess.on_correction(message)` — override hook called on `civitas.eval.correction` signals (nudge / redirect)
- `civitas.eval.halt` message type — breaks target agent's message loop cleanly; `on_stop()` still runs
- Rate limiting on `EvalAgent`: sliding window per target agent (`max_corrections_per_window`, `window_seconds`); excess corrections dropped and logged
- `type: eval_agent` YAML shorthand in `Runtime.from_config()` with `max_corrections_per_window` and `window_seconds` config
- `[eval]` label in `print_tree()` / `civitas topology show` for EvalAgent nodes
- `EvalAgent`, `EvalEvent`, `CorrectionSignal`, `EvalExporter` exported from `civitas` top-level package

#### M3.5 — GenServer

- `GenServer` — OTP-style generic server process with `handle_call` (synchronous, reply required), `handle_cast` (fire-and-forget), and `handle_info` (timers, internal signals) dispatch
- `send_after(delay_ms, payload)` — schedules a `handle_info` message to self after a delay; pending tasks cancelled on stop
- `AgentProcess.call(name, payload)` — synchronous GenServer call (wraps `ask()`, returns payload dict)
- `AgentProcess.cast(name, payload)` — fire-and-forget GenServer cast
- `Runtime.call()` / `Runtime.cast()` — runtime-level GenServer messaging
- `GenServer` exported from `civitas` top-level package
- `type: gen_server` support in `Runtime.from_config()` YAML topology
- `[srv]` label in `print_tree()` / `civitas topology show` for GenServer nodes

#### M3.4 — MCP Integration

- `civitas[mcp]` optional extra (`pip install 'civitas[mcp]'`) — wraps `mcp>=1.0` SDK
- `MCPServerConfig` — config dataclass for stdio and SSE MCP server connections; validated at construction
- `MCPClient` — persistent-per-agent MCP session with `connect()`, `disconnect()`, `list_tools()`, `call_tool()`; `AsyncExitStack` manages transport + session lifecycle as a unit
- `MCPTool` — `ToolProvider` wrapping a single MCP tool; name follows `mcp://server_name/tool_name` URI scheme for direct lookup via `self.tools.get()`; emits `civitas.mcp.call` OTEL span
- `MCPToolError` — raised when an MCP tool call returns `isError=True`
- `AgentProcess.connect_mcp(config)` — connects to an MCP server and registers all its tools into `self.tools`; idempotent (disconnects and deregisters existing tools for the same server before reconnecting)
- `ToolRegistry.deregister_prefix(prefix)` — removes all tools whose name starts with a given prefix
- `mcp.servers` topology YAML key — declare MCP servers in the topology file; `Runtime.from_config()` parses configs and auto-connects all agents on `start()`
- MCP clients are closed gracefully in the `_message_loop` finally block alongside `on_stop()`

---

## [0.1.0] — 2026-04-06

Initial public release.

### Added

#### Core runtime

- `AgentProcess` — asyncio-based agent with bounded mailbox, lifecycle hooks (`on_start`, `handle`, `on_error`, `on_stop`), and injected dependencies (`self.llm`, `self.tools`, `self.store`, `self._tracer`)
- `Supervisor` — fault tolerance tree with three restart strategies: `ONE_FOR_ONE`, `ONE_FOR_ALL`, `REST_FOR_ONE`
- Configurable backoff policies: `CONSTANT`, `LINEAR`, `EXPONENTIAL` (with 25% jitter)
- Sliding-window restart rate limiting (`max_restarts` + `restart_window`)
- Escalation chain — supervisor that exceeds its restart limit escalates to its parent
- Heartbeat-based monitoring for agents running in remote Worker processes
- `Runtime` — assembles and manages the full supervision tree; 13-step deterministic startup sequence
- `Worker` — multi-process agent host; connects to the broker and announces agents via `_agency.register`
- `ComponentSet` — shared infrastructure wiring for both `Runtime` and `Worker`
- `Runtime.from_config()` — builds a complete runtime from a YAML topology file

#### Messaging

- `MessageBus` — name-based routing with Registry lookup and ephemeral reply address fallback
- `send()` — fire-and-forget delivery
- `ask()` — request-reply with configurable timeout and ephemeral reply routing
- `broadcast()` — glob-pattern delivery to multiple agents
- `reply()` — return a reply from `handle()` without knowing the caller's address
- Bounded mailboxes with `asyncio.QueueFull` backpressure
- System message namespace (`_agency.*`) reserved and validated at route time
- Trace context propagation across all message boundaries

#### Transport layer

- `InProcessTransport` — asyncio queues, zero extra dependencies, ~2–5 µs latency
- `ZMQTransport` — XSUB/XPUB proxy, multi-process on a single machine (`pip install civitas[zmq]`)
- `NATSTransport` — distributed multi-machine transport with optional JetStream durable subscriptions (`pip install civitas[nats]`)
- Uniform `Transport` protocol — swap transports with a one-line topology change, no agent code changes
- Remote agent registration/deregistration via `_agency.register` / `_agency.deregister` messages

#### Plugin system

- `ModelProvider` protocol — structural, no base class required
- `AnthropicProvider` — first-party Anthropic SDK integration with built-in token pricing (`pip install civitas[anthropic]`)
- `LiteLLMProvider` — 100+ models via LiteLLM (OpenAI, Gemini, Bedrock, Azure, etc.) (`pip install civitas[litellm]`)
- `ToolProvider` protocol and `ToolRegistry` — named tools with JSON schema, duplicate name detection
- `StateStore` protocol — `get` / `set` / `delete` by agent name
- `InMemoryStateStore` — default, in-process, survives supervisor restarts
- `SQLiteStateStore` — durable persistence, all I/O in thread executor (non-blocking)
- `ModelResponse` dataclass — content, model, token counts, cost, tool calls
- Plugin loading from YAML topology — entrypoint → built-in name → dotted import path resolution
- `PluginError` — fast-fail at `Runtime.start()` with actionable error messages and install hints

#### Observability

- Automatic OTEL spans for every message send/receive, agent lifecycle event, LLM call, tool invocation, and supervisor restart
- `SpanQueue` — non-blocking span emission from the message loop (`put_nowait`, drops oldest if full)
- Three output modes: built-in `logging.DEBUG` console output (no deps) → OTEL `ConsoleSpanExporter` → OTLP gRPC export (Jaeger, Grafana Tempo, Datadog, etc.)
- `llm_span()` and `tool_span()` context managers for custom instrumentation
- Full span attribute reference under `civitas.*`, `llm.*`, `tool.*` namespaces
- Trace context propagation across process and machine boundaries
- `FanOutBackend` — export to multiple backends simultaneously
- Per-agent LLM cost attribution via `llm.cost_usd` span attribute

#### Framework adapters

- `LangGraphAgent` — wraps a LangGraph `CompiledGraph` as an `AgentProcess`; optional typed `input_schema` for early payload validation
- `OpenAIAgent` — wraps an OpenAI Agents SDK `Agent`; maps handoffs to Civitas `send()` calls

#### YAML topology

- Declarative topology YAML — supervision tree, transport, plugins in one file
- Full field schema: supervision strategies, backoff, transport per-implementation config, plugin config
- Process affinity — `process: worker` assigns agents to named Worker processes
- `Runtime.from_config()` with short-name `agent_classes` map
- Flat agent shorthand (`agent: { name: ..., type: ... }`)
- Case-insensitive strategy and backoff values

#### CLI

- `civitas run` — start the runtime from a topology file; `--transport`, `--process`, `--nats-url` overrides
- `civitas topology validate` — structural and configuration validation with grouped output; exit 1 on failure (CI-safe)
- `civitas topology show` — render the supervision tree with inline restart policies
- `civitas topology diff` — meaningful diff between two topology files grouped by section
- `civitas deploy docker-compose` — generate `Dockerfile`, `docker-compose.yml`, and `.env` from a topology; one service per process group
- `civitas state list` / `show` / `clear` — inspect and manage persisted agent state

#### Serialization

- `MsgpackSerializer` — default, binary, fast
- `JsonSerializer` — human-readable, selectable via `AGENCY_SERIALIZER=json`
- All messages serialized even on InProcessTransport — guarantees transport-swap transparency
- `DeserializationError` with stable contract and schema versioning

### Changed

- `Registry` redesigned as `LocalRegistry` with `RoutingEntry` dataclass and glob-pattern `lookup_all()` for broadcast
- `ComponentSet` extracted from `Runtime` to eliminate wiring duplication between `Runtime` and `Worker`
- Backoff computation moved to `Supervisor._compute_backoff()` with explicit jitter on EXPONENTIAL
- Supervisor crash handling uses tracked `asyncio.Task` set (`_pending_crash_tasks`) to prevent races with shutdown
- Sliding window uses `collections.deque` for O(1) append/popleft; child lookup uses supplementary dict for O(1) by name

### Fixed

- ZMQ transport: idempotent `start()`, correct error handling on socket close, reply routing for cross-process messages
- NATS transport: reconnection handling, JetStream stream creation idempotency
- Supervisor: crash handler tasks are cancelled before children are stopped (`stop()` teardown ordering)
- Supervisor: `ONE_FOR_ALL` and `REST_FOR_ONE` skip agents already in `STOPPED` / `STOPPING` / `CRASHED` states
- `AgentProcess`: state restored from `StateStore` before `on_start()` runs on restart
- `MessageBus`: `_agency.*` message type validation applied to all routes, not just system senders
- `OpenAIAgent`: unregistered handoff targets log a warning rather than crashing the handler
- `LangGraphAgent`: non-dict graph outputs are wrapped in `{"output": value}` rather than raising `TypeError`
- Pre-commit hooks: ruff + mypy run on every commit; CI enforces 85% coverage threshold

[Unreleased]: https://github.com/civitas-io/python-civitas/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/civitas-io/python-civitas/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/civitas-io/python-civitas/releases/tag/v0.1.0
