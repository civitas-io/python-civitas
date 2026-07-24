# Design: Supervision Endgame (v0.9.0)

**Status:** ✅ ACCEPTED 2026-07-24 — maintainer ratified Q1–Q4 (§9: Q1 accept staleness, no
warning · Q2 one-minor-version skew tolerance · Q3 hard-reject supervisor suspension · Q4 P4 in
v0.9.0 with the P1–P3 cut line). Implementation plan:
`.sisyphus/plans/supervision-endgame-v0.9.0.md`.
**Author:** design session 2026-07-24
**Scope:** D6 (supervisor unification), D1a (fresh-instance restart), D5-structural (per-process liveness), fold-in decisions B4 + B3.
**Parents:** [`supervision-hardening.md`](supervision-hardening.md) (findings + v0.8.0 groundwork), [`dynamic-spawning.md`](dynamic-spawning.md), [`durable-suspension.md`](durable-suspension.md).
**Payoff:** closes every remaining finding from the 2026-07 architecture review; flips the last strict-xfail tracker; unblocks Medicus (`self-healing.md` explicitly requires trustworthy fresh restarts).

> **One-line framing:** v0.8.x made supervision *honest* on the existing machinery. v0.9.0 replaces
> the machinery's split brain — two restart engines, resumed-object restarts, mailbox-coupled
> liveness — with the model the README already claims: one engine, fresh incarnations, liveness
> that load cannot distort.

---

## 1. Where we are (post-v0.8.2 ground truth)

| Area | Today | Debt |
|---|---|---|
| Engines | `Supervisor` (not an actor: direct child method calls, own crash queue since H2) and `DynamicSupervisor(AgentProcess)` (bus-driven, own restart bookkeeping) | Two implementations of budgets/backoff/restart; divergent semantics (DynSup never escalates); B1 |
| Restart | Same object resumed; `self.state` reset + checkpoint restore (H5b); instance variables survive; mailbox retained | A1 half-open — the xfail tracker; `_civitas_spec` (cls, args, kwargs) already captured on every instance since v0.8.0 but unconsumed |
| Liveness | Remote: per-agent heartbeats through the mailbox (priority since H4) — a busy agent still delays acks to the next loop boundary. Local: task exceptions + opt-in `handle_timeout` | A6 structural residue: liveness is still conflated with per-agent dispatch latency; one 20 s handler on a remote agent = false crash |
| Spawn ergonomics | `wait=True` blocks the DynSup's loop (B4); restart accounting mixes supervisor-wide window with per-child lifetime backoff counts (B3) | Deferred from v0.8.0 to here |

Existing seams this design builds on (all v0.8.x): the serialized crash queue (H2), snapshot
re-registration (H3), spec capture (`AgentProcess.__new__`), the `_runtime` bare-subscription sink
pattern (H9), the announce settle-barrier (V3), and `bus.teardown_agent`'s drain-with-error-replies.

## 2. Design overview — four staged phases, each independently shippable

```
P1  RestartEngine extraction     pure refactor: ONE budget/backoff/restart implementation
    (+ B3 decided here)          behind both classes; behavior-change count: 1 (B3)
P2  Fresh-instance restart (D1a) the engine consumes _civitas_spec; restart = new incarnation
P3  Process-level liveness (D5)  worker heartbeat responder off-mailbox; rich per-agent acks
P4  Supervisor actorization (D6) static Supervisor becomes addressable; control ops ride its
    (+ B4 fixed here)            mailbox; DynSup wait=True stops blocking (deferred replies)
```

Cut line: **P1+P2 are the release core** (they flip the xfail and deliver the Medicus
prerequisite). P3 is high-value independent. P4 is the deepest change; if review of its blast
radius sours, it slips to v0.9.1 without weakening P1–P3 (**Q4**).

---

## 3. P1 — RestartEngine extraction (+ B3)

One internal component owns what both classes duplicate today:

```
class _RestartEngine:                    # civitas/supervision/engine.py (internal)
    budget: intensity window (deque) + max_restarts + restart_window
    backoff: policy + base + max
    def record_crash(now) -> Verdict     # RESTART(delay) | EXHAUSTED
    def note_incarnation_reset()         # fresh budget on subtree restart (H1 rule)
```

`Supervisor` and `DynamicSupervisor` both delegate; their *strategies* (ONE_FOR_ONE/ALL/REST vs
permanent/transient/never) stay where they are — the engine is accounting + verdicts only, not
policy. No public API change.

**B3 decided here — OTP-pure accounting:** the intensity window is supervisor-wide (as today);
**backoff is computed from the window occupancy at verdict time**, not from per-child lifetime
counts. Consequences: backoff naturally decays when the window empties (today a child's 4th
lifetime crash gets 8× base forever); `_restart_counts` survives only as an observability counter
(spans/logs), never as a backoff input. This is the single deliberate behavior change in P1,
CHANGELOG'd.

## 4. P2 — Fresh-instance restart (D1a)

**Contract:** a restarted child is a **new incarnation** — new object from
`_civitas_spec = (cls, args, kwargs)`, fresh instance variables, `self.state` from checkpoint
only (H5b semantics unchanged), **queued mailbox messages carried over in order** (the in-flight
message stays lost — documented at-most-once). This makes AGENTS.md's "instance variables are
undefined across restarts" the literal truth and closes A1.

### Mechanics (the part v0.8.0 deferred as "architecture work")

1. **Instantiate:** `cls(*args, **kwargs)` — user `__init__` re-runs by design (that's the fresh
   heap). Failure to instantiate = restart failure → existing H2 loud-escalation path.
2. **Rewire:** a single `_wire_child(agent)` hook on the engine's owner:
   - static children: Runtime registers a wiring callback on each supervisor at start
     (`ComponentSet.inject` + per-agent credentials + `_dynamic_supervisor_name`) — the injector
     Runtime already applies at startup, made re-invokable;
   - dynamic children: DynSup already self-wires (llm/tools/store refs) — unchanged.
3. **Re-subscribe:** `bus.setup_agent(new_agent)` — handler closures capture the agent object, so
   re-subscription is mandatory. Transport-level: same address, handler dict overwrite; ZMQ topic
   already subscribed (no propagation race — the V3 barrier concerned *new* topics).
4. **Mailbox carry-over:** drain old incarnation's mailbox into the new one before task start
   (both queues, priority first — same order `Mailbox.drain()` yields today).
5. **Registry:** H3's `reregister_preserving` — unchanged.
6. **Reference freshness:** the object identity changes, so:
   - `Runtime._agents_by_name` and `TopologyServer._agents` update via a new
     `on_child_replaced(name, new_agent)` engine callback (same pattern as `add_crash_callback`);
   - `DynamicSupervisor._ChildRec.agent` / `_child_tasks` swap in place;
   - **user-held direct references go stale** — already anti-pattern #6 ("route by name, never by
     object"); called out as a behavior change regardless (**Q1**: accept).
7. **Suspension:** the durable marker rides the checkpoint → a suspended agent restarts into
   SUSPENDED on the fresh instance. `on_suspend` does NOT re-fire (restore, not fresh suspend) —
   S7 semantics preserved byte-for-byte.

### Edge inventory (each becomes a test)

fresh `GenServer` gets clean `send_after` timers · `HTTPGateway` restart re-binds via the same
(shared-reference) `GatewayConfig` · spec kwargs preserve `handle_timeout`/`mailbox_size` ·
`_civitas_spec` re-captured on the new instance (restart-of-restart) · crash DURING `_wire_child`
= restart failure, not a half-wired zombie (wire fully before `_start_nowait`) · old incarnation's
`_pending_streams` fail with `agent_stopped` (existing `_fail_local_streams`) before drain.

## 5. P3 — Per-process liveness (D5-structural)

**Problem residue:** heartbeat acks still require the *agent's* loop boundary; a legitimately
long `handle()` on a remote agent looks dead. Liveness (is the process serving?) is conflated
with responsiveness (is this agent's queue moving?).

**Design — split the two questions:**

1. **Process channel:** each `Worker` subscribes `_agency.worker.<worker-id>.health` at the
   *transport* level (the H9 bare-sink pattern — no mailbox, no agent, answered inline in the
   receive handler). The supervisor pings the process once per interval instead of every agent.
2. **Rich acks:** the health reply carries a per-agent snapshot the Worker can produce without
   touching any mailbox: `{name: {status, task_alive, mailbox_depth}}` — status from
   `agent._status`, task liveness from `task.done()`, depth from the queues' `qsize()`.
3. **Supervisor consumption:** process unreachable × threshold → all that worker's children get
   today's `HeartbeatTimeout` crash-queue treatment (unchanged downstream). Process alive but a
   child's `task_alive=False`/CRASHED → restart *that child* remotely — **new capability: remote
   crash detection without waiting for a full heartbeat starvation cycle.** `mailbox_depth` is
   observability-only in v0.9 (no auto-action — restarting for backlog is a policy decision that
   belongs to Presidium/Medicus, not the runtime).
4. **Version skew:** Workers advertise the channel in their `_agency.register` announce
   (capability flag). Supervisors use the process channel when advertised, else fall back to
   today's per-agent pings — a v0.9 runtime supervises a v0.8 worker correctly (**Q2**: is one
   release of skew tolerance enough?).
5. **Local agents:** explicitly out of scope — task exceptions + `handle_timeout` already cover
   local crash/hang; there is no process boundary to probe.

Retired at the end of P3 (when skew window closes): per-agent `_agency.heartbeat` send path.
The auto-ack in `_message_loop` stays indefinitely (harmless, cheap, aids debugging).

## 6. P4 — Supervisor actorization (D6, + B4)

**The unification:** `Supervisor` becomes an `AgentProcess` subclass (as `DynamicSupervisor`
already is). Concretely:

- **Control ops ride the mailbox.** The H2 crash queue *becomes* the supervisor's mailbox
  (crash events = priority messages to self) — one serialization mechanism instead of two.
  Escalation = a message to the parent supervisor's name. The drain task disappears.
- **Addressable + registered**, with a marker capability (like `DYNAMIC_SUPERVISOR_CAPABILITY`),
  enabling introspection (`ask("root", {"type": "civitas.supervision.status"})`) and giving
  Presidium a uniform governance surface. Registered names = child glob-hygiene rules apply.
- **Constructor and semantics preserved:** `Supervisor(name, children=[], strategy=..., ...)`
  unchanged; strategies unchanged; heartbeat monitoring (per P3) unchanged; `Runtime.on_crash`
  callbacks unchanged. `print_tree`, `all_agents`, `all_supervisors` unchanged.
- **Deliberately NOT inherited:** suspension of supervisors is **rejected** in v0.9 — a paused
  subtree manager is a footgun (children crash unattended). `suspend()` on a supervisor returns
  an error reply (**Q3**: agree?). Similarly no llm/tools injection into supervisors.
- **Startup ordering:** supervisors-as-agents need the bus before children start; Runtime already
  wires bus/registry/tracer into supervisors before `root.start()` — ordering audit is a P4 task,
  not a redesign.

**B4 fixed here:** with control ops on the mailbox, DynSup's `wait=True` must not block the loop.
The spawn handler starts the child, stashes the reply envelope, and a done/ready continuation
sends the reply from the child's readiness event (the announce-after-start D13 plumbing already
does exactly this dance for announcements). Head-of-line blocking ends; `_SPAWN_ASK_TIMEOUT`
semantics unchanged for callers.

### 6.1 P4 implementation decisions (pre-implementation review, 2026-07-24)

Settled before code, after the E1–E3 ground truth made P4's edges concrete:

**D-E4-1 — Crash events use a side-table; the mailbox carries only the trigger.**
The naive "crash event = self-message" collides with a core invariant: `Message.payload` is
JSON-primitives-only (enforced at construction), but a crash event carries an **Exception
object** (the public `add_crash_callback(name, exc)` contract) and an **asyncio.Task** (the
stale-incarnation marker). Resolution: `_on_child_done` stores `(name, exc, task)` in a
supervisor-local side-table keyed by event-id and self-sends a priority
`_agency.child_crashed {event_id}` message; the handler pops and processes. The mailbox provides
ordering/serialization (the actor property we want); the side-table preserves the object-graph
(the Python reality). Escalation to the parent works the same way: the supervision tree is
always in-process relative to its parent (remote children are agents, never supervisors), so the
escalating supervisor writes the parent's side-table directly via `self._parent` and sends the
trigger message to the parent's *name* — the data hop is direct, the control hop rides the bus
(ordered, traced).

**D-E4-2 — Enqueue must be sync and unbounded for supervisors.** `_on_child_done` is a sync
task-callback: it cannot `await Mailbox.put()`, and the priority queue's hardcoded `maxsize=100`
would make `put_nowait` raise under a mass-crash burst — re-creating the crash-drop bug class H2
killed. Resolution: `Mailbox` gains a `priority_maxsize` parameter (default 100, unchanged for
agents) and a `put_nowait()`; supervisors construct their mailbox with an unbounded priority
queue (crash volume is bounded by child count in practice, and "unbounded on purpose" is the
same deliberate choice H2 made for the queue this replaces).

**D-E4-3 — Startup/shutdown ordering.** `Supervisor.start()` becomes: start OWN loop first
(`_start()` — the mailbox must be live before any child can crash-report) → set child parents →
start children → heartbeat monitor. `stop()` reverses: heartbeat → children → own loop. Late
crash messages after children stop die with the mailbox — the exact analog of H2's
dequeue-after-stop discard. Runtime must `setup_agent()` + register every supervisor (with a
`SUPERVISOR_CAPABILITY` marker) before `root.start()`; the explicit startup-order test from plan
constraint 7 gates this.

**D-E4-4 — `Supervisor.handle()` dispatch table.** `_agency.child_crashed` → crash processing
(E1 engine verdict → E2 fresh-incarnation restart — both untouched); `civitas.supervision.status`
→ introspection reply (children, states, window occupancy, restart counts); anything else →
error reply when ask, WARNING drop otherwise. `suspend()` and `_agency.suspend` → hard reject
(Q3). `_agency.child_crashed` joins `SYSTEM_MESSAGE_TYPES` — and per the E3 lesson (a missing
entry produced a vacuous-pass failure shape), the E4 suite asserts the crash path end-to-end
through the bus, not just unit-level.

**D-E4-5 — B4 deferred replies.** `_handle_spawn` `wait=True` stops blocking the loop: stash the
reply envelope, attach a continuation to the child's readiness event (the `_announce_after_start`
plumbing, generalized), route the reply from the continuation. Caller semantics and
`_SPAWN_ASK_TIMEOUT` unchanged; the DynSup serves concurrent spawn/despawn/status during a slow
`on_start()`.

**Migration sizing (measured):** ~47 direct references to `_crash_queue`/`_drain_crashes`/
`_handle_crash`/`_escalate` across 5 test files (36 in `test_supervisor.py`) — mechanical
migrations to the mailbox/side-table surface, each justified per the regression law. Deleted
outright: `_crash_queue`, `_crash_drain_task`, `_drain_crashes` (~60 lines) — H2's hand-built
mailbox replaced by the real one it anticipated.

**D-E4-6 — `Supervisor.stop()` intentionally shadows `AgentProcess.stop(name, drain, timeout)`
(found during Phase A implementation, not anticipated pre-implementation).** Inheriting from
`AgentProcess` pulls in a public `stop(name, drain, timeout)` method (soft-stop a dynamically
spawned child) that collides by name with the pre-existing public `Supervisor.stop()` (shut down
this supervisor and its children) — unrelated operations, same name, now the same namespace.
Verified safe rather than papered over: the inherited method requires
`self._dynamic_supervisor_name` to be wired, and Runtime's `_wire_dyn_sup` never sets it on a
`Supervisor` node (only recurses through its children) — so on any `Supervisor` instance the
shadowed method could only ever have raised `SpawnError`. Resolution: keep the pre-existing
public `stop()` (renaming it would be the actual breaking change), with a `# type:
ignore[override]` carrying this justification. Zero test impact (1130/1130 unit, full integration,
both macOS and Linux) — Halt-Check A criteria met; Phase A proceeds.

**D-E4-7 — Phase B test-authoring heuristic + the two named Halt-Check B proofs (recorded
before Phase B code, 2026-07-24 walkthrough).** Two refinements the pre-implementation review
left as "decide per-test" / "needs proof, not assumption" — resolved concretely so Phase B has
no open judgment calls left when coding starts:

- **Bare-Supervisor test-authoring heuristic.** Dozens of existing tests construct a
  `Supervisor` directly (no `Runtime`, no bus) and call `_handle_crash(name, exc)` directly,
  bypassing crash *delivery* to unit-test crash *handling* (strategy dispatch, budget/backoff
  verdicts, restart calls). Rule for migration: **tests that assert on handling logic keep
  calling `_handle_crash` directly** (its body doesn't change in Phase B — only what calls it
  does); **only the tests whose subject IS the delivery path itself** (crash-queue/drain-task
  internals, stale-incarnation skip, post-stop discard) migrate to asserting through the mailbox/
  side-table or get superseded by the two named proofs below. This keeps the ~47-reference
  migration from being uniformly rewritten when only a subset actually tests delivery.
- **Two Halt-Check B proofs, named explicitly (not left as "needs proof"):**
  1. `test_bare_supervisor_crash_delivery_without_bus` — a `Supervisor` with `_bus=None`
     (today's common test pattern) still delivers a real child crash through `_on_child_done` →
     side-table → `put_nowait` → own mailbox → `handle()`, with NO bus involved (self-delivery
     is local `Mailbox` traffic, not transport traffic — confirmed by Phase A: the loop runs
     standalone). This is the direct evidence for the heuristic above being safe.
  2. `test_no_resurrection_after_stop_during_backoff` — crash a child with a deliberately long
     backoff, call `stop()` while the restart is still sleeping, and assert the child is not
     restarted and no task leaks. Today's guarantee comes from cancelling `_crash_drain_task`
     before touching children; Phase B's candidate mechanism is "the supervisor's own `_stop()`
     (already last, from Phase A) stops the loop from consuming any further crash messages" —
     this test is what turns that candidate mechanism into a verified fact instead of an analogy.

  **If either proof fails to hold without weakening a guarantee, Q4 triggers**: halt Phase B,
  ship E1–E3 as v0.9.0, re-plan E4 for v0.9.1 with the specific failure recorded here.

**D-E4-8 — `Supervisor.stop()`'s own-loop-LAST ordering (D-E4-3) does not survive Phase B;
corrected to own-loop-FIRST (found via mechanical trace before Phase B code, not by running the
proof test).** D-E4-3 chose "start own loop first / stop own loop last" for symmetry ("a live
loop outlives its children's shutdown"). That was safe in Phase A because the mailbox was inert
— nothing sent a Supervisor a message yet. Phase B puts crash-triggered restarts (including the
backoff `asyncio.sleep`) onto that same loop, and the old code's actual protection against
"resurrection after stop" was never the `if not self._running` flag check (that only catches
events still *queued*, not one already past the check and asleep in backoff) — it was
**`self._crash_drain_task.cancel()`**, which aborts an in-flight `asyncio.sleep()` immediately
regardless of what it's doing. A flag can't interrupt a sleep already in progress; only
cancellation can.

With crash-processing merged onto the Supervisor's own loop, the equivalent cancellation is
`self._stop()`'s own timeout-then-cancel fallback (`_shutdown_timeout`, default 30s — shorter
than `backoff_max`'s default 60s, so a long backoff sleep WILL be interrupted, not outlasted).
But that fallback only fires if `self._stop()` runs *before* the cumulative time spent stopping
sibling children could exceed a crash's backoff delay — which "stop own loop last" guarantees
CANNOT happen (children finish stopping first, by definition, before `self._stop()` is ever
reached). Concretely: with N children each taking close to their own `_shutdown_timeout`, a
crash's backoff sleep can complete and call `_restart_child` mid-teardown, resurrecting a child
the supervisor is in the process of shutting down.

**Correction: `Supervisor.stop()` reorders to stop its OWN loop FIRST** (`self._running = False`
→ `await self._stop()` → stop heartbeat monitor → stop children), restoring exact parity with
the pre-E4 "cancel crash-drain before touching children" guarantee, via a mechanism the base
class already provides for every other `AgentProcess`. `start()`'s own-loop-FIRST ordering is
UNAFFECTED (still correct — the mailbox must be live before anything can crash-report into it).
This is not a Q4 trigger: it is a resolvable, well-understood correction discovered by tracing
the mechanism before writing Phase B code (rather than by the halt-check test failing after the
fact) — and it makes `Supervisor` MORE consistent with every other `AgentProcess`, not less (no
agent subtype answers messages during its own shutdown; a Supervisor should not be a special
case). D-E4-3's *stop*-ordering clause is superseded by this entry; its *start*-ordering clause
stands. `test_no_resurrection_after_stop_during_backoff` (Halt-Check B proof #2) is written
against this corrected ordering.

**D-E4-9 — Q3 enforcement mechanism, and a precision refinement to its "error reply" phrasing
(found via trace before Phase C code, 2026-07-24).** Q3 was ratified as "hard-reject supervisor
suspension (error reply + WARNING)". Implementing it exactly as worded runs into a real
structural fact: `_agency.suspend` has **never been a request-reply message type** in this
codebase. `Runtime.suspend()` sends it via `bus.route()` (fire-and-forget, no correlation
tracking) and its own docstring states the resulting timeout-on-ask is intentional. At the exact
point `_message_loop` intercepts `_agency.suspend` — inline, before `handle()` runs —
`self._current_message` is not yet set, so `self.reply()` would itself raise. A constructed reply
message would have nowhere established to arrive: `Runtime.suspend()`'s caller is not polling a
correlation id.

Q3 is therefore enforced as **two separate mechanisms**, matching the two separate ways
suspension can be requested, not one:

1. **Direct API call — `Supervisor.suspend()` override raises immediately.** This IS a genuine
   hard reject: synchronous, catchable, for any caller holding a `Supervisor` reference.
2. **Message path — `_agency.suspend` logs a loud WARNING and drops.** This matches, rather than
   invents, the existing fire-and-forget contract every other `AgentProcess` already has for
   this exact message type — it is not a weaker guarantee than Q3 intended, it is the accurate
   description of the one that was actually implementable given the pre-existing wire contract.

**Mechanism for (2):** a new one-boolean hook on `AgentProcess`, `_suspend_allowed() -> bool`
(default `True`), checked by the shared `_message_loop` immediately before its existing
`_agency.suspend` branch. `Supervisor` overrides it to `False`. This is deliberately NOT a
`Supervisor`-only override of `_message_loop` itself (~90 lines, high drift risk over time) — one
hook, default-preserving for every existing subclass, checked in exactly one place. `resume()` is
left unmodified: since suspend is rejected outright, a `Supervisor` can never reach SUSPENDED, so
the base `resume()`'s existing no-op-when-not-suspended guard already covers it.

## 7. Compatibility & behavior-change ledger

| Change | Kind | Notes |
|---|---|---|
| Fresh-instance restart | **Behavior** | The headline; closes the documented A1 caveat; direct object refs stale (anti-pattern already) |
| B3 window-based backoff | **Behavior** | Backoff decays when the window empties |
| Process-level heartbeats | Protocol (internal) | Skew fallback for one minor version |
| Supervisors registered/addressable | Additive | New names in registry (marker capability) |
| `wait=True` non-blocking DynSup | Fix | Caller-visible latency improves; API identical |
| Public API (`__init__.py`, ctors, YAML schema) | **Unchanged** | Verified against `test_packaging` + docs |

## 8. Testing strategy

- All 1,106 unit + 185 integration tests pass or are *consciously* updated (each update justified
  in the PR body — the v0.8.x reviews found tests pinning bugs three times).
- **Flips `test_restart_resets_instance_variables`** — the last strict-xfail tracker.
- New per phase: P1 engine property tests (window/backoff math, incl. B3 decay); P2 the edge
  inventory of §4 + rewire-completeness (llm/tools/store/credentials/metrics on the new
  incarnation) + mailbox-order carry-over + `get_agent` freshness; P3 busy-remote-agent NOT
  restarted (the A6 false-positive, finally a green test) + fast remote-crash detection + skew
  fallback; P4 supervisor ask/introspection + spawn-during-slow-on_start concurrency + suspend-
  a-supervisor rejection.
- Cross-platform: P3 exercised over real ZMQ in the integration suite (Docker Linux check as in
  V3's verification).

## 9. Open questions for maintainer sign-off

- **Q1 — Fresh-instance reference staleness:** accept documented staleness of user-held object
  refs (recommended), or add a deprecation-style warning when `get_agent` returns a replaced
  instance? (Recommend: no warning — routing by name is already the only supported pattern.)
- **Q2 — Skew tolerance:** one minor version (v0.9 runtime ↔ v0.8 worker) via announce-flag
  fallback — sufficient? (Pre-1.0; recommend yes.)
- **Q3 — Suspending supervisors:** hard-reject (recommended) vs allow-with-warning?
- **Q4 — P4 in v0.9.0 or v0.9.1:** include actorization in this release (recommended — it is the
  "endgame" and B4 rides on it), with the explicit cut line that P1–P3 ship alone if P4's
  implementation review finds surprises?

## 10. Non-goals

- Preemptive scheduling / BEAM reductions (documented boundary, unchanged).
- Restarting for mailbox-depth/backlog (policy → Presidium/Medicus; we only *report* depth).
- Cross-process suspend, `RETRY_AFTER` delay lanes, at-least-once ZMQ routes (tracked elsewhere).
- Hot code reload (see `self-healing.md` — new code still requires a new OS process).
