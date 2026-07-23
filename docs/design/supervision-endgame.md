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
