# Design: Supervision Core Hardening (Actor-Model Fidelity)

**Status:** ✅ ACCEPTED for v0.8.0 (2026-07-21) — findings verified by regression tests; decisions ratified (see §2.1); implementation plan in `.sisyphus/plans/supervision-hardening-v0.8.0.md`. D5-structural and D6 remain DRAFT (own ceremony in v0.9).
**Author:** architecture review session (2026-07-21)
**Scope:** `civitas/supervisor.py`, `civitas/process.py`, `civitas/registry.py`, `civitas/worker.py`, `civitas/bus.py`
**Tracking:** issues [#28](https://github.com/civitas-io/python-civitas/issues/28), [#29](https://github.com/civitas-io/python-civitas/issues/29), [#30](https://github.com/civitas-io/python-civitas/issues/30), [#31](https://github.com/civitas-io/python-civitas/issues/31), [#32](https://github.com/civitas-io/python-civitas/issues/32), [#33](https://github.com/civitas-io/python-civitas/issues/33), [#34](https://github.com/civitas-io/python-civitas/issues/34), [#35](https://github.com/civitas-io/python-civitas/issues/35) · regression tests `tests/unit/test_actor_model_gaps.py` (strict xfail)

> **One-line framing:** Civitas advertises Erlang/OTP fault-tolerance semantics ("let it crash,
> supervisor handles the rest"). A code-level review found a gap between those advertised semantics
> and the implementation — several reachable scenarios where the guarantee fails silently. This doc
> is the findings catalog and the corrective design. The distributed edge (R1–R9) was designed
> carefully; the *local supervision core* (Phase 1, pre-design-doc era) is where the debt lives.

---

## 1. Findings catalog

Severity: 🔴 correctness bug (guarantee fails), 🟡 semantic gap (works, but not the advertised
model), 🔵 limitation to document.

### A. Actor-model violations

| ID | Sev | Finding | Where |
|----|-----|---------|-------|
| A1 | 🔴 | **Restart reuses the dirty instance.** `_restart_child()` restarts the *same* `AgentProcess` object: instance variables survive, un-checkpointed `self.state` survives (`_restore_state()` only overwrites when a checkpoint exists), and the mailbox is retained. Corrupted in-memory state that caused a crash is resurrected — restart loops until the budget is exhausted. AGENTS.md anti-pattern #5 claims the opposite ("instance variables reset on restart"). | `supervisor.py::_restart_child`, `process.py::_restore_state` |
| A2 | 🔴 | **Escalation is a no-op under ONE_FOR_ONE** ([#28](https://github.com/civitas-io/python-civitas/issues/28)). `_restart_child()` returns early for `Supervisor` children, so an escalated subtree is never restarted; it stays dead while the system reports normal operation. The escalated supervisor's `_restart_timestamps` is also never cleared, so any later restart re-escalates immediately. | `supervisor.py::_restart_child`, `_escalate` |
| A3 | 🔴 | **Restart drops capability registrations** ([#29](https://github.com/civitas-io/python-civitas/issues/29)). All restart paths re-register bare (`register(name)`), losing `capabilities` + `capability_metadata` (and YAML overrides, which only `Runtime` knows). `send_capable()` / `find_by_capability()` silently break after the first crash-restart. `Worker._on_restart_command` has the identical bug. | `supervisor.py` (3 sites), `worker.py` |
| A4 | 🟡 | **Blocking `ask()` inside `handle()` + one-message-at-a-time loop → deadlock on cycles.** While A awaits `ask("B")`, A processes nothing; if B (transitively) asks A back, both stall until the 30 s timeout crashes one side. No selective receive, no cycle detection, no documentation of the hazard — the README's own orchestrator pattern is one hop away from it. | `process.py::_message_loop` |
| A5 | 🟡 | **Bounded mailboxes + blocking sends → backpressure deadlock cycles.** `Mailbox.put()` blocks when full and `InProcessTransport.publish()` awaits the recipient inline, so a sender's `handle()` blocks inside a full recipient's mailbox. A↔B full-mailbox cycles deadlock with no detection. Priority queue is hardcoded `maxsize=100`; `broadcast()` is sequential (head-of-line). | `process.py::Mailbox`, `transport/inprocess.py` |
| A6 | 🔴 | **Heartbeats measure mailbox latency, not liveness** ([#31](https://github.com/civitas-io/python-civitas/issues/31)). Sent at priority 0, acked only at a message-loop boundary: a busy agent (~15 s of legitimate `handle()` work at defaults) or a SUSPENDED remote agent is falsely declared crashed and restarted. Restart backoff sleeps inline in the heartbeat loop, stalling monitoring of all other remote children. | `supervisor.py::_heartbeat_loop` |
| A7 | 🔴 | **Local agents have no hang detection at all.** Supervision of local children relies solely on task exceptions; a `handle()` that hangs forever never raises, so the supervisor sees a healthy child indefinitely. There is no per-message watchdog. Remote agents (flawed heartbeats) are better supervised than local ones. | `supervisor.py::_start_child` |
| A8 | 🔵 | **Cooperative scheduling limits fault isolation.** All actors share one event loop; one blocking/CPU-bound `handle()` stalls every agent, supervisor, heartbeat ack, and gateway in the process. BEAM's preemptive scheduling is what makes OTP's isolation real; asyncio cannot replicate it. Must be documented as a boundary of the model, with `asyncio.to_thread` / worker-process guidance. | architecture |

### B. Supervision-layer structure

| ID | Sev | Finding | Where |
|----|-----|---------|-------|
| B1 | 🟡 | **Two divergent supervision implementations.** Static `Supervisor` is not an actor (direct method calls, private-state mutation, cannot be messaged/suspended/traced); `DynamicSupervisor` *is* an actor with separately implemented restart machinery and different semantics (never escalates). Duplicated logic, diverging behavior. | `supervisor.py` |
| B2 | 🔴 | **Crash handling is unserialized and failures vanish** ([#30](https://github.com/civitas-io/python-civitas/issues/30)). `_handle_crash` runs as a fire-and-forget task whose exception is never retrieved: a failed restart leaves the child dead with zero log output. Concurrent crashes under ONE_FOR_ALL race (`double _start()`, registry `ValueError` swallowed). During a nested stop/start window `_running=False` drops sibling crashes entirely. | `supervisor.py::_on_child_done` |
| B3 | 🟡 | **Inconsistent restart accounting.** Budget window (`_restart_timestamps`) is supervisor-wide (Erlang-style intensity), but backoff uses the per-child *lifetime* count (`_restart_counts`, never windowed or reset) — surprising delays; neither model is documented as intended. | `supervisor.py::_compute_backoff` |
| B4 | 🟡 | **DynamicSupervisor head-of-line blocking.** `wait=True` spawn awaits child readiness inside the supervisor's own `handle()` — one slow `on_start()` blocks all spawn/despawn/stop traffic for up to 30 s; a child messaging its own spawning DynSup during `on_start` deadlocks. | `supervisor.py::_handle_spawn` |

### C. Secondary issues

| ID | Sev | Finding | Tracking |
|----|-----|---------|----------|
| C1 | 🟡 | `ErrorAction.RETRY` re-enqueues at the back of the mailbox — FIFO ordering silently broken for retries | [#32](https://github.com/civitas-io/python-civitas/issues/32) |
| C2 | 🟡 | Wall clock (`time.time()`) used for restart windows / TTL / suspend markers; streams correctly use `monotonic` — clock jumps distort budgets | this doc |
| C3 | 🔵 | Up to 4 serialization passes per in-process message (`json.dumps` validation + msgpack route + deserialize/reserialize for `reply_to` injection) | this doc |
| C4 | 🟡 | `sender="_runtime"` is unroutable — agent replies via `message.sender` crash | [#33](https://github.com/civitas-io/python-civitas/issues/33) |
| C5 | 🟡 | `register_b64` pollutes the routing table with phantom non-routable entries (and drops the key it was given) | [#34](https://github.com/civitas-io/python-civitas/issues/34) |
| C6 | 🔵 | `broadcast("*")` glob-matches system entries (`_agency.worker.restart`, gateways, topology server) | this doc |
| C7 | 🔵 | Core violates its own encapsulation rule: `DynamicSupervisor` reads `bus._registry/_serializer/_transport`; `Runtime` swaps `cs.bus._serializer` post-hoc | this doc |
| C8 | 🔵 | Delivery semantics undocumented: at-most-once, in-flight message lost on crash, queued messages survive restart, no DLQ | this doc |
| C10 | 🟡 | README contradicts AGENTS.md/pyproject on provider extras + adapter imports | [#35](https://github.com/civitas-io/python-civitas/issues/35) |

---

## 2. Corrective design

### 2.1 Ratified decisions (maintainer, 2026-07-21)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| D1 restart semantics | **Staged (b)→(a)** — v0.8.0 ships (b) (un-checkpointed `self.state` reset + documented instance-var survival) plus invisible ctor-capture groundwork (`AgentProcess.__new__` records `(cls, args, kwargs)`); full fresh-instance (a) lands in v0.9 with D6, where the required Runtime↔Supervisor re-wiring is being reworked anyway | (a)'s real cost is ctor-spec capture + re-injection + bus re-subscription + reference staleness — architecture work, not a bugfix; (b) kills the corrupted-state restart-loop death spiral now at ~zero risk |
| C1 / #32 RETRY | **Retry in place** — re-run `handle()` immediately inside the dispatch, no mailbox round-trip; FIFO preserved; backoff is the user's job (`await asyncio.sleep` in `on_error`) | The back-of-queue behavior was an accident, not a designed unordered semantic |
| C4 / #33 `_runtime` | **Sink + diagnostics** — register `_runtime` (not an agent: bare subscription); WARNING log on receipt; error-reply when the message carries a `correlation_id` so `ask("_runtime")` fails fast instead of timing out | Crash-proof for `send`, fail-fast for `ask`, every occurrence becomes a discoverable code smell |
| D5 watchdog | **Opt-in `handle_timeout: float \| None = None`** per-agent + YAML; `TimeoutError` through the normal `on_error` path; async-only detection documented | Any finite default is wrong for someone; zero behavior change on upgrade |
| Process | Plan file + this ratification; no external review cycle for v0.8.0 (bug-class work with pre-written regression tests); full ceremony resumes for v0.9 D6/(a) | The xfail harness is a stricter gate than a plan review for this class of change |

Additional ground truth found during planning: `LocalRegistry.register_b64` (C5/#34) has **zero
callers** — both apparent call sites target `KeyRegistry.register_b64`. C5 is dead-code removal
(anti-pattern 16), not a behavior change.

### D1 — Restart semantics: fresh-start protocol (A1)

Choose and implement one of two models; the review recommends **(a)**:

- **(a) OTP-faithful fresh actor (recommended).** On restart the supervisor re-instantiates the
  child from its class + constructor args (captured at first registration as a *child spec*, the
  OTP `child_spec` concept — Civitas already has `_ChildRec` for dynamic children; extend the idea
  to static ones). Fresh instance → fresh instance vars, fresh `self.state` (then
  `_restore_state()` applies the last checkpoint), fresh mailbox **except** that the old mailbox is
  drained into the new one (preserving queued messages and the teardown-error-reply behavior of
  `bus.teardown_agent`). The checkpoint remains the *only* state that survives — which is exactly
  the documented contract.
- **(b) Documented resume semantics (cheaper).** Keep instance reuse but make `_run()` reset
  `self.state = {}` before `_restore_state()`, and rewrite AGENTS.md anti-pattern #5 to say
  instance variables *survive* restart and must not be trusted. This closes the state hole but
  leaves the instance-variable hole open.

Either way: **fix AGENTS.md**, which currently documents behavior the code does not have.

### D2 — Escalation restarts the subtree (A2, #28)

`_restart_child()` gains a `Supervisor` branch:

```
if isinstance(child, Supervisor):
    await child.stop()                    # idempotent for already-dead children
    child._restart_timestamps.clear()     # fresh budget for the new incarnation
    child._restart_counts.clear()
    await child.start()
```

`_restart_all_children` / `_restart_rest_for_one` already call `stop()/start()` on supervisors but
must also clear the budget (the "fresh incarnation" rule — an OTP-restarted supervisor is a new
process with a zeroed intensity window).

### D3 — Registration snapshot preserved across restart (A3, #29)

Registry gains `snapshot = registry.lookup(name)` → re-register from the snapshot
(`capabilities`, `capability_metadata`, `address`), or a single `registry.reregister(name)`
that atomically bumps the entry without losing fields (also avoiding the deregister/register
listener churn Presidium currently sees). Apply to all three `Supervisor` restart paths and
`Worker._on_restart_command`.

### D4 — Serialized, observable crash handling (B2, #30)

- One **crash-processing queue per supervisor**: `_on_child_done` enqueues `(name, exc)`; a single
  long-lived task drains it sequentially (OTP supervisors process EXIT signals serially). This
  removes the concurrent-restart races and the `_running=False` drop window in one move.
- The drain task wraps each restart in `try/except`: on failure, log at ERROR with child name and
  exception, fire crash callbacks, and **escalate** (a supervisor that cannot restart its child is
  itself failing).

### D5 — Liveness redesign (A6/A7, #31)

- **Stopgap (ship first):** heartbeats at `priority=1` — suspended agents ack them (priority-only
  drain), loaded agents ack between messages. Move the backoff sleep out of the heartbeat loop
  (schedule restarts on the crash queue from D4).
- **Structural:** liveness is per-*process*, not per-agent-mailbox — a Worker-level heartbeat
  responder that answers off-loop (it is not an agent), so agent busyness never gates liveness.
  Per-agent health then becomes a separate, opt-in **watchdog**: an optional
  `handle_timeout: float` on `AgentProcess`; the dispatch wraps `handle()` in
  `asyncio.timeout(handle_timeout)`, converting a hung handler into a normal crash the supervisor
  can see (closes A7 for local agents too).

### D6 — Unify supervision as actors (B1) — larger, phased

Static `Supervisor` becomes (or is wrapped by) an agent process, sharing one restart engine with
`DynamicSupervisor`: one budget/window/backoff implementation, one escalation rule, one crash
queue. This is a v0.9/v1.0-scale refactor and needs its own plan; D2–D4 are deliberately designed
to be portable into it.

### D7 — Semantics documentation pass (A4/A5/A8, C8)

A new `docs/messaging-semantics.md` (or a section in `docs/messaging.md`) stating explicitly:
at-most-once delivery; in-flight loss on crash; queued-message survival across restart; RETRY
ordering (per #32 resolution); the ask-cycle deadlock hazard and the `asyncio.timeout` +
fire-and-forget alternatives; bounded-mailbox backpressure and its deadlock mode; cooperative
scheduling boundaries and `asyncio.to_thread` guidance. Every claim in README's comparison table
must be reconciled against this document.

### D8 — Hygiene batch (C1–C7)

Small independent fixes, one PR each: retry-in-place or documented reordering (#32); `monotonic`
for windows/TTL-relative checks (C2); single-validation fast path for in-process messages (C3 —
skip `json.dumps` when the serializer will fail loudly anyway, or validate once at `route()`);
`_runtime` sink or doc (#33); `register_b64` removal in favor of `KeyRegistry` (#34); glob
broadcast excludes `_agency.*` names (C6); accessor methods on `MessageBus` for what
`DynamicSupervisor` needs (C7).

---

## 3. Priorities

| Priority | Items | Rationale |
|----------|-------|-----------|
| **P0** | D2 (#28), D4 (#30), D3 (#29) | The flagship guarantee ("supervisor handles the rest") is false in reachable scenarios; recovery-path routing corruption |
| **P1** | D5 stopgap (#31), D1 decision + implementation | False-positive restarts in production; let-it-crash does not deliver a clean heap |
| **P2** | D7 docs pass, D8 hygiene batch, #35 README sweep | Semantics users can actually rely on |
| **P3** | D5 structural, D6 unification | Larger refactors; own design/plan cycle each |

## 4. Verification

- `tests/unit/test_actor_model_gaps.py` — six `strict xfail` tests, one per verified finding
  (A1×2, A2, A3, B2, A6). Fixing a bug flips its test to XPASS(strict), which **fails the suite**
  until the marker is removed — the tracking cannot go stale.
- Each Dn fix must convert its xfail test(s) to plain tests in the same PR.
- D4 additionally needs a concurrent-crash stress test (N children crashing in the same tick,
  ONE_FOR_ALL) asserting exactly one restart cycle.

## 5. Non-goals

- True preemptive scheduling / BEAM-style reductions (impossible on asyncio; documented instead).
- Selective receive (would change `handle()` semantics; revisit only with strong demand).
- Unbounded mailboxes by default (bounded + documented backpressure retained; alarm/DLQ optional later).
- In-place hot code reload (see `self-healing.md` — already ruled out).
