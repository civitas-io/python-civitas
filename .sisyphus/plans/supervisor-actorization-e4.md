# Implementation Plan — E4: Supervisor Actorization (D6 + B4)

**Parent plan:** `.sisyphus/plans/supervision-endgame-v0.9.0.md` (E4 work package, expanded here
per maintainer request — this is the train's only *architectural* change, so it gets its own
ground truth and phase gates).
**Design:** `supervision-endgame.md` §6 + §6.1 (D-E4-1…5, recorded 2026-07-24).
**Cut line (Q4, ratified):** any structural surprise in Phase A or B ⇒ HALT, ship E1–E3 as
v0.9.0, re-plan E4 for v0.9.1. The halt criteria are explicit per phase below.

## Ground truth (verified file:line, HEAD = e832ff3)

- `supervisor.py`: `class Supervisor` :79 (plain object) · `_crash_queue` :128 ·
  `start` :209 / `stop` :234 (drain-task lifecycle to delete) · `_on_child_done` :329 ·
  `_drain_crashes` :345 (~60 lines: dequeue guard, stale-skip, H2 loud-failure wrapper —
  ALL semantics must survive relocation) · `_handle_crash` :561 (E1 engine verdict + E2
  fresh-restart calls — body untouched, only its caller changes) · `_escalate` :709 (parent
  `_crash_queue.put_nowait` → D-E4-1 side-table + bus trigger) · `DynamicSupervisor` :803 ·
  `_handle_spawn` :932 (`wait=True` blocking region for B4).
- `process.py`: `Mailbox.__init__` :78 (priority queue hardcoded 100 → D-E4-2 param) ·
  `Mailbox.put` :83 (async-only → add `put_nowait`) · `_message_loop` system-type dispatch
  (child_crashed flows to `_dispatch → handle()`, no loop changes needed) · AgentProcess ctor
  kwargs (Supervisor's ctor must feed `mailbox` params through or construct directly).
- `runtime.py`: `root.start()` call :785; supervisor wiring block (bus/registry/tracer +
  E2 `_wire_child`/replaced-callbacks) precedes it; agent registration + `setup_agent` loops
  precede it — supervisors join BOTH loops (registration with `SUPERVISOR_CAPABILITY`,
  `setup_agent` for mailbox delivery).
- `messages.py`: `SYSTEM_MESSAGE_TYPES` — add `_agency.child_crashed`.
- Test surface (measured): 47 refs across 5 files; test_supervisor.py 36, gaps 8,
  process 2, m2_6 2 (adapters — likely restart-path mock), process_liveness 1.
- Worker-hosted supervisors: `DynamicSupervisor` in a Worker — ALREADY an agent; E4 does not
  touch Worker wiring. Static Supervisors never live in Workers (topology builds them only in
  the Runtime process) — confirmed: `worker.py` hosts agents list, no Supervisor instances.

## Correctness constraints

1. **Exception/Task objects never enter Message.payload** (D-E4-1). The side-table is the ONLY
   carrier; the message carries `{event_id}`. Side-table entries are popped exactly once;
   an unknown event_id (stale after stop/start cycle) is a logged no-op.
2. **Crash enqueue is sync + lossless** (D-E4-2): `Mailbox.put_nowait` onto an unbounded
   priority queue for supervisors. Grep-verify no `await` in `_on_child_done`.
3. **Own-loop-first ordering** (D-E4-3): a child crashing in the window between its start and
   the supervisor's loop... cannot exist — loop starts first. The startup-order test crashes a
   child from INSIDE its on_start (wait=False style) and asserts the crash is processed.
4. **All `_drain_crashes` semantics survive**: sequential processing (mailbox FIFO+priority ✓),
   stale-incarnation skip (task from side-table vs `_child_tasks` ✓), restart-failure →
   ERROR log + escalate-to-parent / terminal at root ✓, post-stop discard (mailbox dies) ✓.
   Each maps to an existing test that must pass post-migration.
5. **Supervisors are registered but constrained**: marker capability; glob-hygiene unaffected
   (supervisor names are user-visible, intended); `suspend` hard-rejected (Q3); llm/tools NOT
   injected (Runtime's `_wire_child` is for agent children — supervisors keep their dedicated
   wiring block).
6. **Root bootstrap**: `Runtime.start()` calls `setup_agent(sup)` for every supervisor before
   `root.start()`; root's `start()` starts its own `_start()` before touching children.
   `Runtime.stop()` unchanged (root.stop() now also stops root's own loop — last).
7. **B4 continuation lifecycle**: deferred-reply tasks tracked in `_pending_child_tasks`
   (cancelled on stop, exactly like announce-after-start); reply construction mirrors
   `bus.teardown_agent`'s error-reply pattern (no `self.reply()` — the current message is gone
   by continuation time).

## Phases (each = commit; halt-checks between)

### A — ✅ DONE, HALT-CHECK A PASSED
Deltas: found D-E4-6 (Supervisor.stop() vs inherited AgentProcess.stop(name,...) name collision)
— verified safe (the inherited method could only ever raise for a Supervisor instance, since
_wire_dyn_sup never wires _dynamic_supervisor_name onto Supervisor nodes) and resolved with a
justified type:ignore rather than a breaking rename. Zero test regressions: 1130/1130 unit +
full integration, verified on BOTH macOS and Linux (docker). Proceeding to Phase B.

### A — Mailbox + Supervisor-as-agent skeleton (original)
1. `Mailbox(maxsize=1000, priority_maxsize=100)` + `put_nowait()` (+ unit tests).
2. `class Supervisor(AgentProcess)`: ctor calls `AgentProcess.__init__(name,
   priority_maxsize=0)`-equivalent; keeps engine/children/callbacks state; `handle()` dispatch
   per D-E4-4 (crash processing initially STILL via old queue — skeleton compiles + full suite
   green with zero behavior change).
   **HALT-CHECK A:** if AgentProcess inheritance breaks constructor compat or any of the 5
   knob-properties/engine facade — reassess (this is the "structural surprise" detector).
3. Runtime: register + setup_agent for supervisors; startup-order audit test.

### B — Control-plane swap

**Pre-resolved before coding (design §6.1 D-E4-7, 2026-07-24 walkthrough) — no open judgment
calls remain:**
- **Test-authoring heuristic for the 47-ref migration:** tests asserting HANDLING logic (strategy
  dispatch, budget/backoff verdicts, restart calls) keep calling `_handle_crash` directly — its
  body doesn't change. Only tests whose subject IS the delivery path itself (crash-queue/
  drain-task internals, stale-incarnation skip, post-stop discard) migrate or get superseded by
  the two named proofs below. Expect the ~47 references to split roughly: majority unchanged
  (they call `_handle_crash` already), a smaller set migrated to mailbox/side-table assertions,
  and the drain-task-specific tests replaced outright by items 6–7.

1. Side-table (`_pending_crash_events`) + `_agency.child_crashed` in SYSTEM_MESSAGE_TYPES.
2. `_on_child_done` → side-table + `put_nowait` self-trigger; `handle()` routes to the (renamed)
   `_process_crash_event` = old drain body minus the dequeue loop.
3. `_escalate` → parent side-table + bus trigger to parent name (fallback: direct parent-mailbox
   put_nowait when bus is None — bare-Supervisor tests).
4. DELETE `_crash_queue`/`_crash_drain_task`/`_drain_crashes`; start/stop drop drain lifecycle,
   adopt own-loop-first ordering.
5. Migrate test refs per the heuristic above (each migration justified in the PR body).
6. **NEW test (Halt-Check B proof #1):** `test_bare_supervisor_crash_delivery_without_bus` — a
   `Supervisor` with `_bus=None` still delivers a real child crash through `_on_child_done` →
   side-table → `put_nowait` → own mailbox → `handle()`, with no transport involved (self-
   delivery is local `Mailbox` traffic — confirmed safe by Phase A's standalone-loop finding).
7. **NEW test (Halt-Check B proof #2):** `test_no_resurrection_after_stop_during_backoff` —
   crash a child with a long backoff, call `stop()` mid-sleep, assert no restart and no task
   leak. Turns the candidate mechanism ("own `_stop()`, already last per Phase A, halts further
   crash-message consumption") into a verified fact.
   **HALT-CHECK B:** if proof #1 or #2 cannot hold without weakening a guarantee — halt per Q4:
   ship E1–E3 as v0.9.0, re-plan E4 for v0.9.1 with the specific failure recorded in the design
   doc's §6.1.

### C — Introspection + Q3 + registration polish
`civitas.supervision.status` handler + test · suspend hard-reject (method + `_agency.suspend`
path) + tests · `SUPERVISOR_CAPABILITY` constant + registration + `print_tree` unchanged-check.

### D — B4 deferred replies
`_handle_spawn` wait=True → stash envelope + readiness continuation → routed reply; tests:
concurrent despawn/status during slow `on_start` (head-of-line gone), reply timing semantics
unchanged, continuation cancelled on stop.

### E — Full verification
Unit + integration + Docker ZMQ spot-check + coverage ≥85% + mypy; CHANGELOG (additive:
supervisors addressable; internal: control plane) ; design-doc addendum for any deltas.

## Definition of done
- [ ] `_drain_crashes` and `_crash_queue` grep-clean from `civitas/`
- [ ] All 4 constraint-4 semantics tests green post-migration; startup-order + bare-mode tests new
- [ ] `ask("root", status)` works end-to-end; suspend-supervisor rejected (both paths)
- [ ] B4: spawn-during-slow-on_start concurrency test green
- [ ] Full suites green; every migrated test justified in the PR body
- [ ] Q4 outcome recorded in parent plan (shipped in v0.9.0 vs halted)
