# Design: Durable Suspension (v0.5.0, Bucket B)

**Status:** APPROVED 2026-07-03 — implementing. Authoritative spec is the "Final Design" section
directly below; the "Review findings" and "Design Decisions (D1–D6)" sections beneath it are
retained as rationale/history (D-sections partially superseded — defer to Final Design on any
conflict).
**Author:** Sisyphus
**Last updated:** 2026-07-03

---

## FINAL DESIGN (approved) — implementation spec

Decisions locked with the maintainer. This section is authoritative.

**Governing constraint:** `SUSPENDED` was previously removed as dead API (F02-6). It may only be
re-introduced fully wired — every transition, mailbox behavior, supervisor interaction, and
persistence path defined and tested. See the F02-6 section below for why.

**S1 — `ProcessStatus.SUSPENDED` re-introduced, fully wired.** Transitions: `RUNNING → SUSPENDED`
(suspend), `SUSPENDED → RUNNING` (resume), `INITIALIZING → SUSPENDED` (restore sees marker),
`SUSPENDED → STOPPING → STOPPED` (shutdown/halt/despawn). Update `test_suspended_removed_from_enum`
and `test_expected_states_present`; commit message must reference F02-6.

**S2 — Suspend is a non-blocking flag checked at the loop boundary.** `self.suspend(reason)` marks
intent and returns immediately (no deadlock; no extra business message dispatched). External
`runtime.suspend(name, reason)` sets the same flag via a priority control message. `_message_loop`
guard becomes `while self._status in (RUNNING, SUSPENDED)`; at the top of each iteration, if the
suspend flag is set and status is RUNNING, transition to SUSPENDED (fire `on_suspend`) before
pulling the next business message.

**S3 — While SUSPENDED, drain ONLY the priority queue; leave the business queue in place.** This
preserves message ordering and backpressure for free and makes resume trivial (flip status; normal
`get()` resumes original FIFO order). Requires a priority-only wait variant on `Mailbox` (wait on
`_notify` but only return priority-queue messages; avoid busy-loop / spurious-wakeup footgun,
Oracle finding #10). Control types that MUST be handled while suspended and therefore MUST be
priority≥1: `_agency.shutdown` (already prio 1), `civitas.eval.halt` (verified prio 1,
evalloop.py:215), `_agency.resume` (new, prio 1). Business messages and `civitas.eval.correction`
(prio 0) stay buffered.

**S4 — Durable marker lives INSIDE `self.state` under a reserved key, one atomic write.** Key:
`self.state["_civitas.suspended"] = {"reason": ..., "since": ..., "approver": None}`. Persisted via
the existing `checkpoint()` path (`store.set(name, self.state)`). One atomic write means marker +
any user `pending_action` can never desync across a crash (Oracle finding #1 / governance-critical).
On `_start()`, after `_restore_state()`, if the marker is present the agent enters `SUSPENDED`
instead of `RUNNING` (S1). Durable only with a persistent store; with the default `InMemoryStateStore`
it is not — same caveat as `checkpoint()` today, documented loudly.

**S5 — Write-ahead safety on suspend.** Pause dispatch in-memory FIRST (immediate safety: stop the
agent acting), THEN persist the marker. If the persist fails, stay paused in-memory and emit a
degraded-durability WARNING — never fall back to RUNNING. Immediate "stop acting" outranks durability.

**S6 — Resume REQUIRES an explicit approver signal (maintainer decision).** `resume()` /
`runtime.resume()` take a required non-empty `approver: str`. The `_agency.resume` control message
carries it. Rules:
- `resume(name)` with no/empty `approver` → raise `ValueError` (resume without an approver is
  never valid — presence of a checkpointed `pending_action` alone is NOT authorization).
- On resume, the marker is cleared and `on_resume(approver)` fires with the approver identity.
- The D6 pending-action pattern must only execute after `on_resume` delivers a valid approver; the
  runtime records the approver in the resume `AuditEvent` (S9). This closes Oracle finding #4
  (resume-authorization): an agent must not act on mere local state; a human/policy approver must
  be named.

**S7 — Supervisor/lifecycle changes are REQUIRED (the draft's "zero Supervisor changes" was false).**
- `_stop()` (process.py:814) must handle `SUSPENDED` — currently early-returns, so a suspended agent
  leaks on shutdown and double-loops under ONE_FOR_ALL/REST_FOR_ONE. `_stop()` must actually stop a
  SUSPENDED agent (deliver shutdown / cancel task).
- Supervisor restart-set membership guards (`_restart_all_children`, `_restart_rest_for_one`) must
  treat SUSPENDED correctly (stop then restart, no duplicate loop).
- Restore-into-SUSPENDED must still set `_running_event` (else `_start()` hangs) and must NOT fire
  `on_suspend` (it's a restore, not a fresh suspend); it MAY fire nothing or a distinct
  `on_restored_suspended` — keep it simple: fire nothing on restore.

**S8 — Marker lifecycle: clear on permanent removal, keep on graceful shutdown.**
- `despawn()` / permanent removal (restarts exhausted, NEVER) → CLEAR the marker (else a future
  agent reusing the name resurrects as suspended — Oracle finding #2 zombie suspension).
- Graceful full-runtime shutdown / crash-restart → KEEP the marker (that is the cross-restart point).
- Crash-while-suspended that restarts back into SUSPENDED should NOT consume the restart budget
  toward exhaustion (Oracle finding #5) — or at minimum must be documented; simplest acceptable v1:
  document the behavior, defer budget-exemption if it complicates the supervisor too much (flag in PR).

**S9 — Observability + audit.** Emit `civitas.agent.suspend` / `civitas.agent.resume` spans and an
`AuditEvent` at each (governance-relevant; Presidium consumes). Resume audit records the approver.

**S10 — Scope trims (avoid re-creating the F02-6 dead-API smell).** NO `suspend_peer()`/`resume_peer()`
(no in-repo caller; add when one exists). Use `_agency.suspend` / `_agency.resume` (existing reserved
prefix), added to `SYSTEM_MESSAGE_TYPES` (messages.py). External entry points: `runtime.suspend(name,
reason="")` and `runtime.resume(name, approver)`. Idempotency: suspend-of-suspended keeps original
`since` (updates reason); resume-of-not-suspended is a safe no-op (but still requires an approver arg
for API consistency). `ask()` into a suspended agent times out (documented); fail-fast deferred.

### Implementation checklist (in dependency order)

1. `messages.py`: add `_agency.suspend`, `_agency.resume` to `SYSTEM_MESSAGE_TYPES`.
2. `process.py` `Mailbox`: add priority-only wait/get variant (S3, footgun #10).
3. `process.py` `ProcessStatus`: re-add `SUSPENDED` (S1).
4. `process.py` `AgentProcess`: `_suspend_requested` flag, reserved-key constant, `suspend(reason)`
   (non-blocking, write-ahead S5), `resume(approver)` (S6 validation), `on_suspend(reason)` /
   `on_resume(approver)` hooks (default no-op).
5. `process.py` `_message_loop`: guard `in (RUNNING, SUSPENDED)`; boundary flag check → transition;
   priority-only drain while SUSPENDED; handle `_agency.suspend`/`_agency.resume` control branches.
6. `process.py` `_start`/`_restore_state`: restore-into-SUSPENDED, set `_running_event`, no
   `on_suspend` on restore (S7).
7. `process.py` `_stop`: handle SUSPENDED (S7).
8. `supervisor.py`: restart-set membership for SUSPENDED; marker-clear on despawn/permanent-remove
   (S8); crash-while-suspended note (S8).
9. `runtime.py`: `suspend(name, reason="")` / `resume(name, approver)` external entry points.
10. observability spans + `AuditEvent` (S9).
11. tests: every interleaving in Oracle findings #1–#10 + S6 approver validation + restore path.
12. `CHANGELOG` + `milestones.md` (bucket B).

---

---

## Motivation

Civitas is integration point #8 in the Civitas→Presidium contract
([`boundary.md`](https://github.com/civitas-io/context)):

> **Durable suspension** — Civitas provides `agent.suspend()` / `agent.resume()`; Presidium
> consumes them for the HITL (human-in-the-loop) approval flow.

The scenario: an agent is about to take a consequential action (spend money, call a
destructive tool, send an irreversible message). Presidium's `PolicyEngine` returns
`REQUIRE_APPROVAL`. The agent must **stop processing** until a human or policy approves —
and that approval may take seconds, hours, or days, potentially spanning a process restart
or redeploy. When approval arrives, the agent resumes.

Today Civitas has no way to express "paused, awaiting approval." An agent is either RUNNING,
STOPPING, STOPPED, or CRASHED. This design adds a supervised, durable **SUSPENDED** state.

---

## Prior art in this repo: why `SUSPENDED` was removed (F02-6)

`ProcessStatus` **used to** have a `SUSPENDED` member. It was removed in commit `7b64ac4`
(F02 review, Apr 2026) — see `tests/unit/test_process.py::test_suspended_removed_from_enum`.

**It was not removed because suspension is a bad idea.** It was removed because it was *dead
API*: the enum value existed but nothing ever transitioned into or out of it, nothing checked
it, and no method set it. That is exactly [AGENTS.md anti-pattern #16](https://github.com/civitas-io/python-civitas/blob/main/AGENTS.md)
("Declaring API surface without wiring it up").

**This is the governing constraint of this design.** Re-introducing `SUSPENDED` is only
acceptable if it is *fully wired*: defined transitions in and out, defined mailbox behavior,
defined supervisor interaction, defined persistence, and tests covering each. A decorative
enum value would just recreate the exact defect F02-6 deleted. Every design decision below
exists to satisfy that bar.

---

## Review findings & revised design (post-Oracle, code-verified 2026-07-03)

This draft was pressure-tested by Oracle and the load-bearing claims verified against source.
**The original design below (D1–D6) is superseded where this section contradicts it.** Net: the
central premise was wrong, and the corrected design is actually *leaner*.

**The "zero changes to Supervisor" claim (D3) is FALSE — verified in code:**
- `_message_loop` guard is `while self._status == ProcessStatus.RUNNING` — setting `SUSPENDED`
  makes the loop *exit*, run `on_stop()`, and end the task. A `Supervisor` child then silently
  dies (no exception → no restart); a `DynamicSupervisor` child is read as `clean_exit` and
  permanently removed. So suspend must be a **flag checked at the loop boundary**, and the guard
  must become `while status in (RUNNING, SUSPENDED)`.
- `_stop()` (`process.py:814`) early-returns unless status ∈ {RUNNING, INITIALIZING}. A suspended
  agent is therefore **skipped on shutdown (task leak)**, and under `ONE_FOR_ALL`/`REST_FOR_ONE`
  the restart path calls `_stop()` (no-op) then `_start_child()` → **two live message loops for
  one agent**. `_stop()` and the restart-set membership guards must handle `SUSPENDED`.

**Revised decisions:**

- **Q1 (framing) — confirmed correct.** "Pause dispatch + checkpoint" is the ceiling; CPython
  can't serialize a live coroutine. Add: name the sanctioned finer-grained path (sans-io /
  explicit state machine) and one line on why Temporal-style event-sourced replay is out of scope
  (different programming model, not a suspension primitive).
- **Q2 (self-suspension) — REVERSED from the draft.** Do *not* forbid self-suspension. Implement
  `await self.suspend(reason=...)` as a **non-blocking flag** — it marks intent and returns; the
  loop transitions to SUSPENDED at the *next boundary*, before pulling another business message.
  This kills the deadlock AND the D6 dispatch-window race, and — the elegant part — **external
  `runtime.suspend()` sets the same flag**, so self- and external-suspend converge on one path.
- **Q3 (control-while-suspended) — neither drafted option; use priority-only drain.** The mailbox
  is already two queues (business=prio 0, control=prio 1). While suspended, drain **only the
  priority queue** and leave the normal queue in place. Free wins: ordering preserved (nothing
  dequeued → nothing reordered on resume), backpressure preserved (normal queue fills, senders
  block), trivial resume (flip status). Requires every must-handle-while-suspended control type be
  priority>0: `_agency.shutdown` ✓ (prio 1), `civitas.eval.halt` ✓ (prio 1, verified evalloop.py:215).
  Correction to D2: in-process agents have **no heartbeat** (heartbeats are remote-only, a v0.5
  non-goal); the "looks dead → restart" risk is the *loop exiting*, not missed heartbeats.
- **Q4 (persistence) — REVERSED from the draft.** Store the marker **inside `self.state`** under a
  reserved key (e.g. `self.state["_civitas.suspended"]`), riding the existing checkpoint path — not
  a separate top-level StateStore key. Decisive reason: **atomicity.** The HITL flow persists
  `pending_action` AND the suspend marker; two separate writes create a crash window where the
  agent restarts RUNNING with an unapproved pending action (the exact governance failure suspension
  prevents). One key = one atomic `set()` = window closed. Bonus: no `list_agents()` pollution
  (a separate top-level key shows up as a phantom agent in CLI/migrate). Reject the protocol
  extension (`set_suspended`/`get_suspended`) — forces every store incl. contrib to change for
  something the existing k/v surface already stores.
- **Q6 (scope) — trim.** Cut `suspend_peer()`/`resume_peer()` — no in-repo caller (Presidium uses
  `runtime.suspend()`), so it would be exactly the unwired-API smell (anti-pattern #16 / F02-6)
  this design invokes as its constraint. Use `_agency.suspend`/`_agency.resume` (existing reserved
  prefix), not a new `civitas.control.*` namespace; add them to `SYSTEM_MESSAGE_TYPES`.

**Additional holes to resolve before/while implementing (from Oracle, verified where cited):**

1. Restore-into-SUSPENDED collides with loop startup: `_message_loop` sets `RUNNING`
   unconditionally at entry and sets `_running_event`; the restore path must set status
   conditionally *and* still set `_running_event` (else `_start()` hangs).
2. Stale marker on permanent removal → zombie suspension: `despawn()`/permanent-remove must
   **clear** the marker; graceful full-runtime shutdown must **keep** it (that's the cross-restart
   point). This "gone forever" vs "will return" distinction doesn't cleanly exist today (`_stop()`
   serves both) — needs to be introduced.
3. StateStore write failure during suspend: pause in-memory *first* (immediate safety), then
   persist; on failure stay paused + emit degraded-durability warning; never fall back to RUNNING.
4. Resume authorization: D6 executes `pending_action` on mere presence — resume must be gated by an
   approver signal, not local state alone.
5. Crash-while-suspended restart-budget accounting: a suspended child knocked over by a sibling
   restarts into SUSPENDED but burns a restart; consider not counting restarts that land back in
   SUSPENDED (else a legitimately-suspended agent gets removed on exhaustion).
6. Buffered business messages can TTL-expire and be silently dropped on resume after a long
   suspension (my F01-3 ttl fix discards expired at `Mailbox.get()`) — documented behavior.
7. Idempotency: double-suspend (keep original `since`?) and resume-of-not-suspended (safe no-op).
8. Capability/broadcast black hole: `send_capable()`/`broadcast()` still select a suspended agent;
   messages buffer silently. Document the tradeoff.
9. `on_suspend()`/`on_resume()` semantics: fire on fresh suspend but NOT on restore-into-suspended;
   define behavior if `on_suspend()` raises mid-transition.
10. `_notify` spurious wakeups in priority-only mode (normal-queue puts set the shared event) —
    implementation footgun, needs a priority-only wait variant.

**Effort:** Medium (1–2 days). Most cost is the `_message_loop`/`_stop`/restart-path surgery and
the interleaving tests — NOT the enum or persistence. The draft's "AgentProcess-only" framing
would have under-estimated it.

---

## OTP analogy

| OTP | Civitas |
|-----|---------|
| `sys:suspend(Name)` | `runtime.suspend(name)` / `self.suspend_peer(name)` |
| `sys:resume(Name)` | `runtime.resume(name)` / `self.resume_peer(name)` |
| Suspended process stops handling messages; mailbox keeps buffering | Same |
| Suspension is **in-memory only** (lost if the process dies) | **Durable** — survives supervisor restart when a persistent StateStore is configured |

The durability is the deliberate departure from OTP. OTP suspension is a debugging/maintenance
tool measured in milliseconds; Civitas suspension is an approval-workflow primitive that must
outlive a crash-and-restart.

---

## The one clarification that shapes everything: what "durable" can and cannot mean

**Durable suspension pauses message *dispatch*. It does not snapshot a running `handle()`
coroutine.**

Python cannot serialize a suspended coroutine's stack. There is no way to freeze an agent
halfway through `handle()`, persist the half-executed frame, restart the process, and resume
that frame. Any design claiming otherwise is lying.

So "resume after a process restart" means precisely: the agent's process restarts, restores
its checkpointed `self.state` (existing mechanism), comes back up in `SUSPENDED` status
instead of `RUNNING`, and does **not** dispatch queued business messages until resumed. It
does **not** mean a mid-flight `handle()` call continues from where it was interrupted.

The consequence for the HITL "approve *this specific action*" case is D6 below: the agent must
checkpoint the pending action into `self.state` *before* suspending, and re-read it on resume.
The runtime cannot do this transparently.

This point must survive into the user-facing docs verbatim, or users will expect coroutine
snapshotting and be surprised.

---

## Design Decisions (PROPOSED — for review)

### D1 — Re-introduce `SUSPENDED` as a fully-wired state

`ProcessStatus` gains `SUSPENDED` back, with explicit transitions:

```
RUNNING    --suspend()-->  SUSPENDED
SUSPENDED  --resume()-->   RUNNING
INITIALIZING --(restore sees suspend marker)--> SUSPENDED   (start-up path, D5)
SUSPENDED  --shutdown/halt-->  STOPPING --> STOPPED
```

`test_suspended_removed_from_enum` and `test_expected_states_present` will be updated (the
latter's expected set becomes the 5 current states + `SUSPENDED`). The commit message must
reference F02-6 explicitly so the history reads coherently: "removed as dead API in F02-6,
re-introduced fully-wired for durable suspension."

### D2 — Suspension pauses dispatch; the mailbox keeps buffering

When `SUSPENDED`, `_message_loop()` stops dequeuing/dispatching **business** messages. The
mailbox continues to accept incoming messages up to its existing bound (backpressure applies
when full, unchanged). This is OTP-faithful and matches caller expectations: a message sent to
a temporarily-paused agent should eventually be handled, not rejected.

**Critical carve-out — control and system messages must still be processed while suspended,**
or a suspended agent looks dead to its supervisor and gets restarted (which would defeat
suspension). While `SUSPENDED`, the loop still handles:

- `_agency.heartbeat` → must still ack, or heartbeat monitoring flags it as crashed (see D3)
- `_agency.shutdown` → graceful stop still works
- `civitas.eval.halt` → halt still works
- `civitas.control.resume` → the resume trigger itself (D4)

Everything else stays queued. Mechanically this means the suspended loop is a restricted
dispatch: pull from the priority path / peek for control types, re-buffer or skip business
messages.

> **Open sub-question (Q-A):** "re-buffer business messages" needs a concrete mechanism. The
> current `Mailbox` is two `asyncio.Queue`s; you cannot peek without dequeuing. Options: (a) a
> dedicated control-message fast-path that bypasses the mailbox entirely (route control
> messages via a separate `asyncio.Event`/attribute the loop checks), or (b) dequeue-and-hold
> business messages in a local buffer while suspended, re-enqueue on resume. (a) is cleaner and
> avoids reordering; I lean (a). Flagged for review.

### D3 — A suspended agent is NOT crashed; the supervisor leaves it alone

> **SUPERSEDED (see Review Findings).** The *goal* stands — a suspended agent must not be treated
> as crashed. But the claim that this needs "zero changes to `Supervisor`" is **false**: verified
> in code, `_message_loop`'s `while == RUNNING` guard and `_stop()`'s status guard both break on
> `SUSPENDED`. Achieving the goal requires changes to `_message_loop`, `_stop()`, and the restart-
> set membership checks. Keep the goal; discard the "zero changes" claim.

Suspension is orthogonal to the crash/restart machinery.

- The agent's asyncio task stays alive (the loop runs, it just isn't dispatching business
  work), so the task-done callback never fires and restart strategies are never triggered.
- Heartbeats are still answered (D2), so remote heartbeat monitoring doesn't flag it.

This is the cleanest possible answer to milestones.md's open question *"does a suspended agent
count as crashed?"* — **no**, and the design achieves that with **zero changes to `Supervisor`
restart strategies**. Suspension lives entirely inside `AgentProcess`; `Supervisor` doesn't
need to know the state exists.

### D4 — suspend/resume are control messages (priority) + Runtime/peer API

`suspend()` and `resume()` are delivered as high-priority control messages so they are
actioned even though the agent isn't dispatching normal traffic:

```
civitas.control.suspend   (priority=1)
civitas.control.resume    (priority=1)
```

Entry points:

```python
# External (Presidium, ops tooling, tests)
await runtime.suspend(name, reason="awaiting approval")
await runtime.resume(name)

# Peer agent (e.g. an orchestrator or a governance agent)
await self.suspend_peer(name, reason=...)
await self.resume_peer(name)
```

Presidium wiring: `PolicyEngine` → `REQUIRE_APPROVAL` calls `runtime.suspend()`; the approval
callback calls `runtime.resume()`. Civitas owns the mechanism; Presidium owns the policy.

`_agency.*` is reserved for pure runtime internals; `civitas.control.*` is a new namespace for
operator/governance control-plane messages. (Naming for review — could also be `_agency.suspend`.)

### D5 — Durability: a runtime-owned suspend marker, separate from `self.state`

On suspend, the runtime writes a **suspension record** to the StateStore under a reserved key,
**not** mixed into user `self.state`:

```python
# Reserved key namespace — never collides with an agent name
store.set(f"_civitas.suspend::{agent_name}", {
    "suspended": True,
    "reason": "awaiting approval",
    "since": 1720000000.0,
})
```

On `_start()`, after `_restore_state()`, the runtime checks for a suspend record. If present,
the agent enters `SUSPENDED` instead of `RUNNING`. `resume()` deletes the record.

Rationale:
- **Separate from `self.state`** because user code owns `self.state` and may clear or overwrite
  it; the suspend marker is control-plane data. Mixing them risks user code accidentally
  un-suspending, or the marker polluting user-visible state.
- **Reserved key on the existing StateStore** (rather than a new protocol method) means zero
  changes to the `StateStore` protocol and works with every existing store — `InMemoryStateStore`,
  and the contrib `SQLiteStateStore` / `PostgresStateStore` — with no contrib changes.

> **Durability caveat (must be documented loudly):** durability is only as durable as the
> configured store. With the default `InMemoryStateStore`, a suspended agent that survives to a
> process restart loses its suspend marker (same semantics as `checkpoint()` today). Durable
> across restart **only** with a persistent store (SQLite/Postgres). This is consistent with how
> `self.state` durability already works, so it's a familiar caveat, not a new surprise.

> **Open sub-question (Q-B):** reserved-key (`_civitas.suspend::{name}`) vs. a first-class
> `StateStore.set_suspended()/get_suspended()` protocol extension. Reserved-key is less invasive
> and needs no contrib changes; a protocol method is more explicit and type-safe but forces
> every store implementation (incl. contrib) to update. Leaning reserved-key. Flagged for review.

### D6 — HITL "approve *this action*" is a checkpoint-and-re-read pattern, not stack magic

Because of the coroutine-snapshot impossibility above, the mid-action approval flow is an
explicit application pattern, not a runtime feature:

```python
async def handle(self, message: Message) -> Message | None:
    if self._needs_approval(message):
        # persist the pending action so it survives suspension (and restart)
        self.state["pending_action"] = message.payload
        await self.checkpoint()
        await self.emit_eval("action.requires_approval", {...})  # tell Presidium
        return  # stop here; an external controller will suspend us

    # on resume, a fresh message (or a re-delivery) triggers execution:
    if self.state.get("pending_action"):
        action = self.state.pop("pending_action")
        await self.checkpoint()
        return await self._execute(action)
```

The runtime provides the *pause* (SUSPENDED) and the *durable marker* (D5); the agent provides
the *what-was-I-doing* via its own checkpointed `self.state`. The design doc and user docs must
show this pattern so nobody expects transparent mid-`handle()` resumption.

---

## Open Questions (genuinely unresolved — need a design session)

- **Q1 — Self-suspension deadlock.** Can an agent call `await self.suspend()` on *itself* from
  inside `handle()`? Problem: the message loop is what processes the `resume` control message.
  If `handle()` blocks awaiting resume, the loop can't process resume → **deadlock.** Options:
  (a) forbid self-suspension — `suspend()` is external/peer-only, self-pause is done via the D6
  checkpoint-and-return pattern; (b) allow `self.suspend()` but make it non-blocking — it sets
  the state and the loop suspends *after* `handle()` returns, not mid-call. I lean (a) for v0.5
  (simplest, no deadlock surface), with the D6 pattern as the sanctioned self-pause idiom.
  **Needs your call.**

- **Q2 — `ask()` into a suspended agent.** A caller doing `ask(suspended_agent, ...)` blocks
  until its timeout (default 30s). For long approvals this will time out. Is that acceptable
  (document "don't `ask()` an agent you expect to be suspended; use `send()` + poll/event"), or
  do we want the bus to fail fast with a distinct `AgentSuspendedError`? Failing fast is friendlier
  but requires the bus/registry to know suspension state (currently it doesn't — D3 keeps
  suspension inside `AgentProcess`). Leaning: document the timeout behavior for v0.5, defer
  fail-fast. **Needs your call.**

- **Q3 — Control-message delivery mechanism (Q-A above).** Separate control fast-path vs.
  dequeue-and-hold. Affects whether message ordering is preserved across a suspend/resume cycle.

- **Q4 — Persistence surface (Q-B above).** Reserved StateStore key vs. protocol extension.

- **Q5 — Interaction with dynamic children.** If a `DynamicSupervisor`'s dynamic child is
  suspended and the supervisor is asked to `stop(drain=...)` it, does drain wait for resume, skip,
  or force-stop? Probably force-stop (suspension shouldn't block shutdown), but the interaction
  with the D5 marker (does a force-stopped suspended child leave a stale marker?) needs care —
  `despawn()`/`stop()` must clear the suspend marker.

- **Q6 — Observability.** New spans/events: `civitas.agent.suspend` / `civitas.agent.resume`,
  and an `AuditEvent` at each (this is squarely an audit-relevant, governance action — Presidium
  will want it). Straightforward, just needs to be in scope.

---

## API Surface (provisional — subject to open questions)

```python
# ProcessStatus
class ProcessStatus(Enum):
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"   # re-introduced, fully wired (was removed in F02-6)
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    CRASHED = "CRASHED"

# Runtime — external entry points
await runtime.suspend(name, reason="awaiting approval")
await runtime.resume(name)

# AgentProcess — peer control
await self.suspend_peer(name, reason=...)
await self.resume_peer(name)

# AgentProcess — lifecycle hooks (optional overrides)
async def on_suspend(self, reason: str) -> None: ...   # called as it enters SUSPENDED
async def on_resume(self) -> None: ...                 # called as it leaves SUSPENDED

# Introspection
runtime.get_agent(name).status is ProcessStatus.SUSPENDED
```

---

## Non-Goals (v0.5.0)

- **Mid-`handle()` coroutine snapshotting / stack resumption** — impossible in Python; not
  attempted. Durable suspension pauses *dispatch*, per the clarification section.
- **Suspension state in the routing registry / bus** — kept out of the registry/bus; fail-fast
  `ask()` into a suspended agent is deferred (caller sees a timeout for v0.5). NOTE: this does
  *not* mean "no `Supervisor` changes" — `_stop()` and the restart paths must handle `SUSPENDED`
  (see Review Findings; the earlier "no Supervisor changes" wording was wrong).
- **Cross-process suspend of a remote worker agent** — same trajectory as dynamic spawning:
  control message works over the bus in principle, but v0.5 targets in-process; remote is a
  follow-on. (Confirm scope during review.)
- **Automatic re-delivery of the in-flight message on resume** — the agent that returned from
  `handle()` before suspending is responsible for its own pending-action checkpoint (D6).

---

## Implementation checklist (to be filled in AFTER this design is approved)

Not started — this document is the first deliverable of Bucket B and is for review before any
code is written, per this repo's convention (design doc precedes implementation for changes to
core lifecycle semantics; see M4.2 and dynamic-spawning).

---

## Addendum (2026-07-21) — suspension × remote heartbeats false-crash interaction

Verified finding (supervision review, [`supervision-hardening.md`](supervision-hardening.md) A6,
issue [#31](https://github.com/civitas-io/python-civitas/issues/31)): `_agency.heartbeat` is sent
at **priority 0**, but a SUSPENDED agent drains only the priority queue (S3 in this doc). A
suspended *remote* agent therefore buffers heartbeats, misses the ack threshold, and is declared
crashed and restarted by its supervisor — a deliberately-paused agent gets forcibly recycled
(its suspend marker survives the restart, but the restart consumes budget and fires spurious
crash signals). Fix direction: heartbeats ride the priority channel (stopgap) and liveness moves
out of the per-agent mailbox (structural), per supervision-hardening.md D5. Until then,
cross-process suspend (already a non-goal for v0.5 above) must remain off.

---

## Addendum (v0.9.4) — `SuspendCategory`: an additive, backward-compatible categorization

The original design's `reason: str` was always free text with zero structure — by design, since
this whole mechanism exists for **Presidium**, a separate external product, to consume for its
HITL approval flow (see `docs/design/civitas-presidium-boundary.md`); civitas was never meant to
interpret `reason`'s contents itself. `docs/design/dashboard-v2.md` §6 wanted to render a HITL
approval wait differently from an operational governance pause, but had nothing to key off — both
looked like an identical `SUSPENDED` with an opaque string.

**Resolution**: `suspend()` gains an additive `category: SuspendCategory = SuspendCategory.OTHER`
parameter (`HITL_APPROVAL` / `GOVERNANCE_PAUSE` / `OTHER`), alongside the unchanged free-form
`reason`. This does NOT violate S1-S10 above:

- **S2 (non-blocking boundary transition)** — unchanged; `category` is carried the same way
  `reason` already is (`self._suspend_category`, actioned at the same boundary).
- **S4/S5 (durable marker, write-ahead ordering)** — `category` is written into the SAME marker
  dict `_enter_suspended()` already persists (`reason`/`since`/`approver` + now `category`), in the
  same write-ahead-ordered checkpoint — no new persistence path, no new failure mode.
- **S6 (approver-gated resume)** — unchanged; `resume(approver)` doesn't need to know which
  category triggered the suspend.
- **S10 (idempotent re-suspend)** — `_update_suspend_reason()` now also accepts an optional
  `category` and updates it in place, same idempotent shape as the existing `reason` update.
- **Restore path** — read via a new `suspend_category` property that reads the persisted marker
  DIRECTLY, not a separate in-memory attribute — deliberately more restore-safe than the
  pre-existing (harmless-today) gap where `self._suspend_reason` itself is never re-populated from
  the marker after `_restore_state()`.

**Backward compatibility** (a hard constraint — `suspend()`/`resume()` are a documented
Civitas-Presidium contract): every new parameter defaults such that an existing caller passing
only `reason=` is completely unaffected, landing in `SuspendCategory.OTHER` — today's only
category, in effect. The `_agency.suspend` wire payload's `category` field is optional; a sender
that never sends it (an older Presidium, or any other caller) keeps working exactly as before.

**Convenience wrapper**: `suspend_for_approval(reason: str = "")` — "a subset of suspend/resume"
— is `suspend(reason=reason, category=SuspendCategory.HITL_APPROVAL)`. `resume(approver)` is
unchanged; no separate `approve()` alias was added.

See `docs/design/dashboard-v2.md` §18 for the dashboard-rendering side of this (the actual
motivating use case) and its own real end-to-end verification against a running agent.
