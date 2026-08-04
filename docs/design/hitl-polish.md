# HITL polish — indefinite/fail-fast `ask()` + restart-budget exemption (v0.10.0)

**Status: design confirmed (2026-07-31 review), not yet implemented.** Three orthogonal HITL
rough-edges from the v0.10.0 backlog, built on the durable-suspension primitive
([`durable-suspension.md`](durable-suspension.md)) and the v0.9.4/9.6 HITL work (`SuspendCategory`,
`suspend_for_approval()`, the `POST /agents/{name}/resume` control-plane action). Design-first per
convention (touches the bus, registry, transports, and supervisor).

## 1. Motivating fact: HITL approvals take hours or days

A human-in-the-loop approval is not a sub-second event — depending on the app it can take hours or
days (an operator approving a spend, a reviewer signing off a deploy). The durable-suspension
primitive already reflects this: `suspend()` has **never** had a timeout — an agent stays
`SUSPENDED` indefinitely until `resume()`. What's missing is the *caller* side: today a caller who
`ask()`s a suspended agent is forced into a bounded wait (default 30s), which is useless for a
multi-day approval. This release fixes the caller ergonomics without changing the (already
indefinite) suspension semantics.

## 2. Current behavior (verified, not assumed)

- `ask(recipient, payload, message_type="message", timeout=30.0)` \u2014 `timeout` is already a
  per-call caller-chosen value (opt-in), default 30s.
- An `ask()` to a `SUSPENDED` agent sends a **priority-0 business message**, which **buffers in the
  normal queue** (a suspended agent drains only the *priority* queue, S3). On **resume** the agent
  drains the normal queue, processes it, and **sends the reply** \u2014 which the still-waiting `ask()`
  receives *if within its timeout*. So today's behavior is a **usable "block until approved, then
  get the result" pattern**, just capped at the timeout.
- All three transports (`inprocess`, `zmq`, `nats`) implement `request()` with the identical
  `async with asyncio.timeout(timeout): reply_data = await reply_queue.get()`. **`asyncio.timeout(None)`
  waits forever** \u2014 so indefinite waits are natively supported by passing `None` down, with zero
  per-transport special-casing.
- Suspension state lives **inside `AgentProcess`** (design D3). The bus and registry do **not** know
  an agent is suspended today (confirmed: no `AgentSuspendedError` in `errors.py`, no suspension
  field in `registry.py`/`bus.py`).

## 3. Decisions

### D1 \u2014 Indefinite `ask()`: `timeout: float | None`, never mandatory

`ask()` (and `bus.request()`, and each `transport.request()`) widen `timeout: float` to
`timeout: float | None`.

- **`timeout > 0`** \u2014 bounded wait, exactly as today. Default stays **`30.0`** (backward
  compatible; no existing caller changes).
- **`timeout is None`, or `timeout <= 0` (canonically `-1`)** \u2014 **wait indefinitely** until the
  agent resumes and replies. Both spellings accepted: `None` is Pythonic/asyncio-native; `-1` is
  the explicit "wait until the human reacts" value. Normalized to `None` at the `ask()`/
  `bus.request()` boundary so every transport's `asyncio.timeout()` receives `None`.

The timeout is **never mandatory** \u2014 it is a value the caller picks, and one of its values is
"no limit."

**Documented caveat (docstring, not a code guard):** an indefinite `ask()` holds resources for the
whole wait \u2014 the pending reply queue, and on ZMQ/NATS an open ephemeral subscription \u2014 for hours
or days. That is fine and expected for a *background worker* driving a HITL flow (it is the point).
It is a **foot-gun for a request-scoped caller** (e.g. an HTTP handler must not block a connection
for days) \u2014 those should use `send()` + poll/webhook instead. Documented so nobody wires a web
handler into a multi-day hang.

### D2 \u2014 Opt-in fail-fast: `fail_if_suspended: bool = False` + `AgentSuspendedError`

The opposite intent from D1: a caller who does **not** want to wait at all, and wants an immediate,
*distinct* error (not a generic `TimeoutError` after the timeout elapses) when the target is
suspended.

- `ask(..., fail_if_suspended=False)` \u2014 **default, unchanged**: buffer + deliver-on-resume (D1
  semantics). Zero behavior change, zero cost on the hot path.
- `ask(..., fail_if_suspended=True)` \u2014 `bus.request()` checks the recipient's suspension state and
  raises a new **`AgentSuspendedError`** immediately, before the message is buffered.

**Not mandatory** (the reason we rejected mandatory fail-fast): making it mandatory would *delete*
the D1 "wait for approval then get the result" pattern \u2014 a real capability regression. Opt-in is
strictly additive.

**How the bus learns suspension state (the one structural change):** the registry entry gains a
`suspended: bool` flag, updated on each suspend/resume transition. Consulted **only** when
`fail_if_suspended=True`, so the default path never touches it. This is design option (a) from the
scoping discussion \u2014 chosen over an agent-side fast-reject path because a suspended agent
structurally cannot reply to a buffered business message without breaking the "priority queue only
while suspended" invariant (S3).

- The transition point: `AgentProcess`'s message-loop handling of `_agency.suspend`/`_agency.resume`
  (and the direct `suspend()`/`resume()` methods) updates the registry flag via the already-present
  `self._registry`. Restore-into-`SUSPENDED` (S7, a checkpoint marker bringing an agent up
  suspended) must also set it.
- Scope: same-process registry for v1. Cross-process suspension visibility (a remote agent's
  suspended flag propagating to another process's registry) is deferred \u2014 consistent with
  cross-process suspend already being a documented non-goal (durable-suspension.md); a
  `fail_if_suspended` ask to a *remote* suspended agent falls back to the timeout behavior (still
  correct, just not fast).

### D3 \u2014 Restart-budget exemption for crash-while-`SUSPENDED`

A suspended agent is paused, not working. If it crashes while `SUSPENDED` (e.g. an external kill, or
the known heartbeat interaction where a suspended remote agent's buffered heartbeats miss the ack
threshold), the supervisor restarts it and the durable marker correctly restores it to `SUSPENDED`
\u2014 but that restart currently **counts against the restart-intensity budget** (`crashes_in_window`).
A "paused and something poked it" restart should not burn the same budget as a genuine crash-loop.

- A crash whose incarnation was `SUSPENDED` at crash time is **exempt** from `crashes_in_window`
  (the restart still happens; it just does not count toward the max-restarts escalation window).
- **Care:** the exemption must not mask a genuine crash-loop *in the suspend path itself* (e.g. an
  `on_suspend()` that always raises). Mitigation: the exemption applies to a crash of an
  *already-established* `SUSPENDED` incarnation, not to a failure *during* the suspend transition.
  Exact predicate to be pinned during implementation against `supervisor.py`'s crash-event handling
  (S8 finding #5).

## 4. Not in scope

- Cross-process suspension visibility in a remote registry (D2 is same-process; remote falls back
  to timeout). Deferred, consistent with cross-process suspend being a non-goal.
- Changing suspension semantics themselves (already indefinite; untouched).
- The two HITL *models* unification / a new end-to-end example \u2014 optional follow-on, not part of
  this polish release.

## 5. Build order (each slice independently verified)

1. **D1 indefinite `ask()`** \u2014 widen `timeout` to `float | None` through `ask()` \u2192 `bus.request()`
   \u2192 all 3 `transport.request()`; normalize `-1`/negative \u2192 `None`; verify a real indefinite ask
   returns the reply after a real resume (in-process; ZMQ round-trip for cross-transport proof).
2. **D2 opt-in fail-fast** \u2014 `AgentSuspendedError`; registry `suspended` flag + transitions;
   `fail_if_suspended` param; verify instant error vs. default buffering, and restore-into-suspended
   sets the flag.
3. **D3 restart-budget exemption** \u2014 `supervisor.py` crash-event handling; verify a crash of a
   `SUSPENDED` agent restarts it without consuming budget, while a suspend-path crash-loop still
   escalates.

## 6. Open detail to settle during implementation

- **D3's exact exemption predicate** \u2014 "crash of an established SUSPENDED incarnation" vs "any crash
  whose marker says suspended," and how to tell a suspend-transition failure from a paused-then-poked
  crash. Pinned against the real `supervisor.py` crash-event code in slice 3, not prejudged here.
