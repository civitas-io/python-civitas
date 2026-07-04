# Non-Blocking Dynamic Spawn (v0.7.0 · R1)

**Status:** ✅ Approved — Oracle + Momus reviewed; maintainer signed off 2026-07-04. Implementation in progress.
**Source:** [GH #8](https://github.com/civitas-io/python-civitas/issues/8) (parent), [GH #9](https://github.com/civitas-io/python-civitas/issues/9) (this)
**Related:** [`dynamic-spawning.md`](dynamic-spawning.md) (M4.1b), [`durable-suspension.md`](durable-suspension.md) (F11-5, suspend/resume)
**Roadmap:** [`milestones.md`](../milestones.md) v0.7.0 R1 — headline, *design-first*

> **v2 changelog (Momus REJECT of v1 → addressed):** B1 sync-callback can't `await` → §7 now mirrors the
> existing sync-callback→`create_task` bridge; B2 `wait=True` failure now cleans up **inline before reply**;
> B3 `_restore_state()` stays **outside** the try (no static-agent change, F11-5 preserved); B4 the
> `task.cancelled()` guard is preserved and the cancel rationale restated; B5 `bus.teardown_agent` specified
> as **net-new**; N1 `_reached_loop`, N2 `_start_phase="running"`, N3 `max_total_spawns` change flagged,
> N4 reconciled with existing `_notify_spawner`; nits folded in. See §14.

---

## 1. Problem

`DynamicSupervisor.spawn(AgentClass, name, config)` lets a running agent create a child at runtime.
Today the call **blocks the spawner** until the child's `on_start()` returns. When `on_start()` does real
work (DB connect, model warmup), the spawner's `handle()` is stuck the whole time — it cannot process any
other message. [GH #9](https://github.com/civitas-io/python-civitas/issues/9): *reply as soon as the
child's message loop exists; run `on_start()` inside the child's task.*

## 2. Current behavior (ground truth, with line refs)

`DynamicSupervisor._handle_spawn` (`civitas/supervisor.py:563-633`):
1. dup-name / `max_children` / `max_total_spawns` checks → early error reply
2. import class; `on_spawn_requested` governance veto
3. instantiate + wire deps (`_bus,_tracer,_registry,_dynamic_supervisor_name,llm,tools,store,_audit_sink,_metrics,config`)
4. `self._registry.register(name)` — registry entry created here
5. `await self._bus.setup_agent(agent)` — transport subscription; **mailbox now accepts messages**
6. `await agent._start()` — **blocks**
7. `self._total_spawns += 1` **(after success — line 619)**; store in `_dynamic_children`; attach done-callback
8. `return self.reply({"status":"ok","name":name})`

`AgentProcess._start()` (`process.py:907-945`):
```python
self._status = INITIALIZING
self._running_event = asyncio.Event()
await self._restore_state()                    # OUTSIDE the try — restore failure => NO on_stop
try:
    await self.on_start()                       # runs in the CALLER's context (supervisor awaits step 6)
except Exception:
    self._status = CRASHED
    await self.on_stop()                        # F11-5 (v0.5.0): on_stop DOES run on on_start failure
    <disconnect MCP>; raise
self._task = asyncio.create_task(self._message_loop())   # task created only AFTER on_start succeeds
await self._running_event.wait()                # blocks until loop sets RUNNING
```

Existing crash/cleanup plumbing (this is what R1 modifies — do **not** reinvent it):
- `_on_child_done(name, task)` (`supervisor.py:702`) — **sync** done-callback: `if task.cancelled(): return`
  (line 703), reads `task.exception()`, then bridges to async: `create_task(self._handle_child_exit(...))`
  tracked in `_pending_child_tasks`.
- `_handle_child_exit(name, exc)` (`supervisor.py:711`) — async: early-returns `if name not in
  _dynamic_children` (idempotent), applies NEVER/TRANSIENT/permanent restart logic, calls `_notify_spawner`
  on terminal outcomes. **Restarts on any crash today** (incl. what would become `on_start` failures).
- `_remove_child(name)` (`supervisor.py:782`) — pops `_dynamic_children` + `_child_tasks`. **Does not
  deregister from the registry** (registry deregister currently only happens in `_handle_despawn`/`_handle_stop`).
- `_notify_spawner(name, reason)` (`supervisor.py:795`) — sends `civitas.dynamic.terminated` → triggers the
  spawner's `on_child_terminated(name, reason)` hook (`process.py:365`, invoked at `1009`).
- `bus.setup_agent` (`bus.py:47`) exists; **`bus.teardown_agent` does NOT exist** (net-new — see D9/B5).

Consequences today: a caller who receives `ok` is guaranteed the child is `RUNNING` and `ask(child)` works
immediately; `on_start()` failure propagates as an exception out of `_handle_spawn` (no structured error
reply — the caller likely **times out**), and because the done-callback is only attached *after* success,
the failed child's **registry entry + bus subscription leak**. `_start()` is shared with static agents
(`Supervisor._start_child` → `await agent._start()`).

## 3. Goals / Non-goals

**Goals:** spawner can create a child without blocking its `handle()` on the child's `on_start()`; **zero
default behavior change** (existing `spawn()` keeps today's semantics incl. sync error reporting); preserve
FIFO ordering + buffer-then-dispatch; one unified cleanup path; **fix** the on-failure registry/bus leak.

**Non-goals:** changing static-agent startup; cross-process spawn (R6) — but cleanup is designed so R6 can
reuse it; concurrent spawn dispatch within one supervisor (see §9 dup-name invariant).

## 4. Core design

> **One lifecycle, two reply timings.** `on_start()` always runs **inside** the child's task (before the
> dispatch loop). `_restore_state()` runs inside the task too but **before** the `on_start` guard, matching
> today (restore failure => no `on_stop`). The only thing that differs between blocking and non-blocking is
> **whether the supervisor awaits readiness before replying** — exposed as `spawn(..., wait: bool = True)`.

Safe because the mailbox already buffers (step 5) and the loop gates dispatch on `RUNNING`. Diagram:
`docs/assets/non-blocking-spawn-sequence.svg`.

## 5. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| **D1** | Split `_start()` → `_start_nowait()` (creates task, returns) + in-task `_run()`. `on_start()` runs in the task both modes; `_restore_state()` runs in the task **before** the `on_start` guard. | Single path; F11-5 preserved (restore outside guard). |
| **D2** | `spawn(..., wait: bool = True)`; add `spawn_nowait()` = `spawn(wait=False)` (`Runtime` + `AgentProcess`). | Reject flip-default (breaks error contract); reject spawn-handle (spawn is request/reply). `wait=True` = 100% compatible. |
| **D3** | `wait=True` reply = today's meaning (RUNNING/SUSPENDED reached, or synchronous `error`). `wait=False` `ok` = *"named, subscribed, mailbox live; dispatched FIFO once ready; failure arrives async."* Reply carries `ready: bool` + `state`. **`ready=true` with `state="SUSPENDED"` = started but not dispatching business messages** (sends buffer until resume). | Callers must know which guarantee they hold. |
| **D4** | `_running_event` keeps its meaning — set **only** on RUNNING/SUSPENDED, never on failure. `wait=True` awaits `FIRST_COMPLETED(_running_event, task)`, then inspects `status`; cancel the losing future. | On success the task never completes; must race event vs task-completion. |
| **D5** | Post-`ok` failures surface via the **existing** `_notify_spawner` → `on_child_terminated(name, reason)`, `reason` = `"<phase>: <repr>"` (`phase ∈ {restore, on_start, running}`). No new public hook. | Reuses shipped plumbing; `reason` stays a string. |
| **D6** | Fire `on_child_terminated` **only if `acknowledged`** (an `ok` was sent — i.e. `wait=False`, or `wait=True` after RUNNING). `wait=True` start-failure replies `error` and does **not** notify. | The `wait=True` error reply *is* the notification. |
| **D7** | Single cleanup helper `_terminal_cleanup(name)` = deregister + `bus.teardown_agent` + `_remove_child`, **idempotent**. Called inline on the `wait=True` failure branch **and** from `_handle_child_exit`; the existing `if name not in _dynamic_children: return` makes the second call a no-op. | Start-failure, cancel, crash converge; `wait=True` post-state deterministic (B2). |
| **D8** | **Initial-start failure is terminal — no restart.** Restart eligibility keys off `_reached_loop` (child entered the dispatch loop, RUNNING **or** SUSPENDED). Only `_reached_loop` children are restarted. | Non-blocking hides crash-loops from the initiator; also resolves the `wait=True`-error-vs-restart contradiction. Divergence from OTP — §11 Q2. |
| **D9** ⚠️ | **`bus.teardown_agent(name)` — NET-NEW.** (a) unsubscribe transport; (b) drain the child's mailbox: buffered messages with `reply_to`/`correlation_id` get an **error reply**, others dropped+logged; (c) fail any pending outbound-ask futures whose recipient is `name`. Idempotent. | Without it, `spawn_nowait()`→`ask(child)` **hangs forever** on a bad `on_start()`. |
| **D10** | `max_total_spawns` = lifetime **attempt** guard → increment at **admission** (not after success as today, `supervisor.py:619`), **no refund**. `max_children` = `len(_dynamic_children)`, self-corrects on cleanup. | Storm protection must not be refundable. **Behavior change from today — §11 Q4.** |
| **D11** | Route cleanup+notify through the existing `_on_child_done` (sync, keeps `task.cancelled()` guard) → `_handle_child_exit` (async). `_handle_child_exit` gains the D8 `_reached_loop` gate. No new event system. | Reuses `supervisor.py:702-803`; keeps R6 able to add a remote `terminated` handler later. |
| **D12** ⚠️ | On `on_start()` failure, make `on_stop()` **guarded best-effort** (try/except + log) instead of unconditional. **Observationally identical to F11-5 unless `on_stop` itself raises.** | Honors F11-5's cleanup intent while stopping a throwing `on_stop` from masking the `on_start` error. **Needs sign-off — §11 Q1.** |

## 6. API specification

```python
async def spawn(self, agent_class, name=None, config=None, *, wait: bool = True) -> str: ...
async def spawn_nowait(self, agent_class, name=None, config=None) -> str: ...   # = spawn(wait=False)
```
- `wait=True` (default): returns after RUNNING/SUSPENDED; raises `SpawnError` on start failure (today's behavior).
- `wait=False`: returns as soon as the task exists; start failure delivered later via `on_child_terminated`.

Spawn reply payload (bus-level):
```json
{"status":"ok","name":"w1","ready":true,"state":"RUNNING"}
{"status":"ok","name":"w1","ready":false,"state":"INITIALIZING"}
{"status":"error","name":"w1","phase":"on_start","error":"..."}
```
(`ready=false` = wait=False; `error` = wait=True failure, sent after inline cleanup.)

## 7. Pseudocode (reconciled with existing code)

```python
# process.py
def _start_nowait(self) -> asyncio.Task:
    self._status = INITIALIZING
    self._running_event = asyncio.Event()          # set ONLY at RUNNING/SUSPENDED
    self._reached_loop = False                      # N1: True once the dispatch loop is entered
    self._start_phase = "restore"                   # N2: advances to on_start -> running
    self._task = asyncio.create_task(self._run(), name=self.name)
    return self._task

async def _run(self):
    await self._restore_state()                     # B3: OUTSIDE the guard -> restore failure => NO on_stop (== today)
    self._start_phase = "on_start"
    try:
        await self.on_start()
    except asyncio.CancelledError:
        raise                                       # B4: intentional cancel -> _on_child_done's cancelled-guard handles it
    except Exception:
        self._status = CRASHED
        try:
            await self.on_stop()                    # D12: guarded best-effort
        except Exception:
            logger.exception("[%s] on_stop() after failed on_start()", self.name)
        await self._disconnect_mcp(best_effort=True)
        raise                                       # task.exception() populated
    await self._message_loop()

# in _message_loop(), where it sets RUNNING/SUSPENDED (process.py:955-960):
self._reached_loop = True                            # N1
self._start_phase = "running"                        # N2
self._running_event.set()                            # unchanged
```

```python
# supervisor._handle_spawn — steps 1-5 unchanged; then ONE synchronous block (no await between):
self._total_spawns += 1                              # D10: at admission, no refund
task = agent._start_nowait()
self._dynamic_children[name] = _ChildRec(agent=agent, task=task, acknowledged=False)
self._spawner_names[name] = spawner
self._child_tasks[name] = task
task.add_done_callback(lambda t: self._on_child_done(name, t))   # existing sync bridge (supervisor.py:702)

if not wait:                                         # wait=False — reply() is synchronous; no await before it
    self._dynamic_children[name].acknowledged = True
    return self.reply({"status":"ok","name":name,"ready":False,"state":agent._status.value})

# wait=True — await readiness OR task-completion (D4)
ev = asyncio.ensure_future(agent._running_event.wait())
try:
    await asyncio.wait({ev, task}, return_when=asyncio.FIRST_COMPLETED)
finally:
    ev.cancel()                                     # nit: don't leak the loser future
if agent._status in (RUNNING, SUSPENDED):
    self._dynamic_children[name].acknowledged = True
    return self.reply({"status":"ok","name":name,"ready":True,"state":agent._status.value})
# start failed -> INLINE cleanup BEFORE replying (B2); idempotent with _handle_child_exit
await self._terminal_cleanup(name)
err = None if task.cancelled() else task.exception()   # B4: never call exception() on a cancelled task
return self.reply({"status":"error","name":name,"phase":agent._start_phase,"error":repr(err)})
```

```python
# supervisor — new helper + D8 gate in the EXISTING _handle_child_exit
async def _terminal_cleanup(self, name):            # D7: idempotent
    if self._registry is not None:
        self._registry.deregister(name)             # closes the on-failure registry leak (see §2)
    if self._bus is not None:
        await self._bus.teardown_agent(name)        # D9
    self._remove_child(name)                         # pops _dynamic_children + _child_tasks (supervisor.py:782)

async def _handle_child_exit(self, name, exc):      # supervisor.py:711, augmented
    rec = self._dynamic_children.get(name)
    if rec is None:
        return                                       # already cleaned (inline wait=True path, or stop/despawn)
    crashed = exc is not None
    if crashed and not rec.agent._reached_loop:      # D8: init failure is terminal — never restart
        await self._terminal_cleanup(name)
        if rec.acknowledged:                         # D6: only notify if an ok was sent (wait=False)
            await self._notify_spawner(name, f"{rec.agent._start_phase}: {exc!r}")
        return
    ... existing NEVER / TRANSIENT / permanent restart logic (supervisor.py:717-780) ...
```

`_ChildRec` = mutable dataclass `{agent, task, acknowledged: bool}` (`acknowledged` mutated in place).

## 8. Ordering & readiness guarantees

- **FIFO per sender:** any message a sender emits *after* receiving `ok` is dispatched after the child
  reaches RUNNING, in send order. No cross-sender global order.
- **No new readiness state:** `ProcessStatus` + `_running_event` encode readiness; `_reached_loop` records
  *whether* the loop was ever entered (RUNNING **or** SUSPENDED) — used only for restart eligibility (D8),
  consistent with today's "a child crashed while SUSPENDED restarts" behavior (`supervisor.py:765-768`).
- **Registry-before-ready window** (entry at step 4, RUNNING later): *already exists today*; `wait=False`
  widens it. Correctness holds (third-party `ask(child)` buffers, served after RUNNING; or failed via D9 on
  start failure). Residual cost is **routing quality** (`send_capable` picking a cold child). **Optional
  mitigation (§11 Q3):** a `ready` flag on registry entries so *capability/discovery* routing skips
  not-yet-ready agents while explicit `ask(name)` still buffers.

## 9. Failure modes & mitigations

1. **Pending `ask` hangs on start-failure** — `bus.teardown_agent` fails pending asks + drains the mailbox (D9). *The* load-bearing guarantee for `wait=False`.
2. **`on_stop()` on a half-initialized agent** — guarded best-effort (D12); restore-phase failure never calls `on_stop` (B3).
3. **Cancellation** — restart-avoidance comes from the **`task.cancelled()` guard in `_on_child_done`** (`supervisor.py:703`), which D11 preserves — *not* from a status value. A cancel during the running phase is not wrapped by `_run`'s `except CancelledError`, so the guard is the sole mechanism; `_on_child_done` must never call `task.exception()` before checking `task.cancelled()`.
4. **`wait=True` inherits the spawn-ask timeout** — a slow `on_start` can exceed it; the caller sees a timeout while the child later goes RUNNING (still tracked). This *is* the motivation for `wait=False`; document it.
5. **Dup-name race** — safe **only because the supervisor dispatches one spawn at a time** (single message loop). Documented invariant; breaks if concurrent dispatch is ever added.
6. **Bookkeeping/callback ordering** — `create_task`, `_dynamic_children[name]=…`, `_child_tasks`, `add_done_callback` are one synchronous block with **no `await`** between; `self.reply()` is synchronous, so `wait=False` sets `acknowledged=True` and constructs `ok` before the child task can run — no lost-notification race.
7. **Metrics** — emit `spawn_accepted` (at `ok`) and `spawn_started`/`spawn_failed` (at RUNNING/failure) separately, or dashboards overcount.
8. **Terminated-before-ok race** — a near-instant `wait=False` failure can enqueue `terminated` before the caller processes `ok` (different channels); correlate by `name`.

## 10. Backward compatibility

- `wait=True` is the default and reproduces today's **observable** semantics, and is **strictly better** on
  failure: today `on_start` failure propagates out of `_handle_spawn` with no structured reply (caller times
  out) and leaks the child's registry entry + bus subscription (§2); v2 returns `{"status":"error",...}` and
  runs `_terminal_cleanup` first. Define the target failure post-state as *"deregistered, unsubscribed, not
  in `_dynamic_children`"* rather than "parity with today."
- **`_restore_state()` stays outside the `on_start` guard (B3)** => restore-failure behavior is unchanged
  (no `on_stop`), so **static agents are unaffected** (`Runtime.start()` path).
- `on_child_terminated(name, reason)` signature unchanged; `reason` enriched (still a string).
- **Two intentional changes needing sign-off:** D12 (guarded `on_stop`, observable only if `on_stop`
  raises) and D10 (`max_total_spawns` counted at admission, no refund — today failures are effectively free).

## 11. Resolved decisions (maintainer sign-off — 2026-07-04)

1. **D12 / F11-5:** ✅ **Guarded best-effort `on_stop`** on `on_start` failure — observationally identical to F11-5 unless `on_stop` itself raises.
2. **D8:** ✅ **Terminal** — initial-start failure does **not** restart; only `_reached_loop` children are restart-eligible.
3. **§8 registry `ready` flag:** ⏸️ **Deferred** (YAGNI) — buffering keeps routing correct; revisit if cold-routing to not-yet-ready children becomes a real problem.
4. **D10:** ✅ **Admission-count, no refund** for `max_total_spawns`.
5. **API:** ✅ Ship **both** `spawn(wait=…)` and the `spawn_nowait()` alias.

## 12. Escalation triggers (future, out of scope)

- Callers branch heavily on failure `phase` → add `on_child_start_failed` (defaulting to `on_child_terminated`).
- Cold-routing pain → promote the registry `ready` flag from optional to required.
- Real need for restart-on-init-failure → bounded `temporary`-style policy rather than flipping D8.
- `wait_ready(name, timeout)` helper to formalize post-`spawn_nowait` readiness (ping-ask).

## 13. Test plan

**Reply timing / compat**
- `wait=True` success → `ok`/`ready=true`; `ask` works immediately (parity with today).
- `wait=True` `on_start` raises → synchronous `error` reply with `phase="on_start"`; **no** `on_child_terminated`; child **deregistered + unsubscribed + absent from `_dynamic_children` before the reply is observed** (B2/D7).
- `wait=False` success → immediate `ok`/`ready=false`; buffered `ask` resolves after RUNNING, in order (FIFO).
- `spawn_nowait()` alias behaves as `spawn(wait=False)` (D2).

**Failure semantics**
- `wait=False` `on_start` raises → a pending `ask` sent before failure is **failed, not hung** (D9); a buffered fire-and-forget `send` is dropped+logged (D9); `on_child_terminated` fires with `reason` starting `on_start:` (D5/D6).
- **restore-phase failure** → task fails with `phase="restore"`, **`on_stop` NOT called** (B3); `wait=True` → error reply, `wait=False` → terminated `restore:`.
- **cancel during `on_start`** and **cancel during running** → `_on_child_done` cancelled-guard prevents restart and any `task.exception()` call (B4); no `on_child_terminated`.
- **D8 restart boundary:** `on_start` failure → **no restart**; a crash **after** reaching RUNNING → **restarts** per policy (the exact `_reached_loop` boundary); a child that came up SUSPENDED then crashed → **restarts** (SUSPENDED counts).
- **D12:** a *throwing* `on_stop` after `on_start` failure is swallowed and the original `on_start` error is what gets reported.
- **D7 idempotency:** inline `_terminal_cleanup` + the later `_handle_child_exit` do not double-deregister / error.

**Accounting / static**
- `max_total_spawns` **not** refunded on failure (D10); `max_children` frees after cleanup.
- restore-marker child comes up `SUSPENDED`; `wait=True` reply carries `state="SUSPENDED"`, `ready=true`.
- static-agent suite unchanged (full existing suite green); dup-name rejected within one supervisor dispatch.
- `spawn_accepted` vs `spawn_started`/`spawn_failed` metric points emitted (§9.7).

## 14. Implementer checklist (net-new / changed symbols)

- `AgentProcess`: `_start_nowait()`, `_run()`, `_reached_loop`, `_start_phase`; refactor `_start()` (static path) to `_start_nowait()` + `await FIRST_COMPLETED(_running_event, task)`; guarded `on_stop` (D12).
- `DynamicSupervisor`: `wait` param + `spawn_nowait`; `_ChildRec`; `_terminal_cleanup`; `_reached_loop` gate in `_handle_child_exit`; move `_total_spawns += 1` to admission; registry deregister on terminal paths.
- `MessageBus`: **`teardown_agent(name)`** (D9) + a transport `unsubscribe` if absent.
- `Runtime`: `spawn(wait=…)` / `spawn_nowait` pass-through.
