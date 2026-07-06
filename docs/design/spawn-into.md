# Cross-Tree Spawn — `spawn_into()` (v0.7.0 · R2)

**Status:** ✅ Approved — Oracle-reviewed; maintainer signed off 2026-07-04. Implementation in progress (Momus reviews the `.sisyphus/plans/*.md` implementation plan, its actual mandate).
**Source:** [GH #10](https://github.com/civitas-io/python-civitas/issues/10)
**Related:** [`dynamic-spawning.md`](dynamic-spawning.md) (M4.1b), [`non-blocking-spawn.md`](non-blocking-spawn.md) (R1), [`security-hardening.md`](security-hardening.md) (M4.2a)
**Roadmap:** [`milestones.md`](../milestones.md) v0.7.0 R2

> **v2 changelog (Oracle review):** The routing change is trivial; **the real R2 deliverable is
> authorization.** Added **P0** global name-collision fix (crashes the target supervisor today); reframed
> §6 security around a concrete **confused-deputy** (child inherits the *target's* creds/tools/store but is
> chosen by the *spawner*); resolved D3 (inject marker at registration — YAML strips a class attr) + bounded
> timeout backstop; resolved D4 as **non-breaking `current_spawner` context**; added `spawner_allowlist`
> (D8) + cross-tree audit; added self-target guard (D9) + suspended-target failure mode.

---

## 1. Problem

`AgentProcess.spawn(agent_class, name, config)` only spawns into the **nearest ancestor**
`DynamicSupervisor` (`self._dynamic_supervisor_name`, wired at startup). An agent wanting a child under a
*different* named `DynamicSupervisor` has no public API — it must hand-build a `civitas.dynamic.spawn`
message (fragile, undocumented). GH #10 asks for a first-class helper.

**But cross-tree spawn is fundamentally an authorization change, not just routing** (Oracle): the moment
any agent can target any supervisor, two latent hazards become tree-wide reachable — a **DoS** (§P0) and a
**confused-deputy** (§6). The routing is the easy 10%; the guard rails are the real 90%.

## 2. Current behavior (ground truth, line refs)

- `AgentProcess.spawn()` (`process.py:660-696`) routes to `self._dynamic_supervisor_name`, `spawner=self.name`.
- **`Runtime.spawn(supervisor_name, …)` (`runtime.py:859-891`) already targets a NAMED supervisor** — the
  cross-tree mechanism exists there; `spawn_into` is its agent-side counterpart.
- Routing is **by-name via `registry.lookup`** with **no tree checks** (`bus.py`). `bus.request()` raises
  `MessageRoutingError` **synchronously** for an unknown name (no hang).
- Governance: `on_spawn_requested(self, agent_class, name, config) -> bool` (`supervisor.py:554-562`,
  default `return True`) — **does not receive the spawner**.
- **Child wiring (the crux):** `_handle_spawn` (`supervisor.py:612-623`) injects the **target supervisor's**
  `llm`, `tools`, `store`, `_audit_sink`, `_metrics` into the child; `class_path` + `config` come from the
  **spawner's message**. (Topology `_credentials` are static-only, `runtime.py:646`, and are **not** copied
  to dynamic children — escalation is via the shared `llm`/`tools`/`store` *objects*, not `_credentials`.)
- **Registry is global**; `register()` raises `ValueError` on any duplicate (`registry.py:119-120`). The
  `register()` call in `_handle_spawn` (`supervisor.py:625`) is **not** wrapped; the dup-check
  (`supervisor.py:585`) is **per-supervisor** only.
- `RoutingEntry.capabilities: tuple[str,…]`; YAML `capabilities:` **replaces** class caps at registration
  (`runtime.py:677-683`). `DynamicSupervisor` is in `all_agents` and is registered (extends `AgentProcess`,
  not `Supervisor`).
- Security (M4.2a): spawn messages are **signed** (authn of the spawner); child key-in-message vouching is
  future. Signing is **authentication, not authorization**.
- No existing `spawn_into` reference.

## P0 — Global name-collision crashes the target supervisor (must fix)

`spawn_into(S2, "worker")` when `"worker"` exists **anywhere** (another supervisor's child, or a static
agent): the per-supervisor dup-check (`supervisor.py:585`) passes → `registry.register("worker")` raises
`ValueError` (`registry.py:119`) → propagates out of `_handle_spawn` → `on_error` → `ESCALATE` → **S2
crashes**. Pre-R2 this was hard to reach (ancestor-only); `spawn_into` makes it a one-line cross-tree DoS.
(It is a latent R1/today bug too — fix it here.)

**Fix (two parts, the second is load-bearing):**
1. **Wrap `register()` in `try/except ValueError` → error reply + `_terminal_cleanup`.** This is the *actual*
   guarantee: two **different** supervisors each run their own message loop, so A can pass a pre-check,
   `await on_spawn_requested`/`setup_agent` (yield), then B passes its pre-check for the same global name —
   both reach `register()` and one raises. Only the wrapped `register()` catches that cross-supervisor race.
2. **Global pre-check** beside the existing guards (`supervisor.py:585`,
   `self._registry.lookup(name) is not None → error reply`) for a clean early error in the common
   (single-supervisor) case, before instantiating/wiring the child.

In D7 + tests. Note: within one supervisor there is no interleave (single loop dispatches one spawn at a
time), so the child-facing bookkeeping is safe; the race is strictly *across* supervisors.

## 3. Goals / Non-goals

**Goals:** public `AgentProcess.spawn_into(supervisor_name, …)`; **authorize** cross-tree spawns
(spawner-aware veto + allowlist); clear fast-failure (no silent timeout); fix the P0 collision DoS; reuse
R1 spawn semantics unchanged; DRY (`spawn()` = special case of `spawn_into()`).

**Non-goals:** wire/routing changes; cross-process spawn (R6); full child-key vouching (future M4.2a);
re-architecting child capability inheritance to least-privilege (future).

## 4. Design

`spawn_into` sends the **same** `civitas.dynamic.spawn` message (addressed to `supervisor_name`), so R1's
admission/wait/cleanup/`on_child_terminated` are inherited. `spawn()` delegates:

```python
async def spawn_into(self, supervisor_name, agent_class, name, config=None, *, wait=True) -> str: ...

async def spawn(self, agent_class, name, config=None, *, wait=True) -> str:
    if self._dynamic_supervisor_name is None:
        raise SpawnError("No DynamicSupervisor ancestor found in supervision tree")
    return await self.spawn_into(self._dynamic_supervisor_name, agent_class, name, config, wait=wait)
```

## 5. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| **D1** | `spawn_into(supervisor_name, agent_class, name, config=None, *, wait=True)`; `spawn()` delegates. | DRY; one path; `spawn` = nearest-ancestor sugar. |
| **D2** | Reuse `civitas.dynamic.spawn`, addressed to `supervisor_name`, `spawner=self.name`. | Routing works by-name; no wire change. *(But indistinguishability is why D4/D8 are required — see §6.)* |
| **D3** | **Fast-fail.** `DynamicSupervisor` gets a reserved capability `"_agency.dynamic_supervisor"` **injected at registration unconditionally** (not just a class attr — YAML `capabilities:` would strip it, `runtime.py:677-683`). `spawn_into` does `registry.lookup(supervisor_name)` → `SpawnError` if missing / lacking the marker. Plus a **bounded `ask` timeout** backstop. | The *only* true hang is a live **non-supervisor** recipient (unknown name already raises `MessageRoutingError`; a real DynSup always replies). Marker closes it pre-send; timeout covers a valid-but-wedged target. |
| **D4** | **Spawner-aware governance, non-breaking.** Keep `on_spawn_requested(agent_class, name, config)`; expose a read-only `current_spawner: str \| None` set immediately before the hook and cleared after (`supervisor.py:608`). Safe: the supervisor dispatches one spawn at a time (single message loop). | Adding a `spawner` param would `TypeError` every existing override **at spawn time** (worst failure shape in Alpha). Context attr is non-breaking; (a) is the clean *future* deliberate break. |
| **D5** | Inherit R1 wholesale: `wait=True/False`, inline cleanup, `on_child_terminated`, limits, `_terminal_cleanup`. | `spawn_into` is a different address into the same `_handle_spawn`. |
| **D6** | **P0 collision guard** (above) — global pre-check + wrapped `register()`. | Prevents the cross-tree DoS. |
| **D7** | **Error taxonomy** (all `SpawnError`): no such supervisor; not a DynamicSupervisor; **name already registered** (P0); governance denied; allowlist rejected (D8); limits; start failure (R1); self-target (D9). Wrap `MessageRoutingError`. | One exception, clear messages. |
| **D8** | **`DynamicSupervisor(..., spawner_allowlist: set[str] \| None = None)`** — when set, reject spawners not in it **before** `on_spawn_requested`. Default `None` = today's behavior. Emit an **audit event** on cross-tree admission (spawner, target, class) via the already-wired `_audit_sink`. | Ships the secure recipe in one line; makes D4 the real authz control; blast-radius forensics. |
| **D9** | **Self-target guard:** `spawn_into(self.name)` from a `DynamicSupervisor` → `SpawnError` (would deadlock: `ask` to self while blocked in `handle()`). | `spawn_into` makes self-targeting a first-class path; cheap guard. |

## 6. Security — confused-deputy (the core of R2)

**Trust model:** *the supervisor `S` vouches for and equips the child; the spawner `X` only proposes it.*
Because `_handle_spawn` wires the child with **S's** `llm`/`tools`/`store`/`_audit_sink`/`_metrics`
(`supervisor.py:612-623`) while **X** supplies `class_path` + `config`, `spawn_into(S)` means *"X's chosen
code runs with S's capabilities."* Concretely, X's child can:
- **`store`** (raw `StateStore`) — `get`/`set`/`delete` **any** agent's persisted state through S's handle;
- **`tools`** — invoke any privileged tool in S's registry (fs/shell/http/credentialed);
- **`llm`** — spend S's model credentials/budget.

Signing (M4.2a) authenticates *who X is*; it does **not** authorize *what X's child may wield*. Pre-R2 the
"spawn only into your ancestor" rule was an implicit trust boundary; R2 dissolves it while
`on_spawn_requested` defaults **open**. Therefore R2 must ship the *authorization* controls, not just
routing:
1. **D4** `current_spawner` → the veto hook can see who's asking.
2. **D8** `spawner_allowlist` → one-line rejection of unlisted spawners, checked before the hook.
3. **D8** audit event on cross-tree admission.

Out of scope (future): re-architecting the child to a least-privilege *intersection* of what X may grant
and what S offers — a larger semantic shift. R2's job is to let S **say no**.

## 7. Resolved decisions (from Oracle) + remaining sign-off questions

**Resolved:** D3 mechanism = marker injected at registration + timeout backstop. D4 = non-breaking
`current_spawner` context (not a signature break). Security = `spawner_allowlist` + audit (D8), not
doc-only. API: `wait=False` only (no `spawn_into_nowait`); leave `Runtime.spawn` name as-is.

**Maintainer sign-off (2026-07-04): ✅ go with recommendations.**
1. ✅ **`spawner_allowlist`** built-in `DynamicSupervisor` param (D8); default `None` = today's behavior.
2. ✅ **Reserved `_agency.dynamic_supervisor` capability** (D3); appears in registry / `topology show` introspection (documented).
3. ✅ **Fix P0** here — a duplicate **name** now returns a `SpawnError` reply instead of crashing the target supervisor.
4. ✅ Cross-process targets = **R6** (deferred). Nested dynamically-spawned DynSups: **inject the marker in the child registration path too** (`_handle_spawn`, ~2 lines) so they ARE `spawn_into`-able — no limitation.

## 8. Failure modes

- **Suspended/stopping target:** a `SUSPENDED` supervisor buffers the spawn ask (R1·D3) → **timeout**; the marker check *passes* (still a valid DynSup). The bounded-timeout backstop (D3) is what saves this — document it.
- **Self-target:** guarded (D9).
- **Notify-dead-spawner:** cross-tree `_notify_spawner` → `on_child_terminated` to an X that may have died → best-effort `send` (already tolerant); assert one-line guarantee.
- **Name collision:** P0 guard (D6).

## 9. Test plan

- `spawn_into(other_dynsup, …)` places the child under the **named** supervisor; `spawn()` still nearest-ancestor (R1 suite green).
- Fast-fail: unknown name → `SpawnError` (no timeout); regular-agent name → `SpawnError("not a DynamicSupervisor")` (D3); marker survives a YAML `capabilities:` override.
- **P0:** `spawn_into(S2, "dup")` where `"dup"` exists elsewhere → error reply, **S2 does not crash**; `register()` ValueError path covered.
- **D4:** `on_spawn_requested` can read `current_spawner`; it's `None` outside the hook window.
- **D8:** `spawner_allowlist={"a"}` rejects spawner `"b"` before the hook; cross-tree admission emits an audit event.
- **D9:** `spawn_into(self.name)` → `SpawnError`.
- `wait=False` cross-tree: immediate `ok`; async start-failure → `on_child_terminated` (R1 parity).
- Suspended-target spawn → `SpawnError` via bounded timeout.

## 10. Implementer checklist

- `AgentProcess.spawn_into()` (+ `spawn()` delegates); wrap `ask()` `MessageRoutingError` → `SpawnError`; bounded timeout; self-target guard (D9).
- `DynamicSupervisor`: P0 global-name guard + wrapped `register()` (D6); `current_spawner` property set/cleared around `on_spawn_requested` (D4); `spawner_allowlist` param + pre-hook check + audit event (D8).
- Runtime: inject `_agency.dynamic_supervisor` capability at registration for every `DynamicSupervisor` (D3, unconditional).
- Update the `VetoSupervisor` test override only if needed (signature unchanged under D4).
- Tests per §9; `CHANGELOG [Unreleased]`; docstrings (note `current_spawner` validity window).
