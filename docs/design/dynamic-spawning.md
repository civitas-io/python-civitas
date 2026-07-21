# Design: Dynamic Agent Spawning (M4.1b)

**Status:** In design — v0.4
**Author:** Jeryn Mathew Varghese
**Last updated:** 2026-04

---

## Motivation

Civitas topologies today are fully static: all agents and supervisors are declared in YAML (or code) before the runtime starts, and the supervision tree does not change shape at runtime. This is intentional — static trees are easy to reason about, and most production systems should be static.

However, LLM-driven orchestrators often need to create specialist agents on demand and decommission them when work is done. A research orchestrator might spawn one `ResearchAgent` per query; a paralleliser might spin up N workers for a batch job. Hardcoding the worker count defeats the purpose.

Dynamic agent spawning adds a controlled runtime escape hatch: a designated `DynamicSupervisor` node can accept new children at runtime, subject to capacity and governance constraints, while keeping the rest of the tree static and predictable.

---

## OTP Analogy

Erlang separates `Supervisor` (static child spec, all three restart strategies) from `DynamicSupervisor` (starts empty, children added at runtime, ONE_FOR_ONE only). The separation exists because ONE_FOR_ALL and REST_FOR_ONE strategies depend on a fixed, ordered child list — they lose coherence when children arrive and leave dynamically. ONE_FOR_ONE is the only strategy where each child is fully independent and restart decisions never consult sibling state.

Civitas follows the same separation for the same reasons.

| OTP | Civitas |
|-----|---------|
| `Supervisor` (static) | `Supervisor` (static, all strategies) |
| `DynamicSupervisor` | `DynamicSupervisor` (starts empty, ONE_FOR_ONE only) |
| `DynamicSupervisor.start_child/2` | `self.spawn(AgentClass, name, ...)` |
| `DynamicSupervisor.terminate_child/2` | `self.despawn(name)` |

---

## Design Decisions

### D1 — `DynamicSupervisor` is a first-class node, not an extension of `Supervisor`

`Supervisor` keeps its fixed child spec and full strategy support. `DynamicSupervisor` is a separate class that starts with an empty child list and enforces ONE_FOR_ONE. This keeps both abstractions simple and avoids strategy carve-outs inside `Supervisor`.

### D2 — `DynamicSupervisor` is declared as a static child in topology YAML

The supervisor itself is a fixed, named node in the tree. What changes at runtime is its *children*. This means:

- The static tree structure is always visible in `topology show`
- The `DynamicSupervisor` appears as a named node; dynamic children hang off it
- Blast radius is contained — dynamic children cannot affect static siblings
- Shutdown is clean — stopping the `DynamicSupervisor` stops all its children in one sweep

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - name: orchestrator
      type: OrchestratorAgent
    - name: workers
      type: dynamic_supervisor
      max_children: 20
```

```
root (Supervisor, ONE_FOR_ONE)
├── orchestrator  (OrchestratorAgent, static)
└── workers       (DynamicSupervisor, static node — dynamic children)
    ├── researcher-1  (spawned at runtime)
    └── researcher-2  (spawned at runtime)
```

### D3 — `self.spawn()` targets the nearest ancestor `DynamicSupervisor`


An agent does not name the target supervisor. The runtime walks up the ancestry chain from the calling agent and finds the first `DynamicSupervisor`. If none exists in the chain, `self.spawn()` raises `SpawnError`.

Rationale:
- The topology YAML already makes the relationship explicit — the `DynamicSupervisor` is declared above the spawning agent, so reading the YAML tells you where children land
- Naming the target at the call site would duplicate information already in the topology
- Allowing agents to spawn into *remote* subtrees would couple unrelated parts of the tree; if cross-tree spawning is needed, the correct pattern is to send a message to the agent that owns that `DynamicSupervisor`

```python
# Inside OrchestratorAgent.handle()
# Finds the nearest DynamicSupervisor ancestor ("workers") automatically
agent_name = await self.spawn(ResearchAgent, name="researcher-1", config={"topic": "..."})
await self.despawn("researcher-1")
```

> **`on_start()` runs *inside* the spawn call — keep it fast.** The spawn is only
> confirmed once the new agent's `on_start()` returns, so `await self.spawn(...)`
> blocks the spawner for the full `on_start()` duration and raises `TimeoutError`
> if it outlasts the ask timeout. Put slow / I/O-bound work (LLM calls, browser
> sessions) in `handle()`, kicked off by a fire-and-forget message sent right after
> the spawn is confirmed — not in `on_start()`:
>
> ```python
> name = await self.spawn(WorkerAgent, name="worker-1", config={"job": job_id})
> await self.send(name, {"type": "start_job"})   # fire-and-forget kickoff
> ```
>
> The `config` passed to `spawn()` is applied to the spawned agent and readable as
> `self.config` (in both `on_start()` and `handle()`).

### D4 — `on_spawn_requested` is a governance veto hook

Before a spawn is executed, the runtime calls `on_spawn_requested(agent_class, name, config)` on the `DynamicSupervisor`. The default implementation approves all requests. Subclassing allows governance logic: rate limits, allowlists, policy checks.

```python
class GovernedWorkerPool(DynamicSupervisor):
    async def on_spawn_requested(
        self, agent_class: type, name: str, config: dict
    ) -> bool:
        if agent_class not in ALLOWLIST:
            logger.warning("Spawn of %s denied by policy", agent_class.__name__)
            return False
        return True
```

### D5 — Two decommission operations: `despawn()` and `stop()`

Decommissioning a dynamic child has two explicit operations with distinct semantics:

**`despawn(name)`** — hard stop. Cancels the agent's asyncio task immediately. `on_stop()` still fires. Pending `ask()` callers into the agent receive `SpawnError("agent despawned")`. The slot is freed immediately. Use when you need the capacity back now and don't care about in-flight work.

**`stop(name, drain, timeout)`** — soft stop. Awaitable — returns when the agent is fully stopped. The agent stops accepting new messages immediately (senders receive `SpawnError("agent stopping")`), then:

- `drain="current"` — finishes the message currently being handled, runs `on_stop()`, stops
- `drain="all"` — drains the full mailbox, runs `on_stop()`, stops
- `timeout` (default 30.0s) — if drain isn't complete within the timeout, falls back to a hard stop, then returns

```python
# Hard stop — immediate, slot freed now
await self.despawn("worker-1")

# Soft stop — finish current message, then stop
await self.stop("worker-1", drain="current")

# Soft stop — drain full mailbox, up to 60s, then hard stop if needed
await self.stop("worker-1", drain="all", timeout=60.0)

# Safe to spawn a replacement immediately after either
await self.spawn(ResearchAgent, name="worker-1", config={...})
```

### D6 — Spawn requests are bus messages from day one

`self.spawn()` always sends a `civitas.dynamic.spawn` message to the `DynamicSupervisor` by name — even in-process. The message carries the dotted class path and serialised config rather than a class reference (classes cannot be serialised across process boundaries).

```python
# Internal message shape — not part of public API
{
    "type": "civitas.dynamic.spawn",
    "class_path": "myapp.agents.ResearchAgent",  # dotted import path
    "name": "researcher-1",
    "config": {...},                              # must be JSON-serialisable
}
```

This means:

- **v0.4 (in-process)**: supervisor receives the message, imports the class locally, instantiates it. Call site: `await self.spawn(ResearchAgent, name="researcher-1", config={...})` — the runtime resolves `ResearchAgent` to its dotted path automatically.
- **v0.5 (cross-process)**: same message, same path, routed over ZMQ/NATS to the worker process running the supervisor. The receiving worker imports and instantiates the class. Requires homogeneous deployments where all workers have the same codebase.

The public API (`self.spawn()`, `self.despawn()`, `self.stop()`) never changes between versions. The transport is the only difference.

### D7 — Two independent capacity limits

`DynamicSupervisor` supports two optional, independent limits:

- **`max_children`** — concurrent live children. A slot is freed when a child exits or is despawned. Prevents resource exhaustion.
- **`max_total_spawns`** — lifetime spawn budget. Monotonically increasing, never resets. Useful for audit, billing, or security constraints.

`self.spawn()` raises `SpawnError` if either limit is reached, with a clear reason in the message. Both limits are in-memory — they reset if the `DynamicSupervisor` crashes and restarts. For durable budgets, track spawn counts in `self.state` on the orchestrator and enforce via `on_spawn_requested`.

```yaml
- name: workers
  type: dynamic_supervisor
  max_children: 20        # at most 20 alive at once (default: unbounded)
  max_total_spawns: 1000  # at most 1000 spawns ever (default: unbounded)
```

---

## Restart Semantics (Q2 — resolved)

Dynamic children use **transient** restart mode by default. The `restart` field is configurable per `DynamicSupervisor`.

| Exit type | `permanent` | `transient` (default) | `never` |
|-----------|-------------|----------------------|---------|
| Crash (abnormal exit) | Restart | Restart | Remove, notify |
| Clean exit / `despawn()` | Restart | Remove | Remove |
| Restarts exhausted | Escalate to parent | Remove, notify orchestrator | — |

**Key rule — no escalation on exhaustion.** When a dynamic child exhausts its restarts, the `DynamicSupervisor` removes the child and fires `on_child_terminated(name, reason)` on the spawning agent. It does **not** escalate to its parent supervisor. Escalating would bring down the static tree over a transient worker failure, defeating the purpose of containment.

**Notification hook on `AgentProcess`:**

```python
async def on_child_terminated(self, name: str, reason: str) -> None:
    """Called when a dynamically spawned child is permanently removed.

    reason is one of: "restarts_exhausted", "despawned", "clean_exit"
    Default implementation logs a warning. Override to re-spawn, alert, etc.
    """
```

**Dynamic child list is in-memory only.** If the `DynamicSupervisor` itself crashes and is restarted by its parent, it starts empty. Orchestrators that need durability must checkpoint spawned agent names via `self.state`.

```yaml
- name: workers
  type: dynamic_supervisor
  max_children: 20
  restart: transient      # permanent | transient (default) | never
  max_restarts: 3
  restart_window: 60
```

---

## Open Questions

The following questions are deferred to design sessions before implementation:

- **Q3 — `on_spawn_requested` placement**: Is the hook on `DynamicSupervisor`, on `AgentProcess`, or both? Who is the right place to enforce governance?
- **Q4 — `max_children` semantics**: Is the limit on concurrent live children, total ever spawned, or both?
- ~~Q5 — Despawn semantics~~ → two explicit operations: `despawn()` (hard stop) and `stop()` (soft stop, awaitable, drain="current"|"all", timeout fallback)
- ~~Q6 — Cross-process spawning~~ → bus message protocol from day one; in-process only in v0.4; v0.5 routes same message to remote worker (homogeneous deployments only)
- ~~Q7 — `topology show` live state~~ → `TopologyServer(GenServer)` supervised HTTP endpoint; CLI pings `GET /topology` for live tree; falls back to static YAML if unreachable

### D8 — `TopologyServer(GenServer)` exposes a JSON HTTP management endpoint

`topology show` gets live state by pinging a supervised `TopologyServer` GenServer running inside the runtime. It is declared as a normal child in topology YAML — optional, supervised, lifecycle-bound to the runtime.

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - name: orchestrator
      type: OrchestratorAgent
    - name: workers
      type: dynamic_supervisor
      max_children: 20
    - name: topology_server
      type: topology_server       # built-in shorthand, like http_gateway
      config:
        host: 127.0.0.1           # default: localhost only
        port: 6789                # default
```

**Endpoints (read-only, JSON):**

```
GET /topology           → full supervision tree (static + live dynamic children)
GET /agents             → flat list of all running agents + status
GET /agents/{name}      → single agent: status, restart count, metrics
GET /health             → liveness check {"status": "ok"}
```

**`topology show` behaviour:**

1. Reads topology YAML to find `topology_server` config (`host` + `port`)
2. Attempts `GET /topology` — if reachable, renders full live tree with dynamic children populated and `DynamicSupervisor` nodes visually distinguished (dotted border / `[dynamic]` label)
3. If not reachable (runtime not running), renders static YAML tree with `(runtime not running)` annotation on `DynamicSupervisor` nodes

**Why a GenServer, not a standalone HTTP server:**

- Supervised — crashes are restarted automatically by the parent supervisor
- Lifecycle-bound — starts and stops with the runtime, no orphaned processes
- Direct access to runtime internals — queries the supervisor tree and registry without IPC
- Consistent — everything in Civitas is a supervised process; the management endpoint is no exception
- Universal — JSON over HTTP works on all platforms, in containers, with `curl`, and with the Textual dashboard

**Future — Textual dashboard:**

`civitas dashboard` will be rebuilt on [Textual](https://textual.textualize.io/) (interactive Python TUI framework). It will consume `TopologyServer` endpoints for live tree rendering and per-agent metrics — the same JSON, the same endpoint, no additional protocol.

---

## API Surface (provisional)

```python
# DynamicSupervisor — topology YAML
# type: dynamic_supervisor
# max_children: 20
# max_total_spawns: 1000
# restart: transient
# max_restarts: 3
# restart_window: 60

# AgentProcess — spawn / decommission
await self.spawn(AgentClass, name="worker-1", config={...})  # -> str (agent name)
await self.despawn("worker-1")                               # hard stop
await self.stop("worker-1", drain="current")                 # soft stop, finish current message
await self.stop("worker-1", drain="all", timeout=60.0)       # soft stop, drain mailbox

# AgentProcess — lifecycle notifications
async def on_child_terminated(self, name: str, reason: str) -> None: ...
# reason: "restarts_exhausted" | "despawned" | "clean_exit"

# DynamicSupervisor — governance hook
async def on_spawn_requested(
    self, agent_class: type, name: str, config: dict[str, Any]
) -> bool: ...

# Runtime — external entry points (for non-agent callers)
await runtime.spawn("workers", ResearchAgent, name="researcher-1", config={...})
await runtime.despawn("workers", "researcher-1")
await runtime.stop("workers", "researcher-1", drain="all", timeout=30.0)
```

---

## Cross-Process Spawning (v0.7.0 · R6)

Cross-process spawn is now implemented. See
[`cross-process-spawn.md`](cross-process-spawn.md) for the full design (decisions
D1–D14). The essentials:

- **Mechanism — no new protocol.** Cross-process spawn *is* `spawn_into(<supervisor
  hosted in a Worker>)`. A `dynamic_supervisor` topology node may carry
  `process: worker`; the Worker hosts it (a `DynamicSupervisor` is itself an
  `AgentProcess`), and the existing `civitas.dynamic.spawn` request/reply routes to
  it over ZMQ/NATS. `_handle_spawn` runs *in the Worker*, so the child is
  instantiated in the Worker's process.

  ```yaml
  supervision:
    name: root
    children:
      - name: orchestrator
        type: myapp.Orchestrator
      - name: workers
        type: dynamic_supervisor
        process: worker          # hosted in a Worker, spawns children cross-process
        max_children: 50
  ```

- **Homogeneous deployment only.** Every process runs the same image, so the spawned
  class is importable on the Worker — there is no code distribution (prior art:
  Erlang/Akka assume code is present). A class missing on the Worker surfaces as an
  import-error `SpawnError`.

- **Full distributed ComponentSet (linchpin).** A Worker-hosted `DynamicSupervisor`
  asserts at start that its `_bus`/`_registry` are the cluster-wide ones; otherwise a
  spawned child would register locally only and be unreachable — the misconfiguration
  fails fast with `ConfigurationError`.

- **Cluster-wide registration.** After a child reaches RUNNING the supervisor
  publishes a signed, epoch-tagged `_agency.register` (name, capabilities, and — when
  signing is on — the child's per-incarnation public key). Termination publishes
  `_agency.deregister`. In-process spawn (`InProcessTransport`) announces nothing, so
  the v0.4 path is unchanged.

- **Security.** Announcements are authenticated and the registry enforces name
  ownership (no takeover, no last-writer-wins) and incarnation epochs (no
  resurrection). With signing disabled (in-process / dev) the announcement degrades to
  name-only.

## Non-Goals (v0.4)

- ~~Cross-process spawning (ZMQ / NATS)~~ → implemented in v0.7.0 · R6 (above)
- Spawning into a remote `DynamicSupervisor` by name from an unrelated subtree
- Visual topology editor integration (M4.1 is deferred)
- Per-agent spawn quotas (only global `max_children` per `DynamicSupervisor` in v0.4)
- Textual dashboard — planned as follow-on; `TopologyServer` endpoints are the foundation

## Non-Goals (R6 cross-process)

- Heterogeneous clusters / code serialization / hot-reload
- Placement groups / resource-aware scheduling
- The depth-1 spawner-vouch certificate chain (audit record only for now)
- Multi-datacenter (cross-cluster) spawn

---

## Addendum (2026-07-21) — head-of-line blocking on wait=True spawns

Verified finding (supervision review, [`supervision-hardening.md`](supervision-hardening.md) B4):
`_handle_spawn` with `wait=True` awaits the child's readiness *inside the DynamicSupervisor's own
`handle()`*. One child with a slow `on_start()` blocks **all** other spawn/despawn/stop requests
to that supervisor for up to the 30 s spawn timeout, and a child that messages its own spawning
supervisor from `on_start()` deadlocks against it. Mitigation today: keep `on_start()` fast (as
its docstring already instructs) and prefer `wait=False` for bulk spawning. Fix direction: move
the readiness wait off the message loop (background task + deferred reply), which requires the
reply-from-task plumbing introduced for announce-after-start (D13).
