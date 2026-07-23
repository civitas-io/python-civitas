# Cross-Process Dynamic Spawn (v0.7.0 · R6)

**Status:** ✅ Approved — Oracle-reviewed; maintainer signed off 2026-07-04 (go with recommendations). Implementation in progress.
**Source:** [`dynamic-spawning.md`](dynamic-spawning.md) Q6 ("cross-process v0.5, homogeneous deployments"); [`milestones.md`](../milestones.md) v0.7.0 R6
**Builds on:** R1 (non-blocking spawn, merged), R2 (`spawn_into`, merged), M4.2a (signing/vouching)

> **v2 changelog (Oracle review):** The routing reuse is sound, but D3 as drafted was **unsafe** — added
> 7 must-fixes (D8–D14): authenticate `_agency.register`, enforce name ownership (no last-writer-wins),
> verify the Worker-hosted supervisor gets a **full distributed ComponentSet** (linchpin), resolve
> child-key semantics, **namespace child names** for distributed uniqueness, **announce-after-start +
> epoch** to stop resurrection, and **spawn idempotency (spawn-id)** to stop retry double-spawn. Plus:
> Worker owns termination authority; spawner treats `deregister` as authoritative termination; per-spawn
> audit.

---

## 1. Problem

Dynamic spawn is **in-process only**: `DynamicSupervisor._handle_spawn` imports the class and instantiates + starts the child **in the supervisor's own process** (`supervisor.py:596-721`). An agent cannot place a child in a *different* OS process (a Worker). R6 makes spawn work across processes over ZMQ/NATS.

## 2. Current behavior (ground truth)

- `_handle_spawn` (`supervisor.py:596-721`): `importlib` + `agent_class(name)` + wire (`_bus/_tracer/_registry/llm/tools/store/_audit_sink/_metrics/config`) + `_start_nowait` — **all local**; `self._registry.register(child_name)` registers **only in the local process's registry**.
- **Worker** (`worker.py`): hosts a *static* set of agents given at construction (`process: worker` in topology, resolved by `cli/run.py:_run_worker`); starts them, then **announces** each via `_agency.register` (name + capabilities). It has **no runtime spawn handler** today.
- **Registry propagation**: `_agency.register`/`_agency.deregister` messages → `Runtime._on_remote_register` → `registry.register_remote(name, ...)` (`is_local=False`); bus `route()`/`request()` resolve by name and publish over the transport → **cross-process routing already works for known names**.
- **A `DynamicSupervisor` is an `AgentProcess`** — so it can itself be a worker-hosted agent.
- **Signing** (M4.2a): spawn messages are signed by the spawner; the receiver verifies against the spawner's registered public key. The child's identity is loaded from a local `key_dir`.
- Spawn is already a **bus message** (`civitas.dynamic.spawn`, request/reply) + `_notify_spawner` (`civitas.dynamic.terminated`) — both route by name, so both already work cross-process.

## 3. Design approach — reuse the protocol, close 3 gaps

**Cross-process spawn = place a `DynamicSupervisor` in a Worker, then `spawn_into(that_supervisor)`.** The spawn message already routes by name to the remote supervisor; `_handle_spawn` runs *in the Worker*, so the child is instantiated *in the Worker's process*. R2's `spawn_into` is the entry point; R1's non-blocking/reply semantics carry over the wire. **Homogeneous deployment** (every process runs the same image) means the class is importable on the Worker — no code distribution (prior art: Erlang/Akka assume code present; we adopt that).

What's missing for that to actually work:
1. **Cluster-wide child registration** — a child spawned inside a Worker is only in the Worker's *local* registry; the rest of the cluster can't route to it.
2. **Identity provisioning** for the remote child (signing key + public-key announcement).
3. **Cross-process failure/lifecycle** — done-callback + notify + partition detection across the wire.

## 4. Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| **D1** | **No new spawn protocol.** Cross-process spawn is `spawn_into(<supervisor hosted in a Worker>)`; allow a `dynamic_supervisor` topology node to carry `process: worker` so it's Worker-hosted. Existing `civitas.dynamic.spawn` request/reply routes to it over ZMQ/NATS. | Reuses R1/R2 + the bus; homogeneous deployment ⇒ class is importable on the Worker. |
| **D2** | **Announce spawned children cluster-wide.** When `_handle_spawn` runs in a process with a distributed transport, after local register it publishes `_agency.register` (name + capabilities [+ pubkey, D3]); on termination it publishes `_agency.deregister`. Gate on "is this a distributed transport?" (in-process = no-op). | Without this, no other process can `ask()`/route to the child. Reuses the existing announce path Workers use at boot. |
| **D3** ⚠️ | **Remote child identity.** Homogeneous default: the Worker provisions the child key from its `key_dir` (mode `auto` generates one), and the `_agency.register` announcement carries the child's **public key** so peers can verify its signatures (extends the existing spawn-vouch model to the dynamic case). The spawn request itself is signed by the spawner and **verified by the Worker's supervisor** before instantiation (authorization stays with `on_spawn_requested`/`spawner_allowlist` from R2). | Cross-process needs the child's pubkey distributed; signing must not break. This is the security crux — see §5 + Q1. |
| **D4** | **Lifecycle across the wire.** The Worker-side supervisor owns the child's task + done-callback (R1's `_terminal_cleanup`), fires `_notify_spawner` (routes back to the original spawner by name, cross-process), and publishes `_agency.deregister`. Partition/worker-death detected by the **existing heartbeat monitor**; the child's registry entry is reaped. | Reuses R1 cleanup + existing heartbeat; parent learns via `on_child_terminated`. |
| **D5** | **Spawn-time failure semantics.** Import error / not-a-DynamicSupervisor / worker unreachable → `SpawnError` with a clear reason (R2 fast-fail + the R1 bounded `ask` timeout already cover unreachable/wedged targets). Homogeneous mismatch (class missing on Worker) surfaces as an import-error reply. | Prior art: distinguish spawn-time vs runtime failure; give explicit reasons. |
| **D6** | **API.** No new public method — `spawn_into(supervisor_name, …)` is the cross-process entry (the supervisor just happens to live in a Worker). Optionally a docs example + a convenience later. | Minimal surface; R2 already fits. |
| **D7** | **Scope: homogeneous deployments only** (documented). No code serialization/distribution (rejected — Ray/Thespian complexity not warranted); heterogeneous clusters out of scope. | Q6's stated scope; keeps R6 tractable. |
| **D8** ⚠️ | **Authenticate `_agency.register`**: announcements are signed by the announcing Worker's trusted key; `_on_remote_register` verifies against the trusted Worker keyset and drops unsigned/unknown-signer announcements. | Without this, any bus process can announce `register(name=X, pubkey=attacker)` and hijack X's routing/identity. |
| **D9** ⚠️ | **Name ownership on receive**: reject a `register` for a name already owned locally or by a *different* remote owner, and reject name→pubkey **conflicts** (no last-writer-wins takeover). | Blocks name-squatting/takeover even from a signed-but-rogue peer. |
| **D10** ⚠️ | **Verify full distributed ComponentSet** for a Worker-hosted `DynamicSupervisor` (its `_bus`/`_registry` are the distributed ones, plus llm/tools/store) — assert at init. | The linchpin: if Workers host agents with a reduced/local ComponentSet, D2 silently degrades to local-only registration and children are under-wired. Verify before building. |
| **D11** ⚠️ | **Child-key semantics = per-incarnation fresh key**: each spawn generates the child's keypair on the Worker; the **signed announcement (D8) carries the authoritative pubkey**; verifiers trust the latest signed announcement (no cross-Worker cached name→pubkey). *Not* a deterministic per-name key (that replicates one private key to every Worker → any compromised Worker impersonates that child everywhere). | Avoids shared-secret sprawl; announcement-authoritative identity. |
| **D12** ⚠️ | **Namespace child names** for distributed uniqueness (e.g. `<supervisor_id>/<child>` or a spawn-scoped id) + reject-on-conflict (D9) as the safety net. | R2's dup-name guard is per-process; two Workers could otherwise spawn the same global name. |
| **D13** ⚠️ | **Announce-after-start + epoch**: publish `_agency.register` only once the child has started and subscribed (not at spawn-request time); tag register/deregister with a monotonic incarnation/epoch so a late `register` can't resurrect a deregistered name under R1's async `_terminal_cleanup`. | Prevents route-to-not-yet-listening and reorder resurrection. |
| **D14** ⚠️ | **Spawn idempotency**: the spawn request carries a unique `spawn_id`; the handler dedups (name+spawn_id already active → return the existing result, don't re-spawn). Worker is the **authority for termination**; the spawner side only notifies/reconciles and treats child `deregister` as authoritative termination (belt-and-suspenders vs a lost `terminated`). Emit a signed per-spawn audit record (who/what/where/pubkey). | Request/reply retry would otherwise double-spawn; partition must not leak or zombie. |

## 5. Threat model / security

The spawn **request** is signed by the spawner and verified by the Worker-hosted supervisor (authN); authZ is `on_spawn_requested` + `spawner_allowlist` (R2). The **child** gets an identity provisioned on the Worker (D3); its **public key is announced** via `_agency.register` so the cluster can verify its messages. Risks: (a) a rogue process announcing a child pubkey it shouldn't — mitigated by requiring announcements to be signed by the announcing Worker (whose key is trusted); (b) confused-deputy is unchanged from R2 (child inherits the Worker-supervisor's llm/tools/store — the R2 `spawner_allowlist` gate applies); (c) key distribution race (child announced before pubkey known) — the announcement must carry the pubkey atomically. **Signing disabled** (in-process/dev) ⇒ D3 degrades to name-only (documented).

## 6. Resolved decisions (maintainer sign-off — 2026-07-04: "go with recommendations")

Oracle's must-fixes (D8–D14) are folded in above. Decisions:

1. ✅ **Child identity = per-incarnation fresh key** (D11) + signed authoritative announcement; not per-name key, not (yet) a depth-1 cert chain.
2. ✅ **Signed per-spawn audit record now**; depth-1 cert chain deferred.
3. ✅ **Mechanism = Worker-hosted `DynamicSupervisor` + `spawn_into`** with the D10 full-distributed-ComponentSet assertion; no bespoke Worker-spawn RPC.
4. ✅ **Config**: `process: worker` on a `dynamic_supervisor` node + validation that a Worker runs it.
5. ✅ **Scope**: homogeneous-only, no code distribution; heterogeneous deferred.
6. ✅ **Announce trigger**: cluster announcement when `not isinstance(transport, InProcessTransport)`.

## 7. Test plan (outline)

- Integration: Runtime (process A) + Worker (process B) over in-proc-emulated or real ZMQ; a `DynamicSupervisor` hosted in B; `spawn_into(B_sup, EchoAgent, "child")` from an agent in A → child runs in B; `ask("child", …)` from A round-trips.
- Child is announced cluster-wide (A's registry gets a remote entry) and deregistered on despawn/termination.
- `on_child_terminated` fires on the original spawner (in A) when the B-hosted child exits.
- Worker/partition death → child reaped via heartbeat; parent notified.
- Import error on the Worker → `SpawnError` reply with reason.
- Signing on: spawn request verified; child pubkey announced + a peer verifies a child message. Signing off: name-only path works.
- Full existing suite green (in-process spawn unchanged).

## 8. Implementer checklist (once signed off)

- `_handle_spawn`: after local register, publish `_agency.register` (name/caps[/pubkey]) when transport is distributed; publish `_agency.deregister` in `_terminal_cleanup`/on termination.
- Allow `dynamic_supervisor` topology nodes to carry `process: worker`; `cli/run.py`/`worker.py` host + start them; validate assignment.
- Child identity provisioning on the Worker (D3) + pubkey in the announcement; sign announcements.
- Integration tests (§7); `CHANGELOG`; docs (`dynamic-spawning.md` cross-process section + homogeneous-deployment note).

## 9. Non-goals
Heterogeneous clusters; code serialization/distribution/hot-reload; placement groups / resource-aware scheduling; cross-cluster (multi-datacenter) spawn.

---

## Addendum (2026-07-23) — the announce/subscription race (#41, fixed in v0.8.1)

**Defect (design-level, present since R6):** D13's "announce-after-start" guaranteed the child was
*locally subscribed* on its Worker before the cluster-wide announcement — but not that the
subscription had **propagated to peer PUB sockets**. ZMQ PUB sockets silently drop messages for
topics with no known subscriber, and subscription propagation (SUB → XPUB → XSUB → every peer
PUB) is asynchronous: measured ~5–10 ms on Linux, ~20–25 ms on macOS over IPC. The announcement
rides a long-established fast channel (`_agency.register`) and arrives in ~2–4 ms — it
**systematically outruns routability**. A peer that messaged the child immediately after the
announcement published into a void: deterministic loss on macOS, coin-flip on Linux (which is how
R6's verification could pass while the E2E test later failed everywhere — unnoticed because
integration tests were not in CI, #39).

**The same race had a second leg:** per-request ephemeral reply topics. `ZMQTransport.request()`
subscribed a fresh `_reply.<uuid>` and published immediately; a fast responder's reply (~1 ms)
lost to the same 5–25 ms window and died inside the *responder's* PUB socket. Spawn's own ask
survived only because spawn *processing* happened to exceed the window.

**Fix (v0.8.1):**
1. **Subscription-settle barrier** — `ZMQTransport.wait_subscribed(topic)`: a probe topic
   subscribed on the same SUB pipe (FIFO — cannot overtake the target subscription) is
   self-published until it loops back through the proxy. `DynamicSupervisor._announce_child`
   awaits it before announcing, making D13 mean *routable*, not merely *subscribed*. Transport
   protocol gains the method; in-process is a no-op, NATS flushes (broker-side routing).
2. **Stable reply-topic prefix** — each ZMQTransport subscribes ONCE at start to
   `_reply.<instance>.` (propagation absorbed by the startup `wait_ready`); per-request reply
   topics live under that prefix via ZMQ prefix matching. Eliminates leg 2 entirely and removes
   per-request subscribe/unsubscribe churn.

**Residual (recorded, deferred):** the barrier confirms propagation through the Worker's own PUB
pipe; peer PUB pipes receive the same XSUB broadcast in parallel — sub-millisecond jitter is
theoretically possible. A full at-least-once route-establishment guarantee (sender-side
republish + message-id dedup) is a v0.9+ design if ever needed; 10/10 cross-platform E2E runs
show no residual loss.
