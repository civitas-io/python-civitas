# TopologyServer/HTTPGateway merge — design (v0.9.5, AAA prerequisite)

**Status: design confirmed (2026-07-30 review), not yet implemented.** This is the prerequisite
investigation for v0.9.5's "`TopologyServer` AuthN/AuthZ + access control design" backlog item
(`docs/milestones.md`). Per this repo's convention (design doc precedes implementation for
control-plane changes) and its explicit standing instruction on auth work ("hold off on auth,
don't knee-jerk it"), all four open questions in §6 have been resolved by explicit review (see
each decision below); implementation has not started.

## 1. The question that started this

While scoping the AAA design, `TopologyServer` was found to have **zero authentication today** —
acceptable so far because everything it serves is read-only. Investigating *why* it has no auth
led to a bigger question: **why does `TopologyServer` exist as a separate HTTP-serving
implementation at all**, when `HTTPGateway` already has a mature, ASGI-based, already-audited
AuthN story (API key, JWT, mTLS)? Confirmed by direct investigation, not assumed:
`HTTPGateway` actually **predates** `TopologyServer` by five days (2026-04-22 vs 2026-04-27) — this
wasn't "TopologyServer came first, HTTPGateway fixed it later." There's no design doc or commit
message anywhere explaining a deliberate decision to keep them separate. It just happened this
way.

**Decision: merge them.** `TopologyServer` becomes an ordinary agent behind `HTTPGateway`,
inheriting its auth stack instead of reinventing one. This document is the design + migration
plan for that merge.

## 2. Current state (traced through the actual code, not summarized from memory)

### 2.1 `HTTPGateway`'s implementation has four layers with very different coupling

| Layer | File | Coupled to "route to an agent"? | Coupled to ASGI/uvicorn? |
|---|---|---|---|
| Transport | `asgi.py` (`GatewayASGI`) + uvicorn | No | **Yes** — this is uvicorn's own glue |
| Request/response types + middleware chain | `types.py`, `middleware.py` | No | No — pure dataclasses + async functions |
| Auth middlewares | `auth.py` (API key), `jwt_auth.py`, `mtls.py` | `jwt_auth.py`: soft, `getattr(gateway, "_jwt_verifier", None)` — duck-typed, not `isinstance`. `auth.py`/`mtls.py`: **no coupling at all**. `ratelimit.py`: **yes**, calls `request.gateway.call(...)` | No |
| `GatewayDispatcher` | `dispatch.py` | **Yes, fundamentally** — its entire job is `ask()`/`send()` to an agent by name | No |

The only genuinely ASGI/ agent-routing-coupled pieces are the transport layer and
`GatewayDispatcher`. Everything else — the part that actually matters for "inherit the auth
capabilities" — is already generic.

### 2.2 `TopologyServer` doesn't need `GatewayDispatcher` at all

`TopologyServer(GenServer)` → `GenServer(AgentProcess)` — **it is already a real `AgentProcess`**,
with `.call()`/`.ask()`/`.send()` for free (confirmed: `ratelimit.py`'s `request.gateway.call(...)`
would work unmodified against it).

But `Runtime` wires it completely differently from every other agent — `runtime.py` (~L805, ~L855)
directly injects raw Python references, same-process, no bus, no serialization:

```python
agent._root_supervisor = self._root_supervisor
agent._agents = self._agents_by_name
agent._metrics_collector = self._metrics if isinstance(self._metrics, MetricsCollector) else None
```

This is a **deliberate, privileged exemption** from this codebase's own "route by name, never by
object" rule (there's a comment saying exactly that, next to this exact injection). Its seven
endpoints (`/health`, `/topology`, `/agents`, `/agents/{name}`, `/snapshot`, `/metrics`,
`/processes`) are pure functions over that injected state — six of them are synchronous
same-process reads; `/processes` does one real bus round-trip per Worker
(`self._bus.request(_agency.health_probe, ...)`), which `GenServer` already supports natively.

**None of this needs `GatewayDispatcher`'s ask()-to-an-agent-by-name model.** It needs to keep
being privileged-injected exactly as today, just reachable through `HTTPGateway`'s transport +
middleware chain instead of a hand-rolled `asyncio.start_server`.

### 2.3 The one real structural incompatibility: `/metrics` isn't JSON

`GatewayResponse.body` is always JSON-encoded (`asgi.py`'s `_respond()`: `json.dumps(response.body)`
unconditionally). `TopologyServer`'s `/metrics` returns real Prometheus text-format
(`text/plain; version=0.0.4; charset=utf-8`), not JSON. Today's only precedent for a non-JSON
response (`/docs`, `/openapi.json`) bypasses the whole middleware/dispatch chain entirely via a
hardcoded special case in `_handle_http()` — which would defeat the point here (a hardcoded bypass
never gets auth middleware applied to it). This needs a **real, governed extension**, not a third
copy-pasted bypass — see D4 below.

### 2.4 The dependency-cost question is narrower than it first looked

`civitas[http]` (uvicorn + pydantic) is imported lazily inside `HTTPGateway.on_start()` — paid
only by whoever runs the **Runtime process hosting the gateway**. `civitas/dashboard/client.py`
(what `civitas dashboard`/`topology show --live` actually use) is a plain `asyncio.open_connection`
+ manual HTTP/1.1 parsing client — **zero HTTPGateway/uvicorn dependency, and doesn't need to
change syntactically to keep working**, since it just does `GET /path` over TCP and reads to EOF
on `Connection: close`, which uvicorn honors like any compliant HTTP/1.1 server. So the dependency
cost lands correctly on the side that's already about to need a real HTTP stack for auth anyway
(the server), not on every dashboard viewer.

## 3. Proposed design

### D1 — `TopologyServer`'s HTTP-serving half is deleted; its data half becomes a `GenServer`

New (or repurposed) `TopologyAgent(GenServer)` keeps the **exact same privileged injection
contract** (`_root_supervisor`/`_agents`/`_metrics_collector`, injected by `Runtime` exactly as
today — this does not change). It loses: `_server`, `init()`'s `asyncio.start_server`, `on_stop()`'s
socket cleanup, `_handle_connection()`, and `_route_http()`'s manual path parsing. It gains a
`handle_call(payload, from_)` that dispatches on a `payload["__op__"]` key (`"topology"`,
`"agents"`, `"agent_detail"`, `"snapshot"`, `"metrics"`, `"processes"`, `"health"`), each calling
the exact same `_serialize_node`/`_build_agents_list`/`_build_agent_detail`/`_build_metrics`/
`_build_prometheus_metrics`/`_build_processes` methods, unchanged. `/processes`'s existing
`await self._bus.request(...)` keeps working — `GenServer` already has `self._bus`.

### D2 — Introspection routes are auto-registered, not user-declared

The seven endpoints are **fixed, canonical, not user-remappable** — matching today's zero-config
expectation (`civitas dashboard topology.yaml` needs to keep working with no route configuration).
A new `GatewayConfig.topology_agent: str | None = None` field: when set to an agent name, the
gateway auto-registers those seven routes pointing at that agent, with sane per-route defaults
(D5). This is NOT the same as declaring them by hand in `routes:` — no boilerplate needed for the
common case. See D6d for the one confirmed refinement: a whole-surface path prefix is supported,
per-endpoint remapping is not.

### D3 — YAML stays backward compatible: `type: topology_server` keeps existing, reinterpreted

The `topology_server` YAML node type is **not removed**. `Runtime`'s topology loader keeps
building something from it, but instead of a raw `TopologyServer`, it now constructs **two**
objects under the hood: a plain `TopologyAgent(GenServer)` (privileged-injected as today) and an
internally-owned `HTTPGateway` configured with `topology_agent=<that agent's name>`. The existing
`config: {host, port}` block maps onto the internal gateway's `host`/`port` unchanged. A **new**
optional `config: {auth: {...}}` sub-block (reusing `GatewayAuthConfig`/`http_gateway`'s existing
`middleware:` list shape) is the opt-in auth surface — omitted entirely, the behavior is
byte-for-byte what exists today (no auth, same default host/port).

**This means zero YAML migration for the simple case.** Every existing `topology.yaml` with a bare
`type: topology_server` node keeps working unchanged, including `examples/dashboard_demo/`'s and
the 4 files currently declaring it.

**Optional refinement, not required for the base merge**: `config: {attach_to: <gateway-name>}` to
mount the introspection routes onto an **already-declared** `http_gateway` node instead of
spinning up a dedicated one — for deployments that want introspection and agent-facing routes on
one port. Worth having, not blocking.

### D4 — `GatewayResponse` gains a governed raw-body escape hatch, for `/metrics` only

`GatewayResponse` gains two new optional fields: `raw_body: bytes | None = None`,
`content_type: str | None = None`. `_respond()` checks `raw_body is not None` before the existing
`json.dumps(response.body)` path. This is **only reachable for routes explicitly marked**
(`RouteEntry.raw_response: bool = False` — auto-set `True` only for the auto-registered `/metrics`
route from D2) — not a general "any handler can return anything" hole. `TopologyAgent.handle_call`
returns a plain dict as `GenServer` requires (`{"__raw_body__": ..., "__content_type__": ...}`);
`_result_to_response()` recognizes this sentinel **only when `entry.raw_response` is set**, and
converts it to `GatewayResponse(raw_body=..., content_type=...)`.

### D5 — Per-route auth, resolved here, not deferred again

This is where the actual AAA questions from the parent conversation get answered concretely:

- `/health` gets **no auth middleware** by default, even when the rest of the introspection routes
  do — matches the universal expectation that liveness checks stay reachable (k8s probes, LB
  health checks) — and is configurable per the same `middleware:` mechanism if an operator
  disagrees.
- `/topology`, `/agents`, `/agents/{name}`, `/snapshot`, `/metrics`, `/processes` all read the
  same `config.auth` middleware list — one shared read-tier, matching how they're all read-only
  today. No per-endpoint differentiation needed *yet* because there's no per-endpoint sensitivity
  differential yet — that changes once write routes exist (§5 migration plan, step 7).
- **This is also what unblocks the rest of v0.9.5's backlog** (suspend/resume, kill, mailbox
  ops): once write routes exist, they're just additional `RouteEntry` items with their **own**,
  stricter middleware list — the exact per-route AuthZ granularity that was previously described
  as "not yet designed" now falls directly out of the route table's existing shape, at zero extra
  mechanism cost.

### D6 — `TopologyServer` (old class): removed — deliberate breaking change (decided)

`TopologyServer` is a public, importable name (`from civitas import TopologyServer`).
**Decided: option (b)** — `TopologyServer` is removed entirely, matching this project's history
of explicit, documented breaking changes when justified (e.g. v0.9.3's `/metrics`→`/snapshot`
rename). YAML users are unaffected (D3 preserves the `type: topology_server` node shape); only
direct-construction Python callers (`TopologyServer(name, host, port)` outside of YAML) need to
migrate to constructing an `HTTPGateway` + `TopologyAgent` themselves. This needs a CHANGELOG
"Breaking" callout and a migration note at release time.

### D6b — `config: {auth: {...}}` reuses `GatewayAuthConfig` verbatim (decided)

A `topology_server` node's optional `auth:` block reuses `GatewayAuthConfig` exactly as
`http_gateway` nodes already do (`mtls: {ca_cert, client_cert_mode, mtls_source,
trusted_proxy_cidrs}`, plus a `middleware:` list for API-key/JWT) — one auth-config shape in the
whole YAML schema, not a second parallel one. Additional, genuinely topology-introspection-specific
fields can be added later if a real need shows up; none is known today.

### D6c — `attach_to` (mount onto an existing gateway): deferred, tracked (decided)

The optional `config: {attach_to: <gateway-name>}` refinement (mount introspection routes onto an
already-declared `http_gateway` node, sharing one port/process) is a real, legitimate deployment
simplification but is **explicitly deferred** — not part of this migration (D1-D6). Tracked in
`docs/milestones.md` as a follow-on once the base merge (dedicated-internal-gateway-per-
`topology_server`) is built, tested, and released — not silently dropped.

### D6d — Fixed routes, with an optional whole-surface prefix (decided, refines D2)

The seven introspection paths themselves stay fixed and non-remappable (confirmed: no
per-endpoint renaming, no removing/adding individual endpoints, no changing methods — rejected as
over-general for what is a small, canonical, always-the-same introspection surface). **Refinement
from review**: a `topology_server` node's `config:` block gains an optional `prefix: str = ""`
field, applied uniformly to all seven fixed paths — e.g. `prefix: "/v1"` turns them into
`/v1/health`, `/v1/topology`, `/v1/agents`, `/v1/agents/{name}`, `/v1/snapshot`, `/v1/metrics`,
`/v1/processes`. This is deliberately simple: one prefix for the whole surface, not per-endpoint
control — matches "simplify for now" from review; a richer remapping scheme can be revisited later
if a real need for it shows up. `GatewayConfig` gains a matching `topology_prefix: str = ""` field
that D2's auto-registration logic consults when building the seven `RouteEntry` path patterns.

### D7 — Follow-on, not blocking this merge: `dashboard/client.py` needs to learn to send auth headers

Once a `topology_server`/`http_gateway`-merged node can require auth, `civitas dashboard`/
`civitas top`/`topology show --live`'s polling client (`civitas/dashboard/client.py`) needs a way
to attach `Authorization`/`X-API-Key` headers to its requests (today it sends none). This is real,
necessary follow-on work for auth to be *usable* end-to-end, but is separable from this merge —
tracked here so it isn't lost, not scheduled as part of D1-D5.

## 4. What does NOT change

- The privileged injection contract (`_root_supervisor`/`_agents`/`_metrics_collector`,
  `Runtime`-owned direct references, no bus hop) — this is `TopologyAgent`'s entire reason to
  exist and stays exactly as designed today.
- `civitas/cli/_topology_discovery.py`'s `find_topology_server()` — since the YAML node type and
  its `config: {host, port}` shape are unchanged (D3), this file needs **no functional change**.
- Multi-cluster (v0.9.4) — each cluster's `topology_server` node still maps to its own
  internally-owned gateway/port; the `civitas dashboard a.yaml b.yaml` shape is unaffected.
- The JSON response shapes of all six JSON endpoints — `TopologyAgent`'s `handle_call` calls the
  exact same, unchanged serializer methods.

## 5. Migration plan (phased, each phase independently testable)

1. **`GatewayResponse` raw-body escape hatch (D4)** — small, isolated, testable against a synthetic
   route before anything else changes. No behavior change for existing JSON routes.
2. **Build `TopologyAgent(GenServer)`** wrapping today's serializer logic via `handle_call`,
   verified for byte-for-byte JSON parity against the current `TopologyServer` (same fixtures,
   same expected bodies) — a pure refactor, no new behavior yet.
3. **`GatewayConfig.topology_agent` + auto-route-registration (D2/D6d)** — teach `HTTPGateway` to
   mount the seven fixed routes when configured, with the D5 per-route auth defaults and the
   optional `topology_prefix` applied uniformly.
4. **`Runtime`'s topology loader (D3)** — `type: topology_server` now constructs the
   `TopologyAgent` + internally-owned `HTTPGateway` pair; verify every existing example topology
   (`examples/dashboard_demo/`, the 3 doc-referenced ones) still round-trips unchanged with zero
   YAML edits.
5. **Real end-to-end verification**: a real running `civitas run` process, `civitas dashboard`
   attaching to it exactly as today (no client-side changes needed per D7's scoping), a real
   Prometheus scrape against the merged `/metrics` (proving D4's escape hatch works against a real
   scraper, not just a unit test), and — new — a real JWT-protected request against `/topology`
   proving the inherited auth actually rejects/accepts correctly.
6. **D6 execution** — remove the old `TopologyServer` class entirely (deliberate breaking change,
   decided); delete the dead `asyncio.start_server`/`_handle_connection`/`_route_http` code, which
   becomes fully dead the moment step 4 lands. CHANGELOG "Breaking" callout + migration note for
   any direct-construction (non-YAML) caller.
7. **Only after 1-6 are done and released**: build the actual write actions (suspend/resume, the
   next AAA backlog item) as new routes on this now-merged, now-auditable surface.

## 6. Review outcome (2026-07-30) — all four questions resolved

1. **D6**: (b) — deliberate breaking change. `TopologyServer` is removed; YAML unaffected.
2. **D6b**: `config: {auth: {...}}` reuses `GatewayAuthConfig` verbatim, with room to add
   topology-introspection-specific fields later if a genuine need appears.
3. **D6c**: `attach_to` (mount onto an existing `http_gateway`) is deferred — tracked, not built
   as part of this migration.
4. **D6d**: fixed, auto-registered routes confirmed, refined with one addition: a whole-surface
   `prefix` (e.g. `/v1`) is supported; per-endpoint remapping is not ("simplify for now").

All open questions resolved — proceeding to implementation per the §5 migration plan.
