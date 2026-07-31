# Control-plane write actions + the AuthNZ integration seam — design (v0.9.6)

**Status: design draft, not yet implemented.** First write-action slice for the dashboard
control-plane, building on v0.9.5's TopologyServer/HTTPGateway merge
([`topology-gateway-merge.md`](topology-gateway-merge.md)). Design-first per repo convention
(control-plane mutation).

## 1. The decision that shapes everything: civitas ships mechanism, not policy

**civitas will not build AuthZ.** No roles, no scopes, no SCIM connector, no IdP integration.
Customers already have their own AuthN/AuthZ (SCIM, IdPs, OPA, …); civitas's job is to provide a
clean **seam** to plug those in, an **honest audit binding** so the authenticated identity (not a
spoofable client field) is what gets recorded, and a **safe zero-config default** so a single dev
can run with no ceremony at all.

Same "mechanism, not policy" split already applied to Presidium (civitas ships the `suspend`
primitive; Presidium owns the HITL policy) and to `SuspendCategory` (civitas ships the structured
category; the caller owns what to send).

> **The model in one line:** civitas ships the middleware **seam**, a **principal** convention, an
> honest **audit binding**, and a **safe localhost default**. The customer brings their middleware
> (= their AuthN *and* AuthZ). A single dev brings nothing.

## 2. What already exists (verified, not assumed)

- **The seam is `HTTPGateway`'s middleware chain.** A middleware is `async (request, next) ->
  response`. It authenticates against whatever the customer runs, decides allow/deny (returns 403
  itself on deny — *that is their AuthZ; civitas never sees a role or scope*), and on allow calls
  `next()`. civitas runs whatever middleware a route carries. Per-route middleware already works
  (v0.9.5 phase 3), so write routes can carry stricter middleware than read routes purely by the
  customer's configuration — civitas models no tiers itself.
- **`GatewayRequest.auth: dict[str, Any] | None`** already exists, built for exactly this
  ("verified identity from auth middleware; never merged into the dispatched payload to avoid
  reserved-key collisions").
- **`require_jwt` already populates it**: `request.auth = {**(request.auth or {}), "claims":
  claims}`. **`require_api_key` does not** (a shared secret has no per-user identity).
- **`suspend`/`resume` already exist and are already audited** (`_agency.suspend`/`_agency.resume`
  priority messages; `resume` carries an `approver` recorded in the `agent.resume` AuditEvent;
  `suspend` records `reason`/`category`). `Runtime.suspend()`/`resume()` just `self._bus.route(...)`
  those messages — and `TopologyAgent` already holds `self._bus`.

So the write action is mostly **wiring existing pieces**; the genuinely new part is the principal
→ audit binding and the safe default.

## 3. The three things civitas needs to add (the minimal ground)

### D1 — Convention: middleware sets `request.auth["principal"]` (a dict) — RESOLVED

The entire contract civitas asks of an integrator: *tell me who this is.* A middleware that
authenticates a caller sets `request.auth["principal"]` to a **dict** (chosen over a bare string
for scalability — fields can be added without changing the contract) with one required key:

```python
request.auth["principal"] = {"id": "alice", "method": "jwt"}  # id required; anything else optional
```

- `"id"` is the stable identity string (username, service-account id, cert DN — whatever is
  meaningful in the customer's system). This is the only field civitas reads.
- Middleware may add any sibling keys it likes (`method`, `email`, `groups`, …); civitas carries
  them through untouched but does not interpret them (that would be AuthZ).

- `require_jwt` will set `request.auth["principal"] = {"id": <sub>, "method": "jwt"}` (it already
  has the claims in hand; `claims` stays alongside).
- `require_api_key` stays principal-less (a shared secret identifies no person) — a request
  authenticated only by API key has no `principal`, and falls to the default (D3).
- A customer's SCIM/OPA/IdP middleware sets `principal` from their verified user. **That is the
  whole integration.**

### D2 — The honest audit binding (the one real new wire)

Today the dispatched message payload is `{**body, **path_params, **payload_extra}` — `request.auth`
is deliberately excluded. A write route must carry the authenticated principal into the
`_agency.suspend`/`_agency.resume` message → the `AuditEvent`, under a **reserved key the client
cannot set** (never a body field — that is trivially spoofable: `{"approver": "whoever_i_say"}`).

Concretely: the ASGI dispatch path injects `request.auth`'s principal **dict** into the dispatched
payload under a reserved key `__principal__`, the same way `payload_extra` injects `__op__` — merged
**last** so a client body value can't override it. `TopologyAgent`'s write handler reads
`__principal__` (never `body`), takes `principal["id"]` as the scalar recorded value, and uses it as
the `approver` (resume) / `initiated_by` (suspend) field in the message that becomes the
AuditEvent. The whole principal dict is threaded through so future write actions can use richer
fields without re-plumbing; v1 records the `id` string.

**This is the crux of the whole feature.** "alice suspended agent X" is only true if "alice" came
from the authenticated identity. Get this wrong and the audit trail is fiction. It works with
*any* auth middleware — first-party or a customer's — because civitas only ever reads
`request.auth["principal"]`, never interprets *how* the middleware decided it.

### D3 — Safe zero-config default for the single dev

No auth middleware configured → write routes still work, and the principal defaults to a documented
honest value: **`{"id": "unauthenticated"}`** (RESOLVED — chosen over `"local"`/`"anonymous"` as
the most honest label in an audit log). The **network boundary is the security control**: the
introspection gateway already default-binds `127.0.0.1`. A single dev on localhost gets
suspend/resume with zero ceremony; the audit log honestly reads `approver: "local"`.

The one guardrail civitas adds: **a loud startup warning** (not a block — matching the gateway's
existing footgun-warning pattern, e.g. core.py's WS-mTLS warning) when **write routes are served on
a non-localhost bind with no auth middleware**. civitas refuses to be *silently* dangerous, but
does not impose policy on a deployment that genuinely wants an open control plane behind its own
network controls.

## 4. First slice: suspend / resume as POST routes

- `POST /agents/{name}/suspend` — body optional `{"reason": "...", "category": "..."}`; principal
  from `__principal__` recorded as the actor. Sends `_agency.suspend`.
- `POST /agents/{name}/resume` — principal from `__principal__` becomes the required `approver`.
  Sends `_agency.resume`. (Note: `resume` *requires* a non-empty approver — with the `"local"`
  default this is always satisfied; a customer middleware supplies the real one.)
- POST, never GET (GETs are logged/cached/prefetched/CSRF-prone; writes must be POST).
- Both are already idempotent-safe (redundant suspend/resume are safe no-ops).
- Auto-registered like the read routes (v0.9.5 D2), but POST and **not** `raw_response` (they
  return a small JSON ack). They carry whatever `topology_middleware` the node configures — a
  customer puts their auth middleware there; a single dev configures none.

## 5. Open details — RESOLVED (2026-07-31 review)

1. **Default principal** — `{"id": "unauthenticated"}` (chosen over `"local"`/`"anonymous"`).
2. **`request.auth["principal"]` shape** — a **dict** (`{"id": ..., ...}`), for scalability;
   civitas reads only `"id"`, carries the rest through untouched.
3. **Suspend's actor field in the audit `details`** — **`initiated_by`** (resume keeps `approver`).
4. **Warning scope** — scoped to the topology write routes for this slice; a general
   "unauthenticated write route" detector is a possible later addition, not built now.

## 6. Explicitly NOT in scope

- Any AuthZ engine, role/scope model, SCIM connector, or IdP integration inside civitas.
- Per-route auth *tiering* logic in civitas (it's the customer middleware's job; the route table
  already lets them attach different middleware per route).
- kill / force-restart, mailbox introspection/inject (later v0.9.6 items, built on this same
  principal→audit binding once suspend/resume proves it).
- kill / force-restart, mailbox introspection/inject (later v0.9.6 items).

## 7. D7 — dashboard client sends auth headers (DONE, v0.9.6 slice 2)

The server-side seam made endpoints *able* to require auth; D7 makes civitas's own TUI/CLI able to
*attach* to one. `fetch_json()` gained a `headers` param; `ClusterTarget`/`ClusterView`/
`CivitasDashboardApp` thread per-cluster headers through every poll; `civitas dashboard` and
`civitas topology show` gained a repeatable `--header 'Name: Value'` flag (scheme-agnostic —
`Authorization: Bearer ...`, `X-API-Key: ...`, or any custom header the operator's middleware
expects). Verified end-to-end: `fetch_json` against an API-key-protected endpoint returns 401
without the header, 200 with it.
