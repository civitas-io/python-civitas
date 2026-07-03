# Design: gRPC Gateway (v0.6.0 / G1)

**Status:** APPROVED 2026-07-03 — implementing. D1 resolved to **Option A (grpc.aio default)**.
**Author:** Sisyphus
**Last updated:** 2026-07-03

> **Guiding principle (maintainer, 2026-07-03):** the pure-Python ethos is a general preference,
> not a hard rule — it must not block performance or delivery. C/Rust-backed dependencies (grpcio's
> C-core, and later quiche's Rust QUIC) are sensible, proven choices and are accepted where they are
> the better engineering call. This resolves D1 in favour of the maintained, faster default.

Supersedes the gRPC section of [`http-gateway.md`](http-gateway.md) (written pre-v0.4, before the
gateway's ASGI/middleware layer shipped). That doc's HTTP/1.1/2/3 content stands; its gRPC
"Phase 3" sketch is refreshed here against the shipped gateway and current library facts.

---

## Motivation

The HTTP gateway (v0.4) bridges HTTP/1.1, HTTP/2, and HTTP/3 clients onto the message bus. gRPC is
the remaining first-class RPC surface external clients expect — typed, HTTP/2-multiplexed, with
first-class streaming and tooling (`grpcurl`, reflection). G1 adds a **generic** gRPC surface: one
service that proxies any agent by name, so callers need no per-agent `.proto` or civitas SDK.

Same principle as the rest of the gateway: a thin translate-and-route edge. The agent behind it
only ever sees a `Message`; it has no idea the caller spoke gRPC.

---

## Reconciliation with the shipped gateway (what actually exists in v0.5)

The pre-v0.4 design predates the real code. Grounding the gRPC design in what shipped:

- `HTTPGateway(AgentProcess)` + `GatewayConfig` (dataclass) — `core.py`. `on_start()` launches
  `uvicorn.Server.serve()` as an asyncio task and, if `enable_http3`, an `H3Server` (aioquic) too;
  `on_stop()` tears both down. **gRPC slots in here as a third server task** — the coexistence
  pattern the librarian confirmed (multiple asyncio servers, one loop, different ports).
- `GatewayASGI` (`asgi.py`) owns HTTP→Message translation. The reusable core is `_call_or_cast()`
  (resolve agent → `self._gateway.ask()`/`.send()` → map `MessageRoutingError`→404,
  `TimeoutError`→504, generic→500). **This must be extracted** (see D3) so gRPC and HTTP produce
  identical routing/error semantics instead of two drifting copies.
- `GatewayConfig` has no gRPC fields yet — D4 adds them.
- The gateway drives the bus via `self.ask()` / `self.send()` (it's an `AgentProcess`); `handle()`
  is a no-op. gRPC handlers do the same.

---

## Design Decisions (PROPOSED — for review)

### D1 — Default backend: **grpc.aio (grpcio)** ✅ RESOLVED (Option A)

**Decision: `civitas[grpc]` = grpcio (grpc.aio) is the default; `civitas[grpc-pure]` = grpclib is
the optional pure-Python alternative.** The C-extension is accepted per the guiding principle above
(purity does not outrank a maintained, faster default). This **reverses** the pre-v0.4 doc (which
made grpclib the default). Librarian findings (2026):

- **grpclib is in maintenance mode** — the author stated (Jul 2024) it is "not actively developed…
  in maintenance mode unless there will be contributions." Still gets Python-version bumps
  (v0.4.9, Dec 2025) but no active development.
- **grpc.aio is Google-maintained**, the recommended path for new async services, and has ~30%
  higher throughput on sustained streaming (our G3 use case). grpclib wins single-call latency and
  uses less memory.

**The tension (why this needs your call):** civitas deliberately favours pure-Python, no-C-build
dependencies — that's exactly why **aioquic** (pure Python) is the HTTP/3 default. grpcio ships a
C-core extension (prebuilt wheels exist for common platforms, but not the "pip-installable
anywhere, no toolchain" story grpclib gives). So:

| Option | Default extra | Optional extra | Trades |
|---|---|---|---|
| **A (librarian rec)** | `civitas[grpc]` = grpcio (grpc.aio) | `civitas[grpc-pure]` = grpclib | Maintained + faster streaming; C extension |
| **B (ethos-consistent)** | `civitas[grpc]` = grpclib | `civitas[grpc-fast]` = grpcio | Pure-Python default like aioquic; but default is a maintenance-mode lib |

The abstraction below (D2/D3) is backend-agnostic, so this choice only affects the two extras and a
thin backend adapter — it is reversible later at low cost. **My lean: Option A** (a maintained,
Google-backed default outweighs the C-extension purity for a protocol most users opt into
explicitly), but flagging because it contradicts the shipped ethos and the existing doc. **Need your
decision.**

### D2 — Generic service via a minimal bundled `.proto` (Struct payload), not per-agent codegen

One tiny `.proto` bundled with the package, generated to stubs once at build/ship time:

```protobuf
syntax = "proto3";
package civitas;
import "google/protobuf/struct.proto";

service Agent {
  rpc Invoke (AgentRequest) returns (AgentReply);          // → call()  (unary, G1)
  rpc Cast   (AgentRequest) returns (google.protobuf.Empty); // → cast() (unary, G1)
  rpc Stream (AgentRequest) returns (stream AgentReply);    // → streaming (G3, see below)
}

message AgentRequest {
  string recipient = 1;                 // agent name
  string type = 2;                      // message type (default "grpc.request")
  google.protobuf.Struct payload = 3;   // JSON-ish; interop without a civitas SDK
  string correlation_id = 4;
  string traceparent = 5;               // W3C trace context
}

message AgentReply {
  google.protobuf.Struct payload = 1;
  string error = 2;                     // set when the agent replied with an error payload
}
```

Why a `.proto` (vs. pure `add_generic_rpc_handlers` with raw bytes): it **enables server reflection
and `grpcurl`** for free (D5), gives `Struct`↔`dict` conversion out of the box (aligns with the
existing "JSON payload for interop" decision, http-gateway Q3), and is still fully generic (the
caller names the agent — no per-agent proto). Per-agent typed `.proto` loading from `proto_dir`
remains a **later** feature (G-series backlog), not G1.

### D3 — Extract a transport-agnostic `GatewayDispatcher` shared by HTTP and gRPC

Pull the agent-resolution + `ask`/`send` + error-mapping core out of `GatewayASGI._call_or_cast()`
into a `GatewayDispatcher` (or module-level functions) taking a normalized request
(recipient, type, payload dict, mode, correlation_id, trace_id) and returning a normalized result
(payload dict | error + a status enum). Both surfaces map their protocol to/from this. This
guarantees a gRPC `Invoke` and an HTTP `POST /agents/{name}` hit the bus identically and map the
same failures the same way — no drift. HTTP keeps its extra concerns (routes, contracts,
middleware) on top; gRPC uses the dispatcher directly.

### D4 — Config + lifecycle

`GatewayConfig` gains:

```python
grpc_enabled: bool = False
grpc_port: int | None = None          # e.g. 50051; required when grpc_enabled
grpc_reflection: bool = True          # serve reflection for grpcurl/tooling
# grpc TLS reuses the existing tls_cert / tls_key; insecure when unset
```

`on_start()` launches the gRPC server as a task alongside uvicorn/H3; `on_stop()` calls the
backend's graceful stop (grace period) then cancels, mirroring the existing uvicorn/H3 teardown.
`civitas topology validate`/`show` learn the `grpc:` block and a `[grpc]` label.

### D5 — Reflection + health

Enable server reflection (backend-specific one-liner over the bundled descriptors) when
`grpc_reflection` is true, so `grpcurl -plaintext HOST list` / `describe` / `call` work with zero
client stubs. Optionally register the standard gRPC health service (cheap; nice for load balancers).

### D6 — Error mapping (gRPC status codes)

| civitas outcome | gRPC status |
|---|---|
| no agent registered (`MessageRoutingError`) | `NOT_FOUND` |
| `request_timeout` exceeded (`TimeoutError`) | `DEADLINE_EXCEEDED` |
| reply payload has `error` | `INVALID_ARGUMENT` (or `ABORTED`) + message in trailer |
| unhandled exception | `INTERNAL` |
| agent suspended (future, when bus exposes it) | `UNAVAILABLE` |

Mirrors the HTTP 404/504/400/500 mapping via the shared dispatcher (D3), just different codes.

---

## v0.6.0 scope for G1 (and what defers)

**In G1:** generic `Agent` service with **`Invoke` (unary → `call`)** and **`Cast` (unary →
`cast`)**, the bundled `.proto` + stubs, reflection, config + lifecycle, shared dispatcher refactor,
error mapping, `civitas[grpc]` (+ the optional extra per D1), tests, and docs.

**Deferred to G3 (streaming) — same release, later step:** the **`Stream` (server-streaming) RPC**.
It shares the async-generator↔bus-subscription bridge with G2 (WebSocket) and G3 (SSE), so it's
cleaner to build all three streaming surfaces together on one streaming core than to bolt a
one-off streaming path onto G1. `client-streaming` / full `bidi` stay out of scope for v0.6.0
(no compelling agent-facing semantics yet; `bidi`→`cast()`-per-frame can come later).

---

## Open questions — RESOLVED (2026-07-03)

- **Q1 — library default.** ✅ **Option A** — grpc.aio default (D1).
- **Q2 — one backend or both.** ✅ **Ship grpc.aio only in G1**; add the `grpc-pure` (grpclib) extra
  + adapter in a follow-up. The `GatewayDispatcher` is backend-agnostic so the alternate slots in
  later without churn.
- **Q3 — `.proto` codegen.** ✅ **Commit the generated `_pb2` stubs** (small, stable; no build-time
  protoc dependency for consumers).
- **Q4 — health service.** ✅ **Include** the standard gRPC health service (tiny, LB-friendly).

---

## Non-Goals (v0.6.0)

- Per-agent typed `.proto` loading from `proto_dir` (generic service only in v0.6.0).
- Client-streaming and bidirectional streaming (server-streaming only, in G3).
- gRPC-Web / grpc-gateway HTTP transcoding (separate concern).
- Business logic, load balancing, request queuing in the gateway (unchanged gateway non-goals).

---

## Implementation checklist (fill in after approval)

Not started — this doc is the deliverable for review. On approval + the D1/Q1 decision, the G1 order
is roughly: bundled `.proto` + committed stubs → `GatewayDispatcher` extraction (+ keep HTTP tests
green) → `civitas/gateway/grpc.py` (backend server, generic `Invoke`/`Cast` handlers, `Struct`↔dict,
error mapping) → reflection/health → `GatewayConfig` + `on_start`/`on_stop` wiring → topology
validate/show → extras in `pyproject.toml` → unit + integration tests (real `grpc`/`grpcurl` client
→ gateway → agent → reply) → CHANGELOG/milestones.
