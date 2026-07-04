# Design: HTTPGateway

**Status:** Implemented — v0.4
**Author:** Jeryn Mathew Varghese
**Last updated:** 2026-04

---

## Motivation

Civitas agents communicate exclusively through the message bus. External clients — mobile apps, browsers, microservices, CLI tools — speak HTTP, HTTP/2, HTTP/3, or gRPC. Without a bridge, every integration requires custom glue code outside the supervision tree.

`HTTPGateway` is a supervised `AgentProcess` (or `GenServer`) that binds to a port, translates inbound protocol traffic into Civitas messages, routes them to the appropriate agent or GenServer, and returns the reply as a protocol response. The agent handling the request has no knowledge of HTTP — it only sees `call()` / `cast()` messages, exactly as if another agent sent them.

**What this is not:** a full application framework. `HTTPGateway` is a thin, protocol-aware edge process in the supervision tree. Business logic lives in agents and GenServers behind it.

---

## Protocol support matrix

| Protocol | Transport | Multiplexing | TLS | Civitas mapping |
|----------|-----------|-------------|-----|-----------------|
| HTTP/1.1 | TCP | No (keep-alive only) | Optional | `call` / `cast` |
| HTTP/2 | TCP | Yes (streams) | Required (ALPN) | `call` / `cast` / server-push |
| HTTP/3 | QUIC (UDP) | Yes (streams, 0-RTT) | Built-in | `call` / `cast` |
| gRPC | HTTP/2 | Yes | Optional (insecure mode) | Unary → `call`, streaming → `cast` chain |

All four share the same routing and message-translation layer. Only the network I/O layer differs.

---

## Architecture

![HTTP Gateway Architecture](../assets/http-gateway-arch.svg)

`HTTPGateway` is started as a child of any `Supervisor`. It owns the three server coroutines internally and manages their lifecycle within its `on_start` / `on_stop` hooks.

---

## Request-to-message mapping

### URL routing convention

```
POST   /agents/{name}         →  call(name, body)       # synchronous, returns reply
POST   /agents/{name}/cast    →  cast(name, body)        # fire-and-forget, returns 202
GET    /agents/{name}/state   →  call(name, {__op__: state})
POST   /broadcast             →  broadcast(body)
```

Custom route tables are supported via topology YAML (see below).

### HTTP → Message translation

| HTTP concept | Civitas Message field |
|-------------|----------------------|
| URL path segment `{name}` | `recipient` |
| Request body (JSON) | `payload` |
| `X-Civitas-Type` header | `type` (defaults to `"http.request"`) |
| `X-Correlation-ID` header | `correlation_id` |
| `traceparent` header (W3C) | `trace_id`, `parent_span_id` |
| HTTP method | determines `call` vs `cast` (POST=call, PUT=cast) |

### Message → HTTP response translation

| Message reply field | HTTP concept |
|--------------------|--------------|
| `payload` (dict) | JSON response body |
| `payload.error` present | HTTP 400 |
| No handler registered | HTTP 404 |
| Timeout | HTTP 504 |
| Unhandled exception | HTTP 500 |

---

## HTTP/1.1 and HTTP/2

**Server:** [uvicorn](https://www.uvicorn.org/) with the `[standard]` extra — ASGI server built on two C-backed components:

- **[uvloop](https://github.com/MagicStack/uvloop)** — drop-in replacement for the asyncio event loop, wrapping [libuv](https://libuv.org/) (the C event loop powering Node.js). Delivers 2–4× throughput over the default asyncio loop.
- **[httptools](https://github.com/MagicStack/httptools)** — HTTP/1.1 parser built on [llhttp](https://github.com/nodejs/llhttp) (C). Used by uvicorn for HTTP/1.1 request parsing.

HTTP/2 is negotiated via ALPN on TLS connections — uvicorn handles this transparently when TLS is configured.

!!! note "Windows compatibility"
    uvloop uses libuv APIs that are not available on Windows. The gateway installs uvloop on Linux/macOS and silently falls back to the default asyncio event loop on Windows:

    ```python
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass  # default asyncio event loop (Windows or uvloop not installed)
    ```

**Interface:** Standard ASGI app. `HTTPGateway` implements `__call__(scope, receive, send)` internally, run by uvicorn.

```python
from civitas.gateway import HTTPGateway

gateway = HTTPGateway(
    name="api",
    host="0.0.0.0",
    port=8080,
    tls_cert="certs/server.crt",   # enables HTTP/2 via ALPN
    tls_key="certs/server.key",
    request_timeout=30.0,
)
```

**HTTP/2 specifics:**
- Stream multiplexing is handled by uvicorn — each stream maps to one Civitas `call()`
- Server push is not exposed in v1 (deferred — requires explicit push directives from agents)
- `pip install civitas[http]` installs `uvicorn[standard]` (pulls in uvloop + httptools automatically)

---

## HTTP/3 / QUIC

**Server:** [aioquic](https://github.com/aioquic/aioquic) — pure-Python QUIC and HTTP/3 implementation.

HTTP/3 runs over QUIC — a multiplexed, encrypted transport over UDP. Key benefits over HTTP/2:
- **0-RTT connection resumption** — repeat clients connect with zero round-trip handshake
- **Head-of-line blocking eliminated** — lost UDP packets don't stall other streams
- **Connection migration** — clients can change IP (mobile handoff) without reconnecting

**QUIC requires TLS** — there is no plaintext QUIC. Certificate required.

```python
gateway = HTTPGateway(
    name="api",
    host="0.0.0.0",
    port=8080,
    port_quic=8443,                 # separate UDP port for QUIC
    tls_cert="certs/server.crt",
    tls_key="certs/server.key",
    enable_http3=True,              # adds Alt-Svc header to HTTP/1.1 and HTTP/2 responses
)
```

The `Alt-Svc: h3=":8443"` header is automatically injected into HTTP/1.1 and HTTP/2 responses, advertising the QUIC endpoint to clients that support it.

**Required extra:** `pip install civitas[http3]` (installs `aioquic>=1.0`)

### Why aioquic (and what's next)

aioquic is the established pure-Python QUIC library, is adequate for most deployments, and is the backend today. A Rust-backed QUIC stack (e.g. Cloudflare's [quiche](https://github.com/cloudflare/quiche)) would offer near-native performance, but quiche ships **no official, production-ready Python binding** — the code under its repo is example/demo material, not a published package. A Rust QUIC swap therefore remains a *future evaluation* (roadmap G9), not a committed upgrade path. Because `HTTPGateway` isolates the transport behind its own driver, such a swap would require no public API change if a viable binding later emerges.

---

## gRPC

gRPC uses HTTP/2 as transport and Protocol Buffers (protobuf) as the serialization format. Civitas exposes a **generic reflection service** so clients don't need pre-generated stubs for basic use.

### Default: grpclib (pure Python async)

**Server:** [grpclib](https://github.com/vmagamedov/grpclib) — pure Python async gRPC, no C extension required. Integrates cleanly with asyncio and requires no native build tooling.

**Install:** `pip install civitas[grpc]` (installs `grpclib>=0.4` + `protobuf>=4`)

### Production: grpcio (C core)

For high-throughput deployments, `grpcio` (Google's official Python gRPC library) uses a C core for serialization, compression, and connection management — significantly faster under load.

**Install:** `pip install civitas[grpc-fast]` (installs `grpcio>=1.62` + `protobuf>=4`)

When `grpcio` is present, `HTTPGateway` automatically uses it. When only `grpclib` is present, it falls back gracefully. Both expose the same `HTTPGateway` API.

```python
# Same API regardless of grpclib vs grpcio backend
gateway = HTTPGateway(
    name="api",
    host="0.0.0.0",
    port=8080,
    grpc_port=50051,
)
```

### Generic service (no proto required)

```protobuf
// Automatically served by HTTPGateway
service CivitasGateway {
    rpc Call(Request) returns (Response);         // → call()
    rpc Cast(Request) returns (google.protobuf.Empty); // → cast()
    rpc Stream(Request) returns (stream Response); // → call() + stream reply chunks
}

message Request {
    string recipient = 1;
    string type = 2;
    bytes  payload_json = 3;    // JSON-encoded payload
    string correlation_id = 4;
    string trace_parent = 5;
}

message Response {
    bytes  payload_json = 1;
    string error = 2;
}
```

### RPC → Civitas mapping

| gRPC RPC type | Civitas operation | Notes |
|--------------|------------------|-------|
| Unary | `call()` | One request, one reply |
| Client streaming | Accumulate → `call()` | All client frames collected, then one call |
| Server streaming | `call()` → stream chunks | Agent returns `{"chunks": [...]}` |
| Bidirectional streaming | `cast()` per frame | No correlation; fire-and-forget per message |

### Custom proto stubs (optional)

For typed APIs, agents can expose a `.proto` definition. The gateway loads it at startup and maps each RPC method to a Civitas agent name:

```yaml
# topology.yaml
gateway:
  grpc:
    proto_dir: protos/
    services:
      - proto: protos/assistant.proto
        service: AssistantService
        agent: assistant          # routes all RPCs to this agent name
```

---

## Topology YAML

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - name: api
      type: http_gateway
      module: civitas.gateway
      class: HTTPGateway
      config:
        host: "0.0.0.0"
        port: 8080
        port_quic: 8443
        enable_http3: true
        tls_cert: !ENV GATEWAY_TLS_CERT
        tls_key: !ENV GATEWAY_TLS_KEY
        request_timeout: 30.0
        grpc:
          enabled: true
          proto_dir: protos/
        routes:
          - path: /v1/chat
            agent: assistant
            method: POST
            mode: call
          - path: /v1/notify
            agent: notifier
            method: POST
            mode: cast

    - name: assistant
      type: agent
      module: myapp.agents
      class: AssistantAgent

    - name: notifier
      type: gen_server
      module: myapp.services
      class: NotifierService
```

---

## Implementation plan

### Phase 1 — HTTP/1.1 + HTTP/2 (v0.4)

1. `civitas/gateway/__init__.py` — `HTTPGateway(AgentProcess)`
2. `civitas/gateway/asgi.py` — ASGI app, request translation, response mapping
3. `civitas/gateway/router.py` — route table (path → agent name, mode)
4. uvicorn runner inside `on_start()` / `on_stop()` with uvloop install (Linux/macOS only)
5. TLS config from topology YAML / env vars
6. `civitas[http]` extra: `uvicorn[standard]>=0.30` (pulls in uvloop + httptools)

### Phase 2 — HTTP/3 / QUIC (v0.4)

1. Add `port_quic` config option to `HTTPGateway`
2. Run aioquic QUIC server alongside uvicorn on the UDP port
3. Auto-inject `Alt-Svc` header into HTTP/1.1 and HTTP/2 responses
4. `civitas[http3]` extra: `aioquic>=1.0`
5. Document quiche-python upgrade path for when Rust bindings stabilise

### Phase 3 — gRPC (v0.5)

1. `civitas/gateway/grpc.py` — generic `CivitasGateway` service; detect grpcio vs grpclib
2. `.proto` file bundled with the package
3. Optional custom proto loading from `proto_dir`
4. RPC → message type mapping (unary, client-streaming, server-streaming, bidirectional)
5. `civitas[grpc]` extra: `grpclib>=0.4`, `protobuf>=4`
6. `civitas[grpc-fast]` extra: `grpcio>=1.62`, `protobuf>=4`

### Phase 4 — Advanced (v0.5+)

- gRPC reflection service (clients introspect available services without .proto files)
- HTTP/2 server push
- WebSocket upgrade (maps to long-lived `cast()` sessions)
- Rate limiting via `RateLimiter` GenServer (built-in integration)
- Request authentication middleware (JWT, API key, mTLS client certs)
- Evaluate quiche-python (Rust QUIC) as drop-in replacement for aioquic

---

## What HTTPGateway does NOT do

- **No business logic** — it translates and routes. All logic lives in agents/GenServers.
- **No load balancing** — route to one agent by name. Load balancing is a supervisor/registry concern.
- **No request queuing** — requests that exceed `request_timeout` return 504. Queuing is the agent's responsibility.
- **No schema validation** — payloads are forwarded as-is. Validation belongs in `handle_call()`.

---

## Dependencies summary

| Extra | Installs | Enables | Backend |
|-------|----------|---------|---------|
| `civitas[http]` | `uvicorn[standard]>=0.30` | HTTP/1.1 + HTTP/2 | C (uvloop + httptools) |
| `civitas[http3]` | `aioquic>=1.0` | HTTP/3 / QUIC | Pure Python (aioquic; Rust QUIC swap under evaluation — G9) |
| `civitas[grpc]` | `grpclib>=0.4`, `protobuf>=4` | gRPC | Pure Python async |
| `civitas[grpc-fast]` | `grpcio>=1.62`, `protobuf>=4` | gRPC (high throughput) | C core (Google) |

All can be installed together: `pip install civitas[http,http3,grpc]` or `pip install civitas[http,http3,grpc-fast]`

---

## Open questions

| # | Question | Notes |
|---|----------|-------|
| Q1 | Should `HTTPGateway` be a `GenServer` subclass or `AgentProcess`? | Lean `AgentProcess` — it uses `self.call()` / `self.cast()` on the bus, not `handle_call` itself |
| Q2 | How should streaming gRPC map to the bus? | `cast()` per frame is simplest; bidirectional may need a dedicated session agent |
| Q3 | Should the generic gRPC service use JSON or msgpack for `payload`? | JSON — maximises client interoperability without requiring Civitas SDK |
| Q4 | TLS termination — in-process or via sidecar (Envoy, nginx)? | Both; in-process for simplicity, sidecar for production mTLS |
| Q5 | How do WebSocket upgrades map to the bus? | Long-lived session — each WebSocket frame → `cast()`; close event → `call()` teardown |
| Q6 | Should route tables support middleware chains (auth, rate-limit, logging)? | Yes, via pluggable middleware list in topology YAML — spec separately |
| Q7 | When should `civitas[grpc-fast]` become the default? | When grpcio's asyncio support matures fully — currently requires careful loop management |

---

## Acceptance criteria

- [ ] `HTTPGateway` starts as a child of any `Supervisor`
- [ ] HTTP/1.1 requests route correctly to named agents via `call()` and `cast()`
- [ ] HTTP/2 multiplexed streams each map to independent Civitas calls
- [ ] HTTP/3 QUIC endpoint advertised via `Alt-Svc` header
- [ ] gRPC unary RPC maps to `call()` and returns correct protobuf response
- [ ] `request_timeout` returns HTTP 504 / gRPC DEADLINE_EXCEEDED
- [ ] TLS config loaded from `settings` / topology YAML / env vars
- [ ] `HTTPGateway` stops cleanly on `Supervisor` shutdown — drains in-flight requests
- [ ] Topology YAML `type: http_gateway` supported by `civitas topology validate`
- [ ] `civitas topology show` displays gateway nodes with protocol annotations
- [ ] uvloop installed and active on Linux/macOS; asyncio fallback on Windows
- [ ] grpcio detected and preferred over grpclib when `civitas[grpc-fast]` is installed
- [ ] ≥ 20 unit tests (routing, translation, timeout, error mapping)
- [ ] ≥ 5 integration tests (real HTTP client → gateway → agent → response)
- [ ] Documented with examples for all four protocols
