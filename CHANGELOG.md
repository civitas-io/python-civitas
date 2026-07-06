# Changelog

> **Note:** This project was renamed from Agency to Civitas in April 2026.
> Historical entries below refer to the product as "Agency".

All notable changes to this project are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Security

- **Gateway auth now covers the WebSocket and gRPC surfaces** (#17) — JWT (`require_jwt`) and mTLS
  (`client_cert_mode`) auth configured on `HTTPGateway` is now **auto-inherited** by the WS and gRPC
  surfaces, closing a silent gap where those surfaces stayed unauthenticated regardless of the HTTP
  auth config. **This changes runtime behavior for existing deployments** that configure `require_jwt`
  / `client_cert_mode` *and* expose `ws_routes` or `grpc_enabled=True`: those surfaces go from
  silently-open to enforced.
  - **WebSocket (JWT only).** The bearer token is read from a pinned `civitas.bearer.<jwt>`
    `Sec-WebSocket-Protocol` subprotocol and verified **before** `websocket.accept`; a missing/invalid
    token closes the handshake with WS close code `4401`, and the negotiated subprotocol is echoed on a
    successful accept. WS mTLS remains a non-goal pending
    [#25](https://github.com/civitas-io/python-civitas/issues/25).
  - **gRPC (JWT + mTLS).** A `grpc.aio` server interceptor enforces the `authorization: Bearer`
    metadata JWT and, when `client_cert_mode="required"`, transport-level `require_client_auth=True`
    plus the existing `CIVITAS_GATEWAY_MTLS_ALLOWED_DNS` DN allowlist. The `Health` and
    `ServerReflection` services are exempt so probes and reflection clients keep working. Failures map
    to `UNAUTHENTICATED` (bad/missing JWT), `PERMISSION_DENIED` (unlisted DN), and `INTERNAL` (empty
    allowlist misconfig).
  - **New startup validations.** `client_cert_mode="optional"` is rejected when `grpc_enabled=True`
    (grpc.aio has no `CERT_OPTIONAL` equivalent — `ConfigurationError`); enforcing JWT over a plaintext
    (non-TLS) gRPC port is refused (`ConfigurationError`, since the token would travel in cleartext);
    and an mTLS-only gateway with `ws_routes` logs a startup warning that its WS routes are
    unauthenticated.
  See [`docs/design/gateway-ws-grpc-auth.md`](docs/design/gateway-ws-grpc-auth.md).

## [0.7.1] — 2026-07-05

### Added

- **Bus-native streaming** (v0.7.x · R7) — `AgentProcess.stream(recipient, payload, ...)` returns an
  async iterator over another agent's streamed chunks: the consumer counterpart to the existing
  `stream_reply()` / `emit()` producer API, working across in-process, ZMQ, and NATS with **no transport
  change**. Chunks are demultiplexed on the receive path (so consuming inside `handle()` never deadlocks
  and stream traffic bypasses the business mailbox). Adds a shared `civitas.streaming.StreamSink`, typed
  errors (`StreamError`, `SlowConsumerError`, `StreamInterrupted`, `StreamTimeout`), an optional
  `Message.seq` for ordering / gap detection, cooperative cancellation with a producer-side
  `max_frames` / `max_duration` cap, sender verification, and reserved `civitas.stream.*` message types.
  See [`docs/design/bus-native-streaming.md`](docs/design/bus-native-streaming.md).

## [0.7.0] — 2026-07-05

### Added

- **Per-agent spawn quotas** (v0.7.0 · R5) — `DynamicSupervisor` gains `max_children_per_spawner` and
  `max_total_spawns_per_spawner`, capping a single spawner's concurrent and lifetime children in addition
  to the supervisor-wide `max_children` / `max_total_spawns`. Quotas key on the spawner identity (each
  spawner has an independent budget; lifetime counts are never refunded), configurable per
  `dynamic_supervisor` topology node. Default `None` is unbounded per spawner.
- **Cross-process dynamic spawn** (v0.7.0 · R6) — `DynamicSupervisor` children can now be spawned in
  a *different* OS process over ZMQ/NATS, not just in the supervisor's own process. Cross-process spawn
  is simply `spawn_into(<supervisor hosted in a Worker>)`: a `type: dynamic_supervisor` topology node may
  carry `process: worker`, and the Worker hosts it with the full distributed `ComponentSet` so children it
  spawns are wired (`llm`/`tools`/`store`) and registered cluster-wide. No new public API and the
  in-process spawn path is unchanged (no announcement on `InProcessTransport`). Homogeneous deployments
  only — the spawned class must be importable on the Worker (no code distribution). Hardened per Oracle
  review (D8–D14):
  - **Cluster-wide announcement, after-start, signed, epoched** — once a child reaches RUNNING (never at
    request time), the supervisor publishes a signed `_agency.register` carrying `{name, capabilities,
    capability_metadata, pubkey, epoch}`; on termination it publishes `_agency.deregister {name, epoch}`.
    A monotonic incarnation `epoch` stops a reordered late announcement from resurrecting a dead name.
  - **Authenticated receive + name ownership** — `Runtime._on_remote_register` verifies the announcement
    signature against the trusted key set (dropping unsigned/unknown-signer announcements when signing is
    on) and `LocalRegistry.register_remote`/`deregister_remote` reject a name owned locally, by a different
    remote owner, or with a conflicting public key (no last-writer-wins takeover), and ignore stale epochs.
    A verified child public key is recorded in the `KeyRegistry` so peers can verify the child's messages.
  - **Per-incarnation child identity** — with signing enabled each spawn mints a fresh `AgentIdentity` for
    the child on the Worker and announces its public key; a compromised Worker can never impersonate a
    child elsewhere (no shared per-name private key).
  - **Spawn idempotency + termination authority** — `spawn_into` carries a unique `spawn_id`; a retried
    request for the same `(name, spawn_id)` returns the existing child instead of double-spawning. The
    Worker-side supervisor owns termination; the original spawner treats a child `_agency.deregister` as
    authoritative termination alongside `civitas.dynamic.terminated`.
  - **Per-spawn audit** — the `dynamic.spawn` audit event gains `distributed` and `pubkey` fields for
    cross-process spawns (in-process events are unchanged).
  See [`docs/design/cross-process-spawn.md`](docs/design/cross-process-spawn.md).
- **Encrypted StateStore at rest** (v0.7.0 · R4) — new `EncryptingStateStore`
  (`civitas.EncryptingStateStore`, opt-in `pip install 'civitas[encryption]'`) wraps any
  `StateStore` and transparently encrypts persisted *values* with ChaCha20-Poly1305 (AEAD), leaving
  agent *names* in the clear so `list_agents()` keeps working. The agent name is bound as AEAD
  associated data, so an envelope written for one agent fails to decrypt under another. Values are
  stored as a one-key envelope dict `{"__civitas_enc__": base64(version‖key_id‖nonce‖ciphertext)}`
  (`__civitas_enc__` is a reserved top-level key). Key is a base64 32-byte value from
  `CIVITAS_STATE_KEY` (via `SecretStr`); a missing/short key fails loud with `ConfigurationError`
  and `cryptography` is lazily imported. Supports **key rotation** (a `{key_id: key}` ring decrypts
  any past key while new writes use the current one) and fail-loud tamper/wrong-key detection (new
  `StateDecryptionError`, referencing only the agent name + `key_id`, never contents). Legacy
  plaintext is **strict by default** (raises) with an opt-in `allow_plaintext_read` dual-read for
  gradual migration. Wire it in topology YAML with `plugins.state: {type: encrypted, config: {store:
  {type: sqlite, config: {...}}, allow_plaintext_read: false}}`. `StateStore` is now also exported
  from `civitas`. `civitas state list` renders encrypted values as `<encrypted>`. See
  [`docs/design/encrypted-statestore.md`](docs/design/encrypted-statestore.md).
- **Gateway JWT + mTLS auth** (v0.7.0 · R3) — two first-party, opt-in, secure-by-default gateway
  middleware alongside the existing API-key auth:
  - **JWT bearer verification** (`civitas.gateway.jwt_auth.require_jwt`, opt-in `pip install
    'civitas[jwt]'`) — verifies `Authorization: Bearer` tokens against a `JwtVerifier` built once,
    eagerly, at gateway `on_start` (a missing PyJWT or misconfiguration crashes startup, not the first
    request). Secure by default: explicit `algorithms` (default `["RS256"]`), `exp`/`iss`/`aud`
    **required** (a token without `exp` never expires otherwise) and verified, `alg=none` and RS/HS
    algorithm confusion rejected, `jku`/`x5c`/`kid` header keys never fetched, tokens size-capped
    (~8 KB), bounded leeway, and the blocking JWKS lookup offloaded via `asyncio.to_thread`. Supports
    a JWKS URL (`https://` only) or a static key (exactly one source). Config via `CIVITAS_JWT_JWKS_URL`
    / `CIVITAS_JWT_AUDIENCE` / `CIVITAS_JWT_ISSUER` / `CIVITAS_JWT_ALGORITHMS` /
    `CIVITAS_JWT_PUBLIC_KEY` / `CIVITAS_JWT_SECRET`.
  - **mTLS client-cert authorization** (`civitas.gateway.mtls.require_client_cert`) — authorizes on
    the client certificate's **full subject DN, exact match** (never a CN substring) against
    `CIVITAS_GATEWAY_MTLS_ALLOWED_DNS` (semicolon-separated). Fail-closed: no cert → 401, unlisted DN →
    403, unconfigured allowlist → 500. New `GatewayConfig.tls_ca_cert` / `client_cert_mode`
    (`none`/`optional`/`required`) wire uvicorn's `ssl_ca_certs` + `ssl_cert_reqs`; the CA **must** be
    a dedicated private CA. `GatewayRequest.client_cert` is populated at the ASGI edge from the TLS
    extension, and verified identity from either middleware is attached to the new
    `GatewayRequest.auth` (authN feeding authZ) rather than the dispatched payload.
  - **Security boundary (documented, not covered):** R3 auth applies to HTTP request routes only —
    WebSocket routes, the gRPC surface, and `/docs` + `/openapi.json` bypass the middleware chain.
    `/docs` now defaults to **off** whenever any gateway auth (API-key/JWT/mTLS middleware or
    `client_cert_mode != "none"`) is configured, unless `docs_enabled` is explicitly set. mTLS over
    HTTP/3 is refused at config time (aioquic cannot enforce client certs). WS/gRPC auth is a
    follow-up. See [`docs/design/gateway-auth.md`](docs/design/gateway-auth.md).

### Fixed

- **Gateway middleware-load failures are now fatal (fail-open auth bypass fixed)** (v0.7.0 · R3, M1) —
  the ASGI edge previously caught any error while loading a global or route-scoped middleware, logged
  it, and **continued without that middleware** — so a security middleware that failed to import or
  construct was silently dropped and the gateway served **unauthenticated**. Middleware is now resolved
  eagerly at startup and a load failure raises `ConfigurationError` out of the gateway's `on_start`,
  crashing the supervised gateway instead of serving requests with a missing auth layer.

- **Cross-tree spawn** (v0.7.0 · R2) — `AgentProcess.spawn_into(supervisor_name, agent_class, name,
  config=None, *, wait=True)` spawns a child into any *named* `DynamicSupervisor` in the tree, not just
  the nearest ancestor. `spawn()` is now the nearest-ancestor special case of `spawn_into()` and delegates
  to it, so R1 semantics (`wait` / cleanup / `on_child_terminated`) are inherited unchanged. It fails fast
  with `SpawnError` instead of hanging: an unknown target, a non-`DynamicSupervisor` target, or a
  self-target are rejected before sending, and a valid but unresponsive target (e.g. `SUSPENDED`) surfaces
  as `SpawnError` via a bounded reply timeout. Every `DynamicSupervisor` now carries a reserved
  `_agency.dynamic_supervisor` capability, injected at registration (surviving a YAML `capabilities:`
  override and propagated to nested dynamically-spawned supervisors) so it appears in registry /
  `topology show` introspection. See [`docs/design/spawn-into.md`](docs/design/spawn-into.md).
- **Cross-tree spawn authorization** (v0.7.0 · R2) — because a cross-tree child runs the *spawner's* chosen
  code with the *target* supervisor's `llm` / `tools` / `store` (a confused-deputy surface), R2 ships the
  controls to let the target say no: a new `DynamicSupervisor(..., spawner_allowlist: set[str] | None =
  None)` rejects spawners not in the set *before* the governance hook (default `None` keeps today's open
  behavior); a read-only `DynamicSupervisor.current_spawner` property lets an `on_spawn_requested` override
  authorize by spawner without a signature change (valid only during the hook); and every admitted spawn
  emits a `dynamic.spawn` audit event (`spawner`, `child`, `class_path`, `supervisor`) via the wired audit
  sink.
- **Non-blocking dynamic spawn** (v0.7.0 · R1) — `DynamicSupervisor` spawning no longer forces the
  spawner to block on the child's `on_start()`. `spawn(..., wait=False)` — and the `spawn_nowait()`
  alias on both `AgentProcess` and `Runtime` — return as soon as the child's task exists; the child
  initialises in parallel and messages buffered in the meantime are dispatched FIFO once it is ready.
  `wait=True` remains the default and preserves the previous semantics, including synchronous
  start-failure reporting. Start failures after a non-blocking spawn are delivered to the spawner via
  `on_child_terminated`. See [`docs/design/non-blocking-spawn.md`](docs/design/non-blocking-spawn.md).
- `MessageBus.teardown_agent()` and `Transport.unsubscribe()` — unwire a terminated agent and fail any
  pending `ask()` bound to it (so callers fail fast instead of hanging).

### Changed

- `DynamicSupervisor` `max_total_spawns` now counts **every spawn attempt at admission** (previously
  only successful spawns); a failed spawn is no longer refunded, giving stricter spawn-storm protection.
- On `on_start()` failure, `on_stop()` is now called **best-effort** (a throwing `on_stop()` is logged
  and no longer masks the original start error).

### Fixed

- **Name collision no longer crashes the target `DynamicSupervisor`** (v0.7.0 · R2, P0) — spawning a child
  whose name already exists anywhere in the global registry previously let the `ValueError` from
  `registry.register()` escalate and crash the supervisor. `DynamicSupervisor._handle_spawn` now rejects a
  duplicate name with a `SpawnError` reply via a global pre-check and, for the cross-supervisor race, a
  wrapped `register()` — the supervisor stays running. Latent before R2 (ancestor-only spawn made it hard
  to reach); `spawn_into` turned it into a one-line cross-tree denial-of-service, so it is fixed here.
- Dynamically-spawned agents that fail `on_start()` no longer **leak** their registry entry or transport
  subscription — the supervisor runs a unified, idempotent terminal cleanup.

## [0.6.1] — 2026-07-04

### Fixed

- **Dynamically-spawned agents now inherit audit + metrics wiring** — agents created at runtime via
  `self.spawn()` / `DynamicSupervisor` were wired with the bus, tracer, registry, model provider, tools,
  and state store, but not the audit sink or metrics sink. Their audit events were silently dropped and
  their activity was invisible to metrics collectors. `DynamicSupervisor._handle_spawn` now also propagates
  `_audit_sink` and `_metrics`, matching `ComponentSet.inject` for statically-wired agents.

## [0.6.0] — 2026-07-04

### Added

- **Gateway streaming** (v0.6.0 / G2 + G3) — long-lived and incremental client connections built on one
  shared streaming core:
  - **WebSocket** (`GatewayConfig.ws_routes`) — bidirectional sessions: each inbound frame is `cast()` to
    an agent and the agent streams messages back over the same socket. No new dependency (uses
    `uvicorn[standard]`'s bundled `websockets`).
  - **Server-Sent Events / true streaming** — a route with `mode: "stream"` streams an agent's output
    incrementally as `text/event-stream`, replacing the old `{"chunks": [...]}` buffer-and-serialise
    workaround. Works over HTTP/1.1, HTTP/2, and HTTP/3.
  - **gRPC server-streaming** — the `Agent.Stream` RPC deferred in G1 is now implemented on the same core.
  - New agent API: `self.emit(chunk)` / `self.end_stream()` and the auto-terminating, error-safe
    `async with self.stream_reply() as stream:` context manager. Streams are bounded per-connection with a
    `slow_consumer` fail-fast plus idle/duration timeouts (`GatewayConfig.stream_queue_maxsize` /
    `stream_idle_timeout` / `max_stream_duration`).
- **Gateway middleware & uploads** (v0.6.0 / G4–G6) — first-party gateway building blocks:
  - **Rate limiting** (G4): a `RateLimiter` GenServer + `civitas.gateway.ratelimit.rate_limit`
    middleware (per-client sliding window; HTTP 429 + `Retry-After`).
  - **API-key auth** (G5): `civitas.gateway.auth.require_api_key` — fail-closed `X-API-Key` check
    against `CIVITAS_GATEWAY_API_KEY` (constant-time compare). JWT/mTLS remain integration points.
  - **File uploads** (G6): `multipart/form-data` is parsed at the ASGI edge; uploaded files reach the
    agent base64-encoded under `payload["__files__"]`, keeping payloads primitives-only.
- **Accelerated JSON serialization** (`civitas[fast]`) — installs Rust-backed `orjson`, which
  `JsonSerializer` uses automatically for a large encode/decode speedup, transparently falling back to
  the standard-library `json` module when it isn't installed. The wire format stays plain JSON, so the
  two backends interoperate. The `Message` payload primitives-only validation gate deliberately keeps
  using stdlib `json` — orjson natively accepts `datetime`/`UUID`/dataclasses and would weaken that
  enforcement.
- **gRPC gateway** (`civitas[grpc]`) — a generic `grpc.aio` surface on the gateway (v0.6.0 / G1). One
  `civitas.Agent` service proxies any agent by name, so callers need no per-agent `.proto` or civitas
  SDK: `Invoke` (unary → agent `call()`) and `Cast` (unary → `cast()`) carry a
  `google.protobuf.Struct` payload that maps to/from the JSON-ish dict a `Message` holds. Enabled with
  `GatewayConfig(grpc_enabled=True, grpc_port=…, grpc_reflection=True)` alongside the existing
  HTTP/1.1/2/3 surfaces; every transport shares one `GatewayDispatcher` so routing and error semantics
  stay identical. Ships the standard gRPC health service and optional server reflection (for
  `grpcurl`). Error mapping: no agent → `NOT_FOUND`, timeout → `DEADLINE_EXCEEDED`, unhandled error →
  `INTERNAL`; an agent's own business error is returned in-band on `AgentReply.error` with the payload
  preserved (mirrors the HTTP 400-with-body behaviour). `Stream` (server-streaming) is defined in the
  `.proto` but returns `UNIMPLEMENTED` until G3. Generated `_pb2` stubs are committed (no build-time
  `protoc` needed by consumers).

### Fixed

- **`DynamicSupervisor` spawn now applies `config` to the spawned agent** ([#8]) — the `config` passed
  to `spawn()` was parsed and governance-checked but never reached the agent. It is now available as
  `self.config` (readable in `on_start()` and `handle()`). Also documented that `on_start()` runs
  **synchronously inside the spawn call** — so `await self.spawn(...)` blocks until it returns; keep it
  fast and do slow/background work in `handle()`, kicked off by a post-spawn message (`on_start()`
  docstring + `docs/design/dynamic-spawning.md`).

[#8]: https://github.com/civitas-io/python-civitas/issues/8

## [0.5.0] — 2026-07-03

### Added

- **Durable suspension** (`agent.suspend()` / `agent.resume()`) — the Presidium human-in-the-loop
  approval primitive. `ProcessStatus.SUSPENDED` is re-introduced fully-wired (it was removed as dead
  API in **F02-6**): every transition, mailbox behaviour, supervisor interaction, and persistence
  path is defined and tested. Suspension pauses message *dispatch* only (Python cannot snapshot a
  running `handle()` coroutine). Key semantics: `suspend()` is a non-blocking flag actioned at the
  message-loop boundary; while suspended only the priority queue is drained so business messages stay
  buffered in FIFO order with backpressure preserved; the durable marker rides inside `self.state`
  under `_civitas.suspended` (one atomic checkpoint, durable only with a persistent store); suspend is
  write-ahead (pause in-memory first, then persist — never falls back to RUNNING on persist failure);
  `resume()` requires a non-empty `approver`; on permanent removal (despawn / restarts exhausted) the
  marker is cleared to prevent zombie-suspension, while graceful shutdown and crash-restart keep it.
  External entry points `runtime.suspend(name, reason="")` / `runtime.resume(name, approver)` deliver
  priority `_agency.suspend` / `_agency.resume` control messages. Each suspend/resume emits a
  `civitas.agent.suspend` / `civitas.agent.resume` span and an `AuditEvent` (resume records the
  approver). `ask()` into a suspended agent times out (documented; fail-fast deferred).

### Security

- Bumped `msgpack` floor to `>=1.2.1` (was `>=1.1`, resolved to 1.1.2) to clear
  **GHSA-6v7p-g79w-8964** (SEGV / DoS when a streaming `Unpacker` is reused after an error).
  Civitas's `MsgpackSerializer` uses one-shot `msgpack.unpackb()`, not the reused-`Unpacker`
  pattern, so core was not directly exposed — but the fix clears the `pip-audit --strict` CI gate
  and hardens the untrusted-input path over ZMQ/NATS transports regardless.

### Fixed

- **FD-01/FD-03** — `MetricsCollector` (dashboard) had no working way to receive restart, message,
  or error events; `civitas/cli/dashboard.py` worked around it by monkey-patching
  `runtime._root_supervisor._handle_crash` directly. Added `civitas.observability.metrics.MetricsSink`
  protocol, injected via `ComponentSet`/`Runtime.set_metrics()`; `Supervisor.add_crash_callback()` +
  `Runtime.on_crash()` public hook replace the monkeypatch. `message_handled` and `agent_error` are
  wired from `AgentProcess._dispatch()`, `message_sent` from `send()`/`ask()`. `llm_call` remains
  unwired — no clean interception point exists for `ModelResponse` token/cost data without a larger
  redesign of how `self.llm` is wrapped; flagged as a known follow-up, not silently claimed done.
- **F01-3** — `Message.ttl` was declared and documented (`AGENTS.md`) but never enforced. Added the
  `ttl` field to `Message` and enforcement in `Mailbox.get()`: expired messages are discarded with a
  warning instead of being delivered.
- **F11-5** — `on_stop()` was not called when `on_start()` raised, leaking any resources partially
  acquired in `on_start()` and any open `civitas.agent.start` tracer span. `AgentProcess._start()`
  now runs the equivalent cleanup (close span with error, mark `CRASHED`, run `on_stop()` + MCP
  client disconnect) before re-raising the original exception.
- **FD-07/FD-09** — `plugins.exporters` in topology YAML was parsed but never wired to anything in
  `Runtime` or `Worker`; `Worker` in particular dropped exporters silently in `cli/run.py`'s
  `_run_worker()`. Fixed both together: `Tracer` now skips its direct `TracerProvider` path
  whenever a `SpanQueue` is supplied (previously both could run simultaneously with no
  coordination), and `build_component_set()` assembles a `SpanQueue` + `FanOutBackend` from
  configured exporters, with `Runtime`/`Worker` owning the `OTELAgent` background task's lifecycle.
  `plugins.exporters: [{type: console}]` now actually works. The existing `OTEL_EXPORTER_OTLP_ENDPOINT`
  auto-detect path is unchanged when no `plugins.exporters` are configured.

## [0.4.0] — 2026-07-03

### Fixed

- **[#6](https://github.com/civitas-io/python-civitas/issues/6)** — Route-scoped gateway
  middleware (`RouteEntry.middleware` / a route's `middleware:` list in topology YAML) was
  parsed but never invoked by `GatewayASGI`. The dispatch layer built its middleware chain once
  at construction time from global `config.middleware` only; a matched route's own `.middleware`
  was never read. `GatewayASGI` now matches the route before building the chain and appends the
  route's resolved middleware after global middleware, restoring the documented execution order:
  global → route-scoped → contract validation → bus dispatch. Route middleware is resolved once
  and cached per route.
- **[#7](https://github.com/civitas-io/python-civitas/issues/7)** — Added the missing
  `civitas/py.typed` marker file. The package declared the `"Typing :: Typed"` classifier and
  ran `mypy --strict` internally, but shipped no marker, causing downstream `mypy --strict`
  users to get `import-untyped` errors on every `civitas` import.

### Changed

- **BREAKING CHANGE:** Model provider plugins (`civitas.plugins.{anthropic,openai,gemini,mistral,litellm}`), `civitas.plugins.sqlite_store`, `civitas.plugins.fiddler`, and `civitas.plugins.otel` have moved to `civitas-contrib` (`civitas_contrib.plugins.*`). Update imports, e.g. `from civitas.plugins.anthropic import AnthropicProvider` → `from civitas_contrib.plugins.anthropic import AnthropicProvider` (`pip install civitas-contrib[anthropic]`). `civitas.plugins.model`, `civitas.plugins.tools`, and `civitas.plugins.state` (protocols + `InMemoryStateStore`) remain in core.
- **BREAKING CHANGE:** `MCPClient` and `MCPTool` have moved to `fabrica` (`fabrica.mcp.client`, `fabrica.mcp.tool` — `pip install fabrica[mcp]`); there is no `civitas[mcp]` extra. `civitas.mcp.types` (`MCPServerConfig`, `MCPToolSchema`, `MCPToolError`) remains in core with no `mcp` SDK dependency at import time. `AgentProcess.connect_mcp()` and the `mcp.servers` topology YAML block remain in core and lazily import `fabrica.mcp.client.MCPClient` at call time, raising a `ConfigurationError` with install instructions if `fabrica` is not installed. `CivitasMCPServer` (exposing an agent tree as an MCP server) was never implemented in core — it is a Fabrica-scope feature, not a civitas regression.

### Added

#### M4.1b — Dynamic Agent Spawning

- `DynamicSupervisor` — starts empty, `ONE_FOR_ONE` only, `max_children` + `max_total_spawns` limits
- `self.spawn(AgentClass, name, config)` — routes to the nearest ancestor `DynamicSupervisor`
- `self.despawn(name)` (hard stop) / `self.stop(name, drain, timeout)` (soft stop, awaitable)
- `on_spawn_requested()` governance veto hook on `DynamicSupervisor`; `on_child_terminated()` notification hook on the spawning agent
- `Runtime.spawn()` / `Runtime.despawn()` / `Runtime.stop_agent()` — external entry points; `SpawnError` added to the error hierarchy
- `TopologyServer` — supervised JSON HTTP endpoint (`/topology`, `/agents`, `/agents/{name}`, `/health`); `civitas topology show` uses it for live state, falling back to static YAML

#### M4.2 — Security Hardening

- `civitas/security/` — `AgentIdentity` (Ed25519 keypairs), `KeyRegistry`, `MessageSigner` (v=2 wire format), `NonceCache` (replay protection), `SignatureError`, `SigningSerializer`; `security:` YAML block; InProcess transport bypasses signing entirely
- ZMQ CURVE and NATS TLS + nkeys transport-level encryption/auth; `civitas security init` CLI to scaffold keys
- `civitas.secrets` — `SecretsProvider` protocol, env/file implementations, `${VAR_NAME}` substitution in topology YAML (raises `ConfigurationError` on unset vars), per-agent `credentials:` block
- Bubblewrap-based tool sandbox for MCP subprocess execution on Linux; `sandbox:` YAML block; refuses to start when `sandbox.enabled: true` and `bwrap` is unavailable
- `civitas.audit` — `AuditEvent`, `AuditSink` protocol, `JsonlFileSink` (batched fsync, SIGHUP rotation), `NullSink`, `SyslogSink`, `OtlpSink`; emission at `MessageBus.route()`, tool execution, sandbox violations, secret access

#### M4.4 — Capability-Aware Registry

- `RoutingEntry.capabilities` / `capability_metadata`; `LocalRegistry.find_by_capability()` / `find_by_capabilities(tags, match="any"|"all")`
- `AgentProcess.capabilities` / `capability_metadata` class-level declarations; `AgentProcess.send_capable(capability, payload)`
- `CapabilityNotFoundError`; YAML `capabilities:` / `capability_metadata:` overrides; distributed propagation via Worker announcements
- `RegistryListener` hook — async callbacks on every register/deregister (Presidium integration point); `add_listener()` / `remove_listener()`
- `RoutingEntry`, `RegistryListener`, `CapabilityNotFoundError` exported from `civitas` top-level package

#### Gateway API Surface

- `@route(method, path, mode=)` and `@contract(request=Model, response=Model)` decorators; YAML remains the sole runtime-authoritative source, decorators are documentation + `civitas topology validate` cross-checks
- Global + route-scoped middleware chain (`config.middleware`, `RouteEntry.middleware`); short-circuit by returning a `GatewayResponse` without calling `next_fn`
- Auto-generated OpenAPI 3.1 spec (`GET /openapi.json`), Swagger UI (`GET /docs`), ReDoc (`GET /redoc`); `docs.enabled: false` disables all three

#### Postgres StateStore + Migration

- `PostgresStateStore` — `asyncpg` backend, connection pool, `civitas_agent_state` JSONB table with upsert; `civitas[postgres]` extra
- `StateStore` protocol extended with `list_agents()` / `close()`; `@runtime_checkable` for `isinstance()` checks
- `civitas state migrate <src> <dst>` — dry-run by default, `--execute` to apply; `_parse_dsn()` supports `sqlite:`, `.db`/`.sqlite`, and `postgresql://`

#### M4.1 — HTTP Gateway

- `civitas.gateway.HTTPGateway` — supervised `AgentProcess` that translates HTTP ↔ Civitas messages; external clients never touch the bus directly
- `civitas.gateway.GatewayConfig` — dataclass covering host, port, TLS, HTTP/3 (QUIC), routes, middleware, OpenAPI docs, and request timeout
- `civitas.gateway.RouteTable` — ordered route matching table; path parameters extracted and merged into `message.payload`; YAML is the authoritative source
- `civitas.gateway.route` — `@route(method, path, mode=)` decorator to co-locate route metadata on agent methods; used by `civitas topology validate`, never read at runtime
- `civitas.gateway.contract` — `@contract(request=Model, response=Model)` decorator; wired to `RouteTable.merge_contracts_from()` for automatic 422/500 validation
- `civitas.gateway.GatewayRequest` / `GatewayResponse` — thin middleware types; middleware receives `(request, next_fn)` and can short-circuit or pass through
- Middleware chain: global middleware loaded from dotted import paths in `config.middleware`; per-route middleware supported in `RouteEntry`
- OpenAPI 3.1 spec auto-generated from route table; served at `GET /docs/openapi.json`; Swagger UI served at `GET /docs` (CDN-hosted, zero bundling)
- Default URL conventions: `POST /agents/{name}` → call, `POST /agents/{name}/cast` → cast, `GET /agents/{name}/state` → call with `{"__op__": "state"}`
- HTTP/3 / QUIC via `civitas[http3]` (`aioquic`): `H3Server` runs alongside uvicorn; `Alt-Svc` header injected automatically when `enable_http3: true`
- W3C `traceparent` header parsed and propagated to trace context; `X-Civitas-Type` header overrides the Civitas message type
- Topology YAML support: `type: http_gateway` node type wired into `Runtime._build_node()`; `civitas topology show` renders `[http]` prefix
- `examples/http_gateway.py` — end-to-end example with `EchoAgent` and Swagger UI
- `pyproject.toml` — `civitas[http]` (uvicorn + pydantic) and `civitas[http3]` (+ aioquic) optional extras

#### M4.3 — Codebase Security & Enterprise Posture

- `.github/workflows/security.yml` — three-job security CI workflow running on every PR and weekly:
  - SAST: Bandit (`-lll -iii`, HIGH+ severity) + Semgrep (`p/python`, `p/secrets`, `p/owasp-top-ten`); SARIF uploaded to GitHub Security tab
  - Dependency audit: `pip-audit --strict` against PyPI Advisory Database; fails on any fixable vulnerability
  - Secret scan: `gitleaks` on full git history (`fetch-depth: 0`)
- `.github/dependabot.yml` — weekly Dependabot scans for pip and GitHub Actions dependencies; dev-tools grouped to reduce PR noise
- `publish.yml` — CycloneDX SBOM generated (JSON + XML) on every release tag; attached as GitHub release assets
- `.pre-commit-config.yaml` — `gitleaks` pre-commit hook added; blocks secret commits before they reach the remote
- `SECURITY.md` — responsible disclosure policy: email contact, response SLAs (2 days ack, 14 days for CRITICAL/HIGH), 90-day coordinated disclosure window, CVE process, supported versions
- `docs/security/threat-model.md` — STRIDE analysis for all runtime components: `AgentProcess`, `Supervisor`, `MessageBus`, `ZMQTransport`, `NATSTransport`, `HTTPGateway`, `StateStore`, plugin system, `EvalAgent`; risk summary with 21 itemised threats
- `docs/security/architecture.md` — four-zone trust boundary model (runtime process → Worker processes → remote machines → external clients), transport security posture per level, credential handling patterns, planned M4.2 hardening roadmap
- `docs/security/enterprise-checklist.md` — tiered adoption checklist (Level 1–4 by deployment complexity) + compliance guidance for SOC 2, GDPR, and HIPAA

---

## [0.3.0] — 2026-04-22

### Added

#### M2.5 — EvalLoop

- `EvalAgent` — supervised process that monitors agent behaviour and sends correction signals; sits alongside regular agents in the supervision tree
- `EvalEvent` — observable event emitted by agents; schema aligned with OTEL GenAI Semantic Conventions for remote exporter compatibility
- `CorrectionSignal` — three severity levels: `nudge` (soft guidance), `redirect` (change course), `halt` (stop agent cleanly)
- `EvalExporter` protocol — interface for remote eval engine adapters (Arize, Fiddler, Langfuse, etc.); implementations in M2.6
- `AgentProcess.emit_eval(event_type, payload, eval_agent)` — emit an observable event; no-op when bus not wired (safe in tests)
- `AgentProcess.on_correction(message)` — override hook called on `civitas.eval.correction` signals (nudge / redirect)
- `civitas.eval.halt` message type — breaks target agent's message loop cleanly; `on_stop()` still runs
- Rate limiting on `EvalAgent`: sliding window per target agent (`max_corrections_per_window`, `window_seconds`); excess corrections dropped and logged
- `type: eval_agent` YAML shorthand in `Runtime.from_config()` with `max_corrections_per_window` and `window_seconds` config
- `[eval]` label in `print_tree()` / `civitas topology show` for EvalAgent nodes
- `EvalAgent`, `EvalEvent`, `CorrectionSignal`, `EvalExporter` exported from `civitas` top-level package

#### M3.5 — GenServer

- `GenServer` — OTP-style generic server process with `handle_call` (synchronous, reply required), `handle_cast` (fire-and-forget), and `handle_info` (timers, internal signals) dispatch
- `send_after(delay_ms, payload)` — schedules a `handle_info` message to self after a delay; pending tasks cancelled on stop
- `AgentProcess.call(name, payload)` — synchronous GenServer call (wraps `ask()`, returns payload dict)
- `AgentProcess.cast(name, payload)` — fire-and-forget GenServer cast
- `Runtime.call()` / `Runtime.cast()` — runtime-level GenServer messaging
- `GenServer` exported from `civitas` top-level package
- `type: gen_server` support in `Runtime.from_config()` YAML topology
- `[srv]` label in `print_tree()` / `civitas topology show` for GenServer nodes

#### M3.4 — MCP Integration

- `civitas[mcp]` optional extra (`pip install 'civitas[mcp]'`) — wraps `mcp>=1.0` SDK
- `MCPServerConfig` — config dataclass for stdio and SSE MCP server connections; validated at construction
- `MCPClient` — persistent-per-agent MCP session with `connect()`, `disconnect()`, `list_tools()`, `call_tool()`; `AsyncExitStack` manages transport + session lifecycle as a unit
- `MCPTool` — `ToolProvider` wrapping a single MCP tool; name follows `mcp://server_name/tool_name` URI scheme for direct lookup via `self.tools.get()`; emits `civitas.mcp.call` OTEL span
- `MCPToolError` — raised when an MCP tool call returns `isError=True`
- `AgentProcess.connect_mcp(config)` — connects to an MCP server and registers all its tools into `self.tools`; idempotent (disconnects and deregisters existing tools for the same server before reconnecting)
- `ToolRegistry.deregister_prefix(prefix)` — removes all tools whose name starts with a given prefix
- `mcp.servers` topology YAML key — declare MCP servers in the topology file; `Runtime.from_config()` parses configs and auto-connects all agents on `start()`
- MCP clients are closed gracefully in the `_message_loop` finally block alongside `on_stop()`

---

## [0.1.0] — 2026-04-06

Initial public release.

### Added

#### Core runtime

- `AgentProcess` — asyncio-based agent with bounded mailbox, lifecycle hooks (`on_start`, `handle`, `on_error`, `on_stop`), and injected dependencies (`self.llm`, `self.tools`, `self.store`, `self._tracer`)
- `Supervisor` — fault tolerance tree with three restart strategies: `ONE_FOR_ONE`, `ONE_FOR_ALL`, `REST_FOR_ONE`
- Configurable backoff policies: `CONSTANT`, `LINEAR`, `EXPONENTIAL` (with 25% jitter)
- Sliding-window restart rate limiting (`max_restarts` + `restart_window`)
- Escalation chain — supervisor that exceeds its restart limit escalates to its parent
- Heartbeat-based monitoring for agents running in remote Worker processes
- `Runtime` — assembles and manages the full supervision tree; 13-step deterministic startup sequence
- `Worker` — multi-process agent host; connects to the broker and announces agents via `_agency.register`
- `ComponentSet` — shared infrastructure wiring for both `Runtime` and `Worker`
- `Runtime.from_config()` — builds a complete runtime from a YAML topology file

#### Messaging

- `MessageBus` — name-based routing with Registry lookup and ephemeral reply address fallback
- `send()` — fire-and-forget delivery
- `ask()` — request-reply with configurable timeout and ephemeral reply routing
- `broadcast()` — glob-pattern delivery to multiple agents
- `reply()` — return a reply from `handle()` without knowing the caller's address
- Bounded mailboxes with `asyncio.QueueFull` backpressure
- System message namespace (`_agency.*`) reserved and validated at route time
- Trace context propagation across all message boundaries

#### Transport layer

- `InProcessTransport` — asyncio queues, zero extra dependencies, ~2–5 µs latency
- `ZMQTransport` — XSUB/XPUB proxy, multi-process on a single machine (`pip install civitas[zmq]`)
- `NATSTransport` — distributed multi-machine transport with optional JetStream durable subscriptions (`pip install civitas[nats]`)
- Uniform `Transport` protocol — swap transports with a one-line topology change, no agent code changes
- Remote agent registration/deregistration via `_agency.register` / `_agency.deregister` messages

#### Plugin system

- `ModelProvider` protocol — structural, no base class required
- `AnthropicProvider` — first-party Anthropic SDK integration with built-in token pricing (`pip install civitas[anthropic]`)
- `LiteLLMProvider` — 100+ models via LiteLLM (OpenAI, Gemini, Bedrock, Azure, etc.) (`pip install civitas[litellm]`)
- `ToolProvider` protocol and `ToolRegistry` — named tools with JSON schema, duplicate name detection
- `StateStore` protocol — `get` / `set` / `delete` by agent name
- `InMemoryStateStore` — default, in-process, survives supervisor restarts
- `SQLiteStateStore` — durable persistence, all I/O in thread executor (non-blocking)
- `ModelResponse` dataclass — content, model, token counts, cost, tool calls
- Plugin loading from YAML topology — entrypoint → built-in name → dotted import path resolution
- `PluginError` — fast-fail at `Runtime.start()` with actionable error messages and install hints

#### Observability

- Automatic OTEL spans for every message send/receive, agent lifecycle event, LLM call, tool invocation, and supervisor restart
- `SpanQueue` — non-blocking span emission from the message loop (`put_nowait`, drops oldest if full)
- Three output modes: built-in `logging.DEBUG` console output (no deps) → OTEL `ConsoleSpanExporter` → OTLP gRPC export (Jaeger, Grafana Tempo, Datadog, etc.)
- `llm_span()` and `tool_span()` context managers for custom instrumentation
- Full span attribute reference under `civitas.*`, `llm.*`, `tool.*` namespaces
- Trace context propagation across process and machine boundaries
- `FanOutBackend` — export to multiple backends simultaneously
- Per-agent LLM cost attribution via `llm.cost_usd` span attribute

#### Framework adapters

- `LangGraphAgent` — wraps a LangGraph `CompiledGraph` as an `AgentProcess`; optional typed `input_schema` for early payload validation
- `OpenAIAgent` — wraps an OpenAI Agents SDK `Agent`; maps handoffs to Civitas `send()` calls

#### YAML topology

- Declarative topology YAML — supervision tree, transport, plugins in one file
- Full field schema: supervision strategies, backoff, transport per-implementation config, plugin config
- Process affinity — `process: worker` assigns agents to named Worker processes
- `Runtime.from_config()` with short-name `agent_classes` map
- Flat agent shorthand (`agent: { name: ..., type: ... }`)
- Case-insensitive strategy and backoff values

#### CLI

- `civitas run` — start the runtime from a topology file; `--transport`, `--process`, `--nats-url` overrides
- `civitas topology validate` — structural and configuration validation with grouped output; exit 1 on failure (CI-safe)
- `civitas topology show` — render the supervision tree with inline restart policies
- `civitas topology diff` — meaningful diff between two topology files grouped by section
- `civitas deploy docker-compose` — generate `Dockerfile`, `docker-compose.yml`, and `.env` from a topology; one service per process group
- `civitas state list` / `show` / `clear` — inspect and manage persisted agent state

#### Serialization

- `MsgpackSerializer` — default, binary, fast
- `JsonSerializer` — human-readable, selectable via `AGENCY_SERIALIZER=json`
- All messages serialized even on InProcessTransport — guarantees transport-swap transparency
- `DeserializationError` with stable contract and schema versioning

### Changed

- `Registry` redesigned as `LocalRegistry` with `RoutingEntry` dataclass and glob-pattern `lookup_all()` for broadcast
- `ComponentSet` extracted from `Runtime` to eliminate wiring duplication between `Runtime` and `Worker`
- Backoff computation moved to `Supervisor._compute_backoff()` with explicit jitter on EXPONENTIAL
- Supervisor crash handling uses tracked `asyncio.Task` set (`_pending_crash_tasks`) to prevent races with shutdown
- Sliding window uses `collections.deque` for O(1) append/popleft; child lookup uses supplementary dict for O(1) by name

### Fixed

- ZMQ transport: idempotent `start()`, correct error handling on socket close, reply routing for cross-process messages
- NATS transport: reconnection handling, JetStream stream creation idempotency
- Supervisor: crash handler tasks are cancelled before children are stopped (`stop()` teardown ordering)
- Supervisor: `ONE_FOR_ALL` and `REST_FOR_ONE` skip agents already in `STOPPED` / `STOPPING` / `CRASHED` states
- `AgentProcess`: state restored from `StateStore` before `on_start()` runs on restart
- `MessageBus`: `_agency.*` message type validation applied to all routes, not just system senders
- `OpenAIAgent`: unregistered handoff targets log a warning rather than crashing the handler
- `LangGraphAgent`: non-dict graph outputs are wrapped in `{"output": value}` rather than raising `TypeError`
- Pre-commit hooks: ruff + mypy run on every commit; CI enforces 85% coverage threshold

[Unreleased]: https://github.com/civitas-io/python-civitas/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/civitas-io/python-civitas/compare/v0.1.0...v0.3.0
[0.1.0]: https://github.com/civitas-io/python-civitas/releases/tag/v0.1.0
