# Milestones

Development progress across all phases of Civitas. Two parts: **[Part 1 — Shipped](#part-1--shipped)**
(completed work, historical record — do not edit) and **[Part 2 — Backlog](#part-2--backlog)** (the
active todo list — everything not yet done).

---

## Status legend

| Symbol | Status |
|--------|--------|
| ✅ | Completed |
| 🔄 | In Progress |
| ⏳ | Planned |
| ⏸️ | Deferred |
| 💡 | Idea — to be specced |
| 🗂️ | Tracked backlog (index of deferred work) |

---

## Part 1 — Shipped

Everything in this part is done.

### Overview

| Phase | Milestone | Completed |
|-------|-----------|-----------|
| 1 | [Core Runtime](#phase-1-core-runtime) | Mar 2026 |
| 2 | [Ecosystem — Transports](#m21-zmq-multi-process-transport) | Mar 2026 |
| 2 | [Ecosystem — Observability](#m23-otel-observability) | Apr 2026 |
| 2 | [Ecosystem — EvalLoop (local)](#m25-evalloop) | Apr 2026 |
| 2 | [Ecosystem — Remote Eval Exporters](#m26-remote-eval-exporters) | Apr 2026 |
| 3 | [Developer Experience — CLI & Dashboard](#phase-3-developer-experience) | Mar 2026 |
| 3 | [Developer Experience — MCP Integration](#m34-mcp-integration) | Apr 2026 |
| 3 | [Developer Experience — GenServer](#m35-genserver) | Apr 2026 |
| — | [Infrastructure & Release](#infrastructure--release) | Apr 2026 |
| 4 | [Dynamic Agent Spawning](#m41b-dynamic-agent-spawning) | Apr 2026 |
| 4 | [Security Hardening](#m42-security-hardening) | May 2026 |
| 4 | [Codebase Security & Enterprise Posture](#m43-codebase-security--enterprise-posture) | Apr 2026 |
| 4 | [Capability-Aware Registry](#m44-capability-aware-registry) | May 2026 |
| 4 | [HTTP Gateway](#http-gateway) | Apr 2026 |
| 4 | [Gateway API Surface](#gateway-api-surface) | Apr 2026 |
| 4 | [Postgres StateStore + Migration](#postgres-statestore--migration) | May 2026 |
| — | [v0.4.0 Release Fixes](#v040-release-fixes) | Jul 2026 |
| — | [v0.5.0 — Released](#v050--released) | Jul 2026 |
| — | [v0.6.0 — Gateway Completion](#v060--gateway-completion-released) | Jul 2026 |
| — | [v0.7.0 / v0.7.1 / v0.7.2 / v0.7.3 / v0.7.4 — Spawn Maturation, Gateway Auth & Bus-Native Streaming](#v070--spawn-maturation--gateway-auth-released) | Jul 2026 |

---

## Phase 1 — Core Runtime

**Status: ✅ Completed — March 2026**

| # | Deliverable | Priority | Status |
|---|-------------|----------|--------|
| M1.1 | `AgentProcess` base class, mailbox, `handle()` lifecycle | 🔴 High | ✅ |
| M1.2 | `Supervisor` with `ONE_FOR_ONE`, `ONE_FOR_ALL`, `REST_FOR_ONE` strategies | 🔴 High | ✅ |
| M1.3 | Backoff policies (`CONSTANT`, `LINEAR`, `EXPONENTIAL`), restart windows, crash timestamps | 🔴 High | ✅ |
| M1.4 | `Serializer` with msgpack + schema versioning; `DeserializationError` contract | 🔴 High | ✅ |
| M1.5 | `InProcessTransport` + `MessageBus` routing; request-reply with ephemeral topics | 🔴 High | ✅ |
| M1.6 | `StateStore` protocol; SQLite plugin; state persistence across restarts | 🟡 Medium | ✅ |
| M1.7 | Plugin system; LLM providers (Anthropic, OpenAI, Gemini, Mistral, LiteLLM) | 🔴 High | ✅ |

> M1.8 (Medicus self-healing hero demo) is not done — tracked in [Part 2 — Backlog](#part-2--backlog).

---

## Phase 2 — Ecosystem

### M2.1 — ZMQ Multi-Process Transport

**Status: ✅ Completed — March 2026**

| Deliverable | Status |
|-------------|--------|
| `ZMQTransport` with XSUB/XPUB proxy | ✅ |
| `ZMQProxy` daemon thread | ✅ |
| PUB/SUB bridging across OS processes | ✅ |
| Request-reply over ephemeral topics | ✅ |
| `Worker` process class for multi-process deployment | ✅ |

---

### M2.2 — NATS Distributed Transport

**Status: ✅ Completed — March 2026**

| Deliverable | Status |
|-------------|--------|
| `NATSTransport` with JetStream support | ✅ |
| At-least-once delivery via durable consumers | ✅ |
| Multi-machine deployment support | ✅ |
| Worker multi-transport handoff | ✅ |

---

### M2.3 — OTEL Observability

**Status: ✅ Completed — April 2026**

| Deliverable | Status |
|-------------|--------|
| `Tracer` with automatic span generation per message | ✅ |
| `SpanQueue` with overflow protection | ✅ |
| `OTELAgent` batch exporter with configurable flush interval | ✅ |
| `ConsoleBackend` and `FanOutBackend` | ✅ |
| OTLP gRPC exporter plugin | ✅ |
| Trace propagation across agents (trace_id, parent_span_id) | ✅ |

---

### M2.5 — EvalLoop (Local)

**Status: ✅ Completed — April 2026**

Corrective observability loop: a supervised `EvalAgent` process monitors agent behaviour and injects correction signals back into running agents. Local in-process evaluation only — remote eval engine integrations are M2.6. See [design spec](design/evalloop.md).

| Deliverable | Status |
|-------------|--------|
| `civitas/evalloop.py` — `EvalEvent`, `CorrectionSignal`, `EvalAgent` base class | ✅ |
| `AgentProcess.emit_eval(event_type, payload, eval_agent)` — emit observable events | ✅ |
| `AgentProcess.on_correction(message)` — override hook for nudge/redirect signals | ✅ |
| `civitas.eval.halt` message type — cleanly stops target agent (on_stop still runs) | ✅ |
| Rate limiting — sliding window per target agent (`max_corrections_per_window`, `window_seconds`) | ✅ |
| `EvalExporter` protocol — interface defined, not implemented (M2.6) | ✅ |
| Topology YAML — `type: eval_agent` shorthand in `Runtime.from_config()` | ✅ |
| 20 unit + integration tests | ✅ |
| `EvalAgent` exported from `civitas` top-level package | ✅ |

#### Implementation checklist

1. **Core module — `civitas/evalloop.py`**
   - [x] `EvalEvent` dataclass: `agent_name`, `event_type`, `payload`, `trace_id`, `message_id`, `timestamp`
   - [x] `CorrectionSignal` dataclass: `severity` (nudge / redirect / halt), `reason`, `payload`
   - [x] `EvalExporter` protocol: `async export(event: EvalEvent) -> None`
   - [x] `EvalAgent(AgentProcess)` — `handle()` routes `civitas.eval.event` messages
   - [x] `on_eval_event(event: EvalEvent) -> CorrectionSignal | None` — override point
   - [x] Rate limiter — sliding window, keyed by target agent name, drops + logs when exceeded
   - [x] For nudge/redirect: send `civitas.eval.correction` to target agent
   - [x] For halt: send `civitas.eval.halt` to target agent

2. **AgentProcess integration**
   - [x] `emit_eval(event_type, payload, eval_agent="eval_agent")` — sends `civitas.eval.event`; no-op if bus not wired
   - [x] `on_correction(message: Message)` — override hook called on `civitas.eval.correction`
   - [x] `civitas.eval.halt` handled in `_message_loop()` — breaks loop, on_stop() still runs

3. **Runtime + package**
   - [x] `type: eval_agent` shorthand in `Runtime.from_config()` `_build_node()`
   - [x] `EvalAgent` exported from `civitas.__init__`

4. **Tests (≥ 12 unit + ≥ 1 integration)**
   - [x] `EvalEvent` and `CorrectionSignal` field validation
   - [x] `on_eval_event()` returning None sends no correction
   - [x] nudge signal delivered to `on_correction()` hook
   - [x] redirect signal delivered to `on_correction()` hook
   - [x] halt signal stops target agent (status → STOPPED, on_stop runs)
   - [x] Rate limiter allows corrections up to the window limit
   - [x] Rate limiter drops corrections beyond the window limit
   - [x] Rate limiter resets after window_seconds
   - [x] `emit_eval()` is no-op when bus not wired
   - [x] `emit_eval()` reaches EvalAgent in a live runtime
   - [x] Integration: full supervision tree — EvalAgent halts a misbehaving sibling

5. **Example + release**
   - [x] `examples/eval_agent.py` — policy enforcement with halt, redirect, nudge
   - [x] `CHANGELOG.md` entry

---

### M2.6 — Remote Eval Exporters

**Status: ✅ Completed — v0.4 | Priority: 🔴 High**

Plugin adapters connecting Civitas's `EvalEvent` stream to external eval engines. All platforms consume the same `EvalEvent` schema; each exporter translates to the platform's expected format. OTEL GenAI Semantic Conventions are the alignment layer — `EvalEvent` fields map directly to standard OTEL attributes. See [design spec](design/evalloop.md).

| Deliverable | Status |
|-------------|--------|
| `EvalExporter` protocol implementation + registration on `EvalAgent` | ✅ |
| `civitas[arize]` — Arize Phoenix exporter (OTEL GenAI spans via OTLP) | ✅ |
| `civitas[fiddler]` — Fiddler exporter (export to Fiddler AI; two-way guardrail receive deferred to M4.2) | ✅ |
| `civitas[langfuse]` — Langfuse exporter (open-source, self-hostable) | ✅ |
| `civitas[braintrust]` — Braintrust exporter | ✅ |
| `civitas[langsmith]` — LangSmith exporter | ✅ |
| `emit_eval()` forwards to all registered exporters in addition to local EvalAgent | ✅ |
| Topology YAML — declare exporters per eval_agent node | ✅ |
| ≥ 5 unit tests per exporter (mocked SDK calls) | ✅ |

---

## Phase 3 — Developer Experience

### M3.1–M3.3 — CLI and Dashboard

**Status: ✅ Completed — March 2026**

| Deliverable | Status |
|-------------|--------|
| `civitas init` project scaffolding | ✅ |
| `civitas run` supervisor + worker modes | ✅ |
| `civitas topology validate / show / diff` | ✅ |
| `civitas deploy docker-compose` generation | ✅ |
| `civitas state list / clear` | ✅ |
| `civitas dashboard` live terminal dashboard | ✅ |

---

### M3.4 — MCP Integration

**Status: ✅ Completed — April 2026 | Corrected — July 2026**

> **Correction (July 2026):** This section originally described `MCPClient` and `MCPTool` as
> civitas-core deliverables. Per [`boundary.md`](https://github.com/civitas-io/context)'s
> ownership split, the MCP tools gateway (Fabrica) is a civitas-contrib concern. The actual
> implementation moved there; civitas core kept only the wire-layer types and the lazy-import
> integration point. Table below reflects current reality, not the original plan.

MCP protocol plumbing — the wire layer between Civitas agents and MCP tool servers. Agents call tools by direct address (`mcp://server/tool`); civitas core owns the config types and the `AgentProcess.connect_mcp()` integration point; the actual client/transport implementation lives in Fabrica (civitas-contrib).

**Scope:** civitas core keeps config types (no `mcp` SDK dependency at import time) and the `connect_mcp()` lazy-import hook. Connection handling, tool wrapping, connection pooling, circuit breakers, unified tool namespacing, and semantic retrieval are **not** in core scope — they belong to Fabrica. See [design spec](design/mcp-integration.md) (describes the original plan; superseded on the client/tool split, see correction above).

**Dependency chain:** M3.4 (types + integration point) → Fabrica (`MCPClient`, `MCPTool`, pooling, retrieval)

| Deliverable | Status | Lives in |
|-------------|--------|----------|
| `civitas.mcp.types` — `MCPServerConfig`, `MCPToolSchema`, `MCPToolError` (no `mcp` SDK import) | ✅ | civitas core |
| `AgentProcess.connect_mcp(config)` — lazily imports `fabrica.mcp.client.MCPClient`, registers tools into `self.tools`; idempotent | ✅ | civitas core |
| `ToolRegistry.deregister_prefix(prefix)` | ✅ | civitas core |
| Topology YAML `mcp.servers` block — parsed into `Runtime._mcp_configs`, auto-connect at agent startup | ✅ | civitas core |
| `MCPClient` — connect (stdio + SSE), `list_tools`, `call_tool` | ✅ | **civitas-contrib (fabrica)** — not this repo |
| `MCPTool(ToolProvider)` — `mcp://server_name/tool_name` name scheme, `civitas.mcp.call` OTEL span | ✅ | **civitas-contrib (fabrica)** — not this repo |
| `civitas[mcp]` optional extra | ❌ removed | use `pip install fabrica[mcp]` instead |
| `CivitasMCPServer(GenServer)` — expose an agent tree as an MCP server | ⏸️ | deferred to Fabrica (scope boundary decision), not started anywhere |
| Unit tests (types + YAML parsing, core-side only) | ✅ | civitas core (`tests/unit/test_mcp.py`) |

**Explicitly out of scope for civitas core:**
- `MCPClient` / `MCPTool` implementation — Fabrica (civitas-contrib)
- Connection pooling / persistent sessions — Fabrica (`MCPToolSource`)
- Circuit breakers per server — Fabrica
- Semantic or keyword tool retrieval (`find_tools`) — Fabrica
- Unified cross-agent tool namespace — M4.4 ToolStore
- Per-agent credential isolation — M4.2 Security Hardening

---

### M3.5 — GenServer

**Status: ✅ Completed — April 2026**

OTP-style generic server primitive for separating stateful API/RPC service processes from AI agent processes on the message bus. See [design spec](design/genserver.md).

| Deliverable | Status |
|-------------|--------|
| `GenServer` base class with `handle_call` / `handle_cast` / `handle_info` dispatch | ✅ |
| `call()` — synchronous request-reply with timeout | ✅ |
| `cast()` — async fire-and-forget | ✅ |
| `send_after()` — delayed self-message (tick / timer support) | ✅ |
| `init()` — startup initialisation hook | ✅ |
| Supervision-compatible (works as a child of any `Supervisor`) | ✅ |
| Topology YAML support (`type: gen_server`) | ✅ |
| 19 unit tests | ✅ |
| `examples/rate_limiter.py` — token-bucket rate limiter demo | ✅ |

#### Implementation checklist

Ordered tasks — each step is independently mergeable.

1. **Core module — `civitas/genserver.py`**
    - [ ] `GenServer(AgentProcess)` class — no LLM or tool plugin injection
    - [ ] `handle()` dispatcher: route by `reply_to` → `handle_call`; `__cast__` marker → `handle_cast`; else → `handle_info`
    - [ ] `handle_call` / `handle_cast` / `handle_info` stubs with correct signatures
    - [ ] `async def init()` hook invoked once at process start
    - [ ] `send_after(delay_ms, payload)` — schedules `handle_info` to self
    - [ ] Track `send_after` tasks; cancel all on `stop()`
    - [ ] Enforce `handle_call` returns a dict (reject `None` to prevent caller hangs)
2. **`call()` / `cast()` aliases**
    - [ ] `AgentProcess.call(name, payload, timeout)` — alias over existing `ask()`
    - [ ] `AgentProcess.cast(name, payload)` — `send()` with `__cast__` marker
    - [ ] `Runtime.call()` / `Runtime.cast()` — external entry points
3. **Topology YAML support**
    - [ ] Loader accepts `type: gen_server` (module/class resolution identical to `type: agent`)
    - [ ] `civitas topology validate` passes for gen_server nodes
    - [ ] `civitas topology show` renders gen_server with distinct icon/label
    - [ ] `civitas topology diff` treats gen_server nodes correctly
4. **Observability**
    - [ ] Emit `civitas.genserver.call` span for `handle_call`
    - [ ] Emit `civitas.genserver.cast` span for `handle_cast`
    - [ ] Emit `civitas.genserver.info` span for `handle_info`
    - [ ] Trace propagation preserved across `call()` boundaries
5. **Tests (≥ 15 cases in `tests/test_genserver.py`)**
    - [ ] `handle_call` returns reply via `reply_to`
    - [ ] `handle_cast` runs, no reply emitted
    - [ ] `handle_info` invoked for non-call non-cast messages
    - [ ] `call()` timeout raises within configured bound
    - [ ] `send_after` fires `handle_info` after delay
    - [ ] `send_after` tasks cancelled cleanly on `stop()`
    - [ ] `init()` runs before first message handled
    - [ ] GenServer as child of `ONE_FOR_ONE`, `ONE_FOR_ALL`, `REST_FOR_ONE` supervisors
    - [ ] Restart triggers `init()` again (state resets unless `StateStore` configured)
    - [ ] `StateStore`-backed state survives restart
    - [ ] `self.llm` not present on GenServer instance
    - [ ] `self.tools` not present on GenServer instance
    - [ ] `handle_call` returning non-dict raises
    - [ ] GenServer ↔ AgentProcess sibling communication round-trip
    - [ ] Topology YAML round-trip: load → run → `topology show` matches
6. **Example + documentation**
    - [ ] `examples/rate_limiter/` — end-to-end `RateLimiter(GenServer)` with consumer agent
    - [ ] User guide page referencing `docs/design/genserver.md`
    - [ ] API reference entry for `civitas.genserver`
    - [ ] `mkdocs.yml` nav updated
7. **Release**
    - [ ] `CHANGELOG.md` entry under `## [0.3.0]`
    - [ ] Cross-reference M3.4 (MCP) and M2.5 (EvalLoop) for coordinated v0.3 cut

---

## Infrastructure & Release

**Status: ✅ Completed — April 2026**

| Deliverable | Status | Completed |
|-------------|--------|-----------|
| Agency → Civitas rename (115 files) | ✅ | Apr 2026 |
| Pre-commit hooks (ruff, mypy, file hygiene) | ✅ | Apr 2026 |
| GitHub Actions CI (Python 3.12 / 3.13 / 3.14) | ✅ | Apr 2026 |
| PyPI publishing via OIDC trusted publishing | ✅ | Apr 2026 |
| GitHub Pages documentation site | ✅ | Apr 2026 |
| Test coverage raised from 85% → 90%+ | ✅ | Apr 2026 |
| Framework adapters: LangGraph, OpenAI Agents SDK, CrewAI (stub) | ✅ | Mar 2026 — **civitas-contrib**, not this repo; `civitas/adapters/` does not exist in python-civitas |

---

## Phase 4 — Platform Maturation

### M4.1b — Dynamic Agent Spawning

**Status: ✅ Completed — April 2026 | Priority: 🔴 High**

Agents spawn and decommission other agents at runtime. Enables LLM-driven orchestrators that create specialist agents on demand. See [design spec](design/dynamic-spawning.md).

**Design decisions locked:**
- `DynamicSupervisor` is a separate class from `Supervisor` (Erlang-faithful separation — ONE_FOR_ONE only, starts empty)
- `DynamicSupervisor` is declared as a static child in topology YAML; its *children* are dynamic
- `self.spawn()` targets the **nearest ancestor `DynamicSupervisor`** — no explicit target at the call site
- `on_spawn_requested` is a governance veto hook on `DynamicSupervisor` (return `False` to deny)
- `max_children` enforces blast radius per `DynamicSupervisor`

**Open design questions (being resolved):**
- ~~Q2 — Restart semantics~~ → transient default; no escalation on exhaustion; `on_child_terminated` hook
- Q3 — `on_spawn_requested` placement (supervisor vs agent vs both)
- ~~Q4 — Limit semantics~~ → both: `max_children` (concurrent) + `max_total_spawns` (lifetime budget)
- ~~Q5 — Despawn semantics~~ → `despawn()` hard stop + `stop(drain, timeout)` soft stop (awaitable, timeout fallback to hard stop)
- ~~Q6 — Cross-process spawning~~ → bus message protocol from day one; in-process v0.4; cross-process v0.5 (homogeneous deployments)
- ~~Q7 — `topology show` live state~~ → `TopologyServer(GenServer)` JSON HTTP endpoint; CLI pings `/topology`; falls back to static YAML if unreachable

| Deliverable | Status |
|-------------|--------|
| `DynamicSupervisor` class — starts empty, ONE_FOR_ONE, `max_children` + `max_total_spawns` limits | ✅ |
| `type: dynamic_supervisor` in topology YAML | ✅ |
| `self.spawn(AgentClass, name, config)` — nearest ancestor routing | ✅ |
| `self.despawn(name)` — hard stop; `self.stop(drain, timeout)` — soft stop | ✅ |
| `on_spawn_requested` governance hook on `DynamicSupervisor` | ✅ |
| `on_child_terminated` notification hook on spawning agent | ✅ |
| `Runtime.spawn()` / `Runtime.despawn()` / `Runtime.stop_agent()` — external entry points | ✅ |
| `SpawnError` added to error hierarchy | ✅ |
| 38 unit + integration tests | ✅ |
| `TopologyServer(GenServer)` — supervised JSON HTTP management endpoint | ✅ |
| `topology show` pings `TopologyServer`; falls back to static YAML | ✅ |
| `examples/dynamic_spawning.py` | ✅ |

---

### M4.2 — Security Hardening

**Status: ✅ Completed — v0.4 | Priority: 🔴 High**

Design approved. Splits into five independently shippable sub-milestones — see [`docs/design/security-hardening.md`](design/security-hardening.md) for full rationale, design decisions, and resolved questions.

Recommended delivery order: **a → c → d → e → b**.

#### M4.2a — Identity & Signing

**Status: ✅ Complete**

| Deliverable | Status |
|-------------|--------|
| `civitas/security/` package: `IdentityConfig`, `SigningConfig`, `SecurityConfig` | ✅ |
| `AgentIdentity`: Ed25519 keypair generation, OpenSSH-style storage (`id_ed25519` / `id_ed25519.pub`) | ✅ |
| `KeyRegistry`: public key lookup by agent name | ✅ |
| `MessageSigner`: sign outgoing envelopes (v=2 wire format), verify incoming | ✅ |
| `NonceCache`: bounded LRU replay protection (10k entries) | ✅ |
| `SignatureError` — new `CivitasError` subclass | ✅ |
| `SigningSerializer` wrapping `MsgpackSerializer` | ✅ |
| Multi-node key distribution: public keys in topology YAML; spawn-message vouching for dynamic agents | ✅ |
| `security:` YAML block parsing in `Runtime.from_config()` | ✅ |
| InProcess transport: signing bypassed entirely (D9 performance rule) | ✅ |
| `signing.allow_unsigned: true` escape hatch for rolling upgrades | ✅ |
| Unit + integration tests ≥90% coverage on new code | ✅ |

#### M4.2b — Transport mTLS

**Status: ✅ Complete**

| Deliverable | Status |
|-------------|--------|
| ZMQ CURVE: server keypair on proxy, client keypairs on Workers | ✅ |
| NATS TLS + nkeys: Ed25519-based subject auth, TLS cert/key/CA config | ✅ |
| `security.transport` YAML block plumbing into ZMQ and NATS transports | ✅ |
| `civitas security init` CLI — scaffold keys and config for ZMQ/NATS deployments | ✅ |

#### M4.2c — Credential Isolation

**Status: ✅ Complete**

| Deliverable | Status |
|-------------|--------|
| `${VAR_NAME}` env-var substitution in `Runtime.from_config()` | ✅ |
| Unset variable raises `ConfigurationError` with clear message | ✅ |
| `civitas.secrets.SecretsProvider` protocol + file/env/Vault implementations | ✅ |
| Per-agent `credentials:` block in topology YAML | ✅ |
| Plugin handles: `self.llm("anthropic")` resolves per-agent credential at call time | ✅ |

#### M4.2d — Tool Sandbox

**Status: ✅ Complete**

| Deliverable | Status |
|-------------|--------|
| Bubblewrap wrapper for MCP subprocess execution on Linux | ✅ |
| `sandbox:` YAML block per MCP server (network, filesystem allowlists) | ✅ |
| Refuse-to-start when `sandbox.enabled: true` and `bwrap` unavailable | ✅ |
| Clear error messages with per-distro install instructions | ✅ |

#### M4.2e — Audit Log

**Status: ✅ Complete**

| Deliverable | Status |
|-------------|--------|
| `civitas.audit` module: `AuditEvent` TypedDict, `AuditSink` protocol | ✅ |
| `JsonlFileSink`: batched fsync (100ms / 100 events), `sync_writes` option, SIGHUP rotation | ✅ |
| `NullSink` for tests | ✅ |
| Emission at chokepoints: `MessageBus.route()`, `MCPTool.execute()`, sandbox violations, secret access | ✅ |
| `SyslogSink` and `OtlpSink` implementations | ✅ |

---

### M4.3 — Codebase Security & Enterprise Posture

**Status: ✅ Completed — April 2026 | Priority: 🔴 High**

Complements M4.2. Where M4.2 hardens the **runtime** (mTLS, message signing, credential isolation, sandboxing), M4.3 hardens the **codebase and supply chain** so enterprises have a clear security story before adoption: known vulnerabilities tracked, dependencies scanned, secrets never committed, a published threat model, and a documented disclosure process.

The deliverables are split across tooling (CI-enforced scanners), documentation (threat model, security architecture, adoption checklist), and process (disclosure policy, release notes, third-party audit).

| Deliverable | Status |
|-------------|--------|
| SAST in CI — Bandit + Semgrep on every PR, fail build on `HIGH`+ | ✅ |
| Dependency scanning — `pip-audit` in CI + Dependabot weekly | ✅ |
| SBOM generation — CycloneDX SBOM published with every release | ✅ |
| Secret scanning — `gitleaks` pre-commit hook + CI job on full history | ✅ |
| `docs/security/threat-model.md` — STRIDE analysis per runtime component | ✅ |
| `docs/security/architecture.md` — security model (trust boundaries, supervision, transport isolation) | ✅ |
| `SECURITY.md` — responsible disclosure policy, contact, supported versions, response SLAs | ✅ |
| `docs/security/enterprise-checklist.md` — adoption checklist (deployment hardening, config review, audit log integration) | ✅ |
| External security audit before v1.0 — fix all `HIGH`+ findings, publish summary | ⏳ Deferred to pre-v1.0 |
| Continuous posture — CVE watch on runtime deps, security release notes, CVSS-scored advisories | ⏳ Ongoing process |

---

### M4.4 — Capability-Aware Registry

**Status: ✅ Completed — May 2026 | Priority: 🟡 Medium**

Agents declare capability tags at the class level; the registry supports filtered lookups; agents can route to any capable peer without knowing its name.

| Deliverable | Status |
|-------------|--------|
| `RoutingEntry.capabilities` + `RoutingEntry.capability_metadata` fields | ✅ |
| `LocalRegistry.register()` / `register_remote()` accept capabilities | ✅ |
| `find_by_capability(tag)` — all agents (local + remote) with that tag | ✅ |
| `find_by_capabilities(tags, match="any"\|"all")` — multi-tag filtered lookups | ✅ |
| `AgentProcess.capabilities` / `capability_metadata` class-level declarations | ✅ |
| `AgentProcess.send_capable(capability, payload)` — fire-and-forget to any capable agent | ✅ |
| `CapabilityNotFoundError` raised when no registered agent declares the tag | ✅ |
| YAML `capabilities:` / `capability_metadata:` block overrides class-level defaults | ✅ |
| Distributed propagation: Worker announcements carry capabilities; `_on_remote_register` populates remote entries | ✅ |
| `RegistryListener` hook: async callbacks fired after every register/deregister (Presidium integration point) | ✅ |
| `LocalRegistry.add_listener()` / `remove_listener()` — fire-and-forget tasks with error logging | ✅ |
| Public exports: `RoutingEntry`, `RegistryListener`, `CapabilityNotFoundError` from `civitas` top-level | ✅ |
| 29 unit tests covering all registry operations, listener lifecycle, and `send_capable` | ✅ |

#### Design notes

**Boundary with Presidium**: Civitas capability tags are operational routing data — plain strings by convention (e.g., `"text.summarize"`). Presidium owns the controlled vocabulary, human-readable descriptions, and governance metadata. Presidium plugs in via the `RegistryListener` hook — it receives every register/deregister event with full capability info and maintains its own authoritative Agent Registry.

**Distributed topology**: Every node (Runtime and Worker) has a complete capability view of the deployment. Worker announcements include `capabilities` and `capability_metadata`; the Runtime's `_on_remote_register` handler populates `register_remote()` entries. `send_capable()` thus works transparently across process boundaries.

**Tag format**: plain strings, dot-namespaced by convention (`"domain.action"`). No enum enforcement — Presidium owns the controlled vocabulary and Civitas treats tags as opaque routing keys.

---

### HTTP Gateway

**Status: ✅ Completed — April 2026**

Supervised edge process bridging external HTTP traffic into the Civitas message bus. HTTP/1.1 + HTTP/2 (uvicorn) and HTTP/3 / QUIC (aioquic) in v0.4. gRPC deferred to v0.5. See [design spec](design/http-gateway.md).

| Deliverable | Status |
|-------------|--------|
| `HTTPGateway(AgentProcess)` — ASGI app, request translation, route table | ✅ |
| HTTP/1.1 + HTTP/2 via uvicorn[standard] — uvloop + httptools (`civitas[http]`) | ✅ |
| HTTP/3 / QUIC via aioquic — `Alt-Svc` header, 0-RTT (`civitas[http3]`) | ✅ |
| TLS config from topology YAML / env vars | ✅ |
| Topology YAML support (`type: http_gateway`) | ✅ |
| Graceful drain on supervisor shutdown | ✅ |
| ≥ 20 unit tests + ≥ 5 integration tests | ✅ |
| `examples/http_gateway.py` | ✅ |
| gRPC via grpclib / grpcio | ⏸️ v0.5 |
| Custom `.proto` loading from `proto_dir` | ⏸️ v0.5 |

#### Implementation checklist

1. **Package setup**
   - [x] `civitas/gateway/__init__.py` — package stub, re-export `HTTPGateway`
   - [x] `civitas[http]` extra in `pyproject.toml` — `uvicorn[standard]>=0.30`
   - [x] `civitas[http3]` extra — `aioquic>=1.0`

2. **Core — `civitas/gateway/core.py`**
   - [x] `GatewayConfig` dataclass — `host`, `port`, `port_quic`, `tls_cert`, `tls_key`, `request_timeout`, `enable_http3`
   - [x] `HTTPGateway(AgentProcess)` — holds config, route table, uvicorn server reference
   - [x] `on_start()` — install uvloop (Linux/macOS), start uvicorn server as background task
   - [x] `on_stop()` — signal uvicorn to drain in-flight requests, cancel server task
   - [x] `handle()` — handles internal messages (e.g., topology-triggered reconfiguration); no-op for now

3. **ASGI app — `civitas/gateway/asgi.py`**
   - [x] `GatewayASGI.__call__(scope, receive, send)` — ASGI callable
   - [x] HTTP scope: parse method, path, headers, body
   - [x] Route lookup: path + method → agent name, mode (`call` vs `cast`)
   - [x] Default routes: `POST /agents/{name}` → `call`, `POST /agents/{name}/cast` → `cast`
   - [x] HTTP → `Message` translation: body → `payload`, `X-Civitas-Type` → `type`, `traceparent` → trace context
   - [x] `call()` mode: await reply, serialise `payload` as JSON response body
   - [x] `cast()` mode: fire-and-forget, return HTTP 202
   - [x] Timeout: `asyncio.wait_for` with `request_timeout`; return HTTP 504 on expiry
   - [x] Error mapping: `payload.error` → 400, no route → 404, unhandled exception → 500

4. **Router — `civitas/gateway/router.py`**
   - [x] `RouteEntry` dataclass — `method`, `path_pattern`, `agent`, `mode`
   - [x] `RouteTable` — ordered list of `RouteEntry`; `match(method, path)` returns `(RouteEntry, path_params)`
   - [x] Path parameter extraction: `{name}` segments captured into dict
   - [x] Default route fallback when no custom routes are configured
   - [x] YAML route loading: `config.routes` list → `RouteEntry` instances

5. **HTTP/3 — `civitas/gateway/h3.py`**
   - [x] `H3Server` — wraps aioquic QUIC server; runs on `port_quic` (UDP)
   - [x] HTTP/3 request → same `GatewayASGI` handler (reuse ASGI layer)
   - [x] `Alt-Svc: h3=":port_quic"` header injected into all HTTP/1.1 and HTTP/2 responses
   - [x] `H3Server` started / stopped alongside uvicorn in `on_start()` / `on_stop()`

6. **Topology YAML support**
   - [x] `type: http_gateway` in `Runtime.from_config()` `_build_node()`
   - [x] `GatewayConfig` populated from YAML `config:` block; `!ENV` resolver for TLS cert/key paths
   - [x] `civitas topology validate` accepts `type: http_gateway` nodes without errors
   - [x] `civitas topology show` displays gateway node with `[http]` / `[http3]` label

7. **Tests (≥ 20 unit, ≥ 5 integration)**
   - [x] `RouteTable.match()` — exact path, path parameters, method mismatch, no route
   - [x] Default route fallback: `POST /agents/foo` → `call("foo", body)`
   - [x] `call` mode: reply payload returned as JSON 200
   - [x] `cast` mode: 202 returned immediately
   - [x] Timeout: `request_timeout=0.001` → 504
   - [x] Error mapping: `payload.error` → 400; unhandled exception → 500
   - [x] No route: 404
   - [x] `traceparent` header propagated into `message.trace_id`
   - [x] `GatewayConfig` validation: missing TLS cert when `enable_http3=True`
   - [x] `on_start()` installs uvloop on Linux
   - [x] `on_stop()` cancels server task cleanly
   - [x] Integration: real HTTP client (`httpx.AsyncClient`) → gateway → `AgentProcess` → reply
   - [x] Integration: concurrent requests all return correct replies
   - [x] Integration: gateway node in topology YAML starts correctly via `Runtime.from_config()`

8. **Example + release**
   - [x] `examples/http_gateway.py` — minimal REST API with two agent endpoints
   - [x] `CHANGELOG.md` entry under `## [Unreleased]`

---

### Gateway API Surface

**Status: ✅ Completed — April 2026**

Declarative routes, Pydantic request/response validation, middleware chain, and auto-generated OpenAPI 3.1 docs on top of `HTTPGateway`. See [design spec](design/gateway-api-surface.md).

| Deliverable | Status |
|-------------|--------|
| `@route` decorator — documents HTTP method + path on agent handler (YAML is authoritative for wiring) | ✅ |
| Path parameter extraction into `message.payload` | ✅ |
| `@contract` decorator — Pydantic request/response validation, 422 error shape | ✅ |
| `GatewayRequest` / `GatewayResponse` / `NextMiddleware` types | ✅ |
| Global + route-scoped middleware chain | ✅ |
| Stateful GenServer middleware via `request.gateway.call()` | ✅ |
| Auto-generated OpenAPI 3.1 spec at `GET /openapi.json` | ✅ |
| Swagger UI at `GET /docs`, ReDoc at `GET /redoc` | ✅ |
| YAML-declared routes and schemas (no decorators required) | ✅ |
| `civitas topology validate` cross-checks YAML routes against `@route` decorators | ✅ |
| ≥ 15 unit tests + ≥ 3 integration tests | ✅ |

**Routing authority:** YAML is the single source of truth for gateway wiring. `@route` stores metadata on the method object only — it is never read by the gateway at runtime. Its value is (1) colocated documentation of intent and (2) a machine-checkable annotation that `civitas topology validate` cross-references against YAML to warn on drift.

#### Implementation checklist

1. **Types — `civitas/gateway/types.py`**
   - [x] `GatewayRequest` dataclass — `method`, `path`, `path_params`, `query_params`, `headers`, `body`, `client_ip`, `gateway` (AgentProcess ref)
   - [x] `GatewayResponse` dataclass — `status`, `body`, `headers`
   - [x] `NextMiddleware` type alias — `Callable[[GatewayRequest], Awaitable[GatewayResponse]]`

2. **Route decorator — `civitas/gateway/router.py`**
   - [x] `@route(method, path, mode="call")` — stores `_civitas_route` metadata dict on the decorated function; no side effects, no global registry
   - [x] `RouteTable.from_config(routes_config)` — sole runtime source; builds `RouteEntry` list from topology YAML `routes:` block
   - [x] `RouteTable.from_class(cls)` — validation-only helper; scans class methods for `_civitas_route` metadata; used exclusively by `civitas topology validate`
   - [x] `civitas topology validate`: when a gateway node references an agent, import the class and warn if a YAML route has no matching `@route` on the handler, or if a `@route` exists with no corresponding YAML entry

3. **Contract decorator — `civitas/gateway/contracts.py`**
   - [x] `@contract(request=Model, response=Model)` — stores `_civitas_contract` metadata on the function; `request` and `response` are optional Pydantic `BaseModel` subclasses
   - [x] Request validation in ASGI dispatch: if route has a contract, `Model.model_validate(body)` before calling the bus; 422 on `ValidationError` with FastAPI-compatible error shape `{"detail": [...]}`
   - [x] Response validation: `Model.model_validate(reply_payload)` after reply received; 500 on mismatch
   - [x] No-op when `@contract` not applied — pass-through

4. **Middleware — `civitas/gateway/middleware.py`**
   - [x] `MiddlewareChain` — ordered list of async callables; builds `call_next` chain via closure
   - [x] Global middleware loaded from `config.middleware` (dotted import path → callable)
   - [x] Route-scoped middleware loaded from `route.middleware`
   - [x] Execution order: global → route-scoped → contract validation → bus dispatch — parsing landed here, but wiring into the ASGI dispatch path had a gap; not actually wired until [GH #6](https://github.com/civitas-io/python-civitas/issues/6), fixed for the v0.4.0 release
   - [x] Short-circuit: middleware returning `GatewayResponse` without calling `call_next` skips remainder

5. **Wire into ASGI — `civitas/gateway/asgi.py` updates**
   - [x] Replace direct bus dispatch with: build `GatewayRequest` → run middleware chain → contract validate → dispatch
   - [x] `GatewayRequest.gateway` set to the `HTTPGateway` instance (for stateful GenServer middleware)
   - [x] Contract metadata read from the agent class method via `@route` + `@contract` on the matched handler

6. **OpenAPI — `civitas/gateway/openapi.py`**
   - [x] `build_spec()` — reads `RouteTable` (from YAML) + loads agent class to read `@contract` metadata
   - [x] Generates OpenAPI 3.1 `paths` from route entries
   - [x] Request body schema from `@contract(request=Model)` via `Model.model_json_schema()`
   - [x] Response schema from `@contract(response=Model)`
   - [x] Tags from agent name
   - [x] Auto-includes 422 response schema when request model is declared
   - [x] `GET /openapi.json` — returns generated spec
   - [x] `GET /docs` — Swagger UI (CDN-hosted, no static assets)
   - [x] `docs.enabled: false` config disables all three endpoints

7. **Tests (≥ 15 unit, ≥ 3 integration)**
   - [x] `@route` stores metadata on the function, no global registry side-effect
   - [x] `RouteTable.from_config()` builds routes correctly from config dict
   - [x] `RouteTable.from_class()` reads `@route` metadata from class methods
   - [x] Path parameters extracted correctly from URL
   - [x] `@contract` request validation: valid body → dispatched; invalid → 422 with FastAPI error shape
   - [x] `@contract` response validation: valid reply → 200; invalid → 500
   - [x] Middleware chain: all middleware called in order
   - [x] Middleware short-circuit: returning response without `call_next` skips rest of chain
   - [x] Global middleware runs before route-scoped middleware — test added for the v0.4.0 release ([GH #6](https://github.com/civitas-io/python-civitas/issues/6)); no test had actually exercised route-scoped execution before this
   - [x] `/openapi.json` returns valid OpenAPI 3.1 spec
   - [x] `/docs` returns 200 with Swagger UI HTML
   - [x] `docs.enabled: false` → `/docs` returns 404
   - [x] Tags populated from agent name
   - [x] Integration: end-to-end with real HTTP client

8. **Example + release**
   - [x] `examples/http_gateway.py` — minimal REST API with agent endpoints
   - [x] `CHANGELOG.md` entry

---

### Postgres StateStore + Migration

**Status: ✅ Completed — May 2026 | Priority: 🔴 High | Corrected — July 2026**

> **Correction (July 2026):** This section originally described `PostgresStateStore` itself as a
> civitas-core deliverable. Per [`boundary.md`](https://github.com/civitas-io/context), state
> store *implementations* (SQLite, Postgres, Redis) are a civitas-contrib concern — only the
> `StateStore` protocol, `InMemoryStateStore` (the trivial default), the plugin loader's lazy
> resolution, and the `civitas state migrate` CLI are core's job. Table corrected below.

SQLite works for single-process deployments but breaks under concurrent cross-process writes (ZMQ Level 2+, NATS Level 3). `PostgresStateStore` extends the `StateStore` protocol — switching backends is a topology YAML change with no agent code changes, and no top-level `civitas` import ever references `asyncpg` directly.

| Deliverable | Status | Lives in |
|-------------|--------|----------|
| `StateStore` protocol extended with `list_agents()` and `close()` | ✅ | civitas core |
| `InMemoryStateStore.list_agents()` / `close()` | ✅ | civitas core |
| Plugin loader entry `type: postgres` → lazy `civitas_contrib.plugins.postgres_store.PostgresStateStore` import | ✅ | civitas core (resolution only) |
| `@runtime_checkable StateStore` — `isinstance()` checks work | ✅ | civitas core |
| `civitas state migrate <src> <dst>` — dry-run by default, `--execute` to apply; lazy-imports `PostgresStateStore` from civitas-contrib | ✅ | civitas core |
| `_parse_dsn()` — `sqlite:<path>`, `.db`/`.sqlite` extension, `postgresql://` URL | ✅ | civitas core (`cli/state.py`) |
| `PostgresStateStore` — `asyncpg` backend, connection pool, `civitas_agent_state` JSONB table | ✅ | **civitas-contrib** — not this repo |
| `civitas[postgres]` optional extra — `asyncpg>=0.29` | ❌ removed from core | use `civitas-contrib[postgres]` |
| Helpful `ImportError`/`ConfigurationError` with install hint if civitas-contrib not installed | ✅ | civitas core (lazy-import pattern) |
| 20 unit tests covering protocol, migrate CLI DSN parsing, and mocked contrib import | ✅ | civitas core |
| Zero-downtime dual-write migration | ⏸️ Deferred — maintenance-window copy is sufficient for v0.4 |
| PgBouncer deployment guide | ⏸️ Deferred to docs pass |
| MySQL StateStore (`aiomysql`/`asyncmy` backend) | ⏸️ Deferred — see below; **civitas-contrib**'s job if built |

> **MySQL StateStore** — deferred because Postgres covers the multi-process persistence gap and asyncpg is a better async foundation. If ever built, it belongs in **civitas-contrib** alongside the other state store implementations, following the same lazy plugin-loader pattern (`type: mysql` loader entry resolving to `civitas_contrib.plugins.mysql_store.MySQLStateStore`, `mysql://` DSN in `_parse_dsn`).

---

## v0.4.0 Release Fixes

**Status: ✅ Completed — July 2026**

Two bugs reported against v0.3.0, found by a downstream project building against `civitas` at HEAD. Both fixed and folded into the v0.4.0 release alongside the Phase 4 work above — the v0.4.0 changes were already sitting on `main`, unreleased, when these were reported.

| Deliverable | Status |
|-------------|--------|
| [GH #6](https://github.com/civitas-io/python-civitas/issues/6) — Route-scoped gateway middleware wired into ASGI dispatch | ✅ |
| [GH #7](https://github.com/civitas-io/python-civitas/issues/7) — `civitas/py.typed` marker added | ✅ |

#### GH #6 — Route-scoped gateway middleware is parsed but never executed

`RouteEntry.middleware` (a route's own `middleware:` list in topology YAML) was parsed into
`RouteEntry` objects but never read by `GatewayASGI`. The dispatch layer built its middleware
chain once at construction time from `config.middleware` (global only) — the matched route's
`.middleware` field was never consulted. A route declaring its own auth/guard middleware (e.g.
an admin-only route on an otherwise public gateway) would silently run **without** that guard,
with no error or warning.

This contradicted `docs/gateway.md` and this file's own Gateway API Surface checklist, both of
which stated route-scoped middleware runs after global middleware, before contract validation.

**Fix:**
- `GatewayASGI._handle_http()` now matches the route *before* building the middleware chain.
- Route-scoped middleware (`entry.middleware`, resolved via the existing `load_middleware()`
  loader) is appended after global middleware when building the chain, restoring the documented
  order: global → route-scoped → contract validation → bus dispatch.
- Resolved route middleware callables are cached per `RouteEntry` (by object identity) so they
  are loaded once, not on every request.
- Unresolvable route middleware paths are logged and skipped, matching the existing behavior for
  global middleware — never raises at request time.
- Corrected the two checklist items below (Gateway API Surface, "Wire into ASGI" and "Tests")
  that had been checked off despite this gap.

#### GH #7 — Missing `py.typed` marker despite "Typing :: Typed" classifier + mypy --strict

`pyproject.toml` declares the `"Typing :: Typed"` classifier and the package runs
`mypy --strict` internally, but no `civitas/py.typed` marker file existed in the source tree or
the published 0.3.0 wheel. Downstream projects running their own `mypy --strict` got
`error: ... missing library stubs or py.typed marker [import-untyped]` for every import from
`civitas`.

**Fix:**
- Added empty `civitas/py.typed`.
- `[tool.hatch.build.targets.wheel] packages = ["civitas"]` picks it up automatically — verified
  present in the built wheel (`unzip -l dist/*.whl`) as part of the release checklist below.

---

## v0.5.0 — Released

**Status: ✅ Released — July 2026 (buckets A + B + C)**

Scope was three buckets; a fourth candidate (D) was explicitly deferred to a future version — see
below. Bucket A (correctness & hardening) ✅, Bucket B (durable suspension) ✅, Bucket C (doc
hygiene) ✅ — all done and fully tested. A `msgpack>=1.2.1` security bump (GHSA-6v7p-g79w-8964) also
landed on this line. Cutting the v0.5.0 release tag is the maintainer's call.

### A — Correctness & hardening

**Status: ✅ Completed — July 2026**

Seven items from [`context/known-issues.md`](https://github.com/civitas-io/context) (private
cross-repo tracker). Each was re-verified against the current codebase — via grep, not taken on
faith — immediately before fixing, since one originally-scoped item (F01-2, span leak on
serializer error in `bus.route()`) turned out already fixed, and F04-2 (below) turned out already
fixed too, only found because a search for its old, differently-worded issue text missed the
actual `_KNOWN_CONFIG_KEYS` implementation on first pass.

| ID | Priority | Issue | Resolution |
|---|---|---|---|
| FD-01 | 🔴 High | `MetricsCollector` not wired to real event sources — dashboard always showed 0 for message flow, restarts | `civitas.observability.metrics.MetricsSink` protocol added; injected via `ComponentSet`/`Runtime.set_metrics()`. Wires `message_handled` + `agent_error` in `AgentProcess._dispatch()`, `message_sent` in `send()`/`ask()`. **`llm_call` is not auto-wired** — `llm_span()` is a bare context manager with no interception point for `ModelResponse` token/cost data without a larger redesign of how `self.llm` is wrapped; that's a real follow-up, not silently claimed done here. |
| FD-03 | 🟡 Medium | `civitas/cli/dashboard.py` monkey-patched `runtime._root_supervisor._handle_crash` directly | `Supervisor.add_crash_callback()` + `Runtime.on_crash()` public hook added, invoked from `_handle_crash()` before the restart strategy runs. `cli/dashboard.py` monkeypatch removed. |
| F04-2 | — | ~~`Runtime.from_config` silently accepts unknown topology YAML keys~~ | **Already fixed** — `Runtime._KNOWN_CONFIG_KEYS` + the `ConfigurationError` raise already existed in `from_config_dict()`. No code change; added the missing regression test only. |
| F01-3 | 🟡 Medium | `Message.ttl` declared and documented, never enforced | `ttl` field (optional float, seconds) added to `Message`; enforced in `Mailbox.get()` — expired messages are discarded with a warning and the search continues. |
| F11-5 | 🟡 Medium | `on_stop()` not called when `on_start()` raises | `AgentProcess._start()` now wraps `on_start()`; on failure, closes the open `civitas.agent.start` span with the error, sets `CRASHED`, runs `on_stop()` + MCP client cleanup, then re-raises. The pre-existing test asserting the *old* behavior (`on_stop` not called) was inverted to assert the new one. |
| FD-07 | 🟢 Low | `Worker` processes didn't receive exporters from topology YAML | Fixed together with FD-09 (same underlying gap — see below). `exporters` now flows through `Worker.__init__` and `cli/run.py`'s `_run_worker()`. |
| FD-09 | 🟢 Low | Two parallel OTEL span export paths coexist | **Scope note:** rather than the originally-sketched "write an `OTLPExportBackend` that converts `SpanData` to OTEL wire format," which is a genuinely large, failure-prone undertaking (hand-rolling OTLP span construction) for a bug-fix pass, the actual fix makes the two paths **mutually exclusive**: `Tracer.__init__` skips the direct `TracerProvider` entirely whenever a `span_queue` is supplied. `build_component_set()` now builds a `SpanQueue` + `FanOutBackend` only when `plugins.exporters` is configured in YAML, and `Runtime`/`Worker` own starting/stopping the `OTELAgent` task. This makes `plugins.exporters: [{type: console}]` actually work for the first time — previously dead code, per `civitas/plugins/loader.py`'s own docstring example (`type: otel`) which was never backed by a real implementation. The existing OTLP env-var auto-detect path (`OTEL_EXPORTER_OTLP_ENDPOINT`) is untouched and still uses the direct `TracerProvider` path when no `plugins.exporters` are configured — zero behavior change for that default case. |

### B — Durable suspension

**Status: ✅ Done** (design + implementation)

`agent.suspend()` / `agent.resume()` — integration point #8 in
[`boundary.md`](https://github.com/civitas-io/context)'s eight-point Civitas→Presidium contract,
required for Presidium's human-in-the-loop (HITL) approval flow, where an agent must pause, durably
persist enough state to resume later (possibly after a process restart), and resume when Presidium's
policy engine or a human approves.

Design spec: [`docs/design/durable-suspension.md`](design/durable-suspension.md) (FINAL DESIGN
S1–S10). Delivered per that spec:

- `ProcessStatus.SUSPENDED` re-introduced fully-wired (removed as dead API in **F02-6**), with every
  transition defined and tested — the governing constraint that F02-6 flagged.
- Suspension pauses *dispatch* only (no coroutine snapshotting). `suspend()` is a non-blocking flag
  actioned at the message-loop boundary; while suspended only the priority queue is drained so
  business messages stay buffered (FIFO + backpressure preserved).
- Durable marker persisted inside `self.state` (reserved key `_civitas.suspended`) via the existing
  `checkpoint()` path — no `StateStore` protocol change, so all contrib stores work unchanged.
  Durable only with a persistent store (same caveat as `checkpoint()`).
- Write-ahead suspend (pause in-memory first, then persist; never falls back to RUNNING);
  approver-gated resume; marker cleared on permanent removal (despawn / restarts exhausted) but kept
  on graceful shutdown / crash-restart. Supervisor `_stop()` and restart strategies handle SUSPENDED.
- Suspend/resume emit spans + `AuditEvent`s (resume records the approver). A suspended agent does
  **not** count as crashed. `ask()` into a suspended agent times out (fail-fast deferred).

### C — Doc hygiene (completed as part of scoping this release)

`docs/milestones.md` had drifted from [`boundary.md`](https://github.com/civitas-io/context)'s
repo-ownership split in four places, all corrected in this pass:

- **M3.4 MCP Integration** — table claimed `MCPClient`/`MCPTool`/`civitas[mcp]` as civitas-core
  deliverables; they live in Fabrica (civitas-contrib). Corrected to show only the types +
  `connect_mcp()` lazy-import point as core.
- **Postgres StateStore + Migration** — table claimed `PostgresStateStore` itself as civitas-core;
  it lives in civitas-contrib. Corrected to show only the protocol, loader resolution, and
  `civitas state migrate` CLI as core.
- **Infrastructure & Release — Framework adapters** — claimed LangGraph/OpenAI adapters as
  civitas-core with CrewAI merely "planned"; `civitas/adapters/` doesn't exist in this repo at
  all — all three (including CrewAI as a stub) live in civitas-contrib. Corrected.
- **Phase 5 — Prompt Library & Playground, Skills Gateway** — framed as "Civitas-side features";
  `boundary.md` assigns both to civitas-contrib. Corrected with explicit "Lives in" callouts
  matching how Fabrica's entry already read.

### D — Explicitly deferred to a future version

Not python-civitas's job per `boundary.md`, and not touched in v0.5.0: Prompt Library &
Playground, Skills Gateway, CrewAI adapter full implementation, MySQL StateStore, Fabrica. All
belong to civitas-contrib or the separate `civitas-forge` repo — revisit there, not here.

---

## v0.6.0 — Gateway Completion (Released)

**Status: ✅ Released — 2026-07-04** ([v0.6.0](https://github.com/civitas-io/python-civitas/releases/tag/v0.6.0), [PyPI](https://pypi.org/project/civitas/0.6.0/))

The HTTP Gateway shipped in v0.4 with HTTP/1.1, HTTP/2, and HTTP/3 (QUIC), but several planned
transport and middleware features were deferred across the gateway design docs
([`http-gateway.md`](design/http-gateway.md) Phases 3–4, [`gateway-api-surface.md`](design/gateway-api-surface.md)).
v0.6.0 completes the gateway as a coherent theme. A design refresh across those two docs should
precede implementation (they were written pre-v0.4 and predate the shipped ASGI/middleware layer).

| # | Deliverable | Priority | Source |
|---|-------------|----------|--------|
| G1 | **gRPC gateway** — generic `civitas.Agent` service proxying any agent by name; `Invoke`/`Cast` unary RPCs with a `Struct` payload; committed `.proto` + `_pb2` stubs; health + server reflection; `civitas[grpc]` (grpcio default). `Stream` (server-streaming) deferred to G3; per-agent `proto_dir` loading is a non-goal | ✅ Done | grpc-gateway.md |
| G2 | **WebSocket upgrade** — long-lived bidirectional sessions: inbound frames `cast()` to an agent, agent streams back over the same socket (`ws_routes`) | ✅ Done | gateway-streaming.md |
| G3 | **SSE / true streaming responses** — `mode: "stream"` routes stream agent output as Server-Sent Events (replaces the `{"chunks": [...]}` workaround); also completes the gRPC `Stream` RPC deferred in G1 | ✅ Done | gateway-streaming.md |
| G4 | **Rate-limiting middleware** — `RateLimiter` GenServer + `rate_limit` middleware (`civitas.gateway.ratelimit`) | ✅ Done | gateway-api-surface.md |
| G5 | **Auth middleware** — first-party API-key auth (`civitas.gateway.auth.require_api_key`, fail-closed, `CIVITAS_GATEWAY_API_KEY`); JWT (opt-in `civitas[jwt]`) + mTLS remain integration points | ✅ Done (API-key; JWT/mTLS deferred) | gateway-api-surface.md |
| G6 | **File uploads** — `multipart/form-data` parsed at the ASGI edge; files delivered base64 under `__files__` | ✅ Done | gateway-api-surface.md |
| G7 | **HTTP/2 server push** | ⛔ Won't do — Server Push was removed from Chrome (2022) and is effectively dead across browsers; use SSE/WebSocket (G2/G3) for server-initiated data | http-gateway.md |
| G8 | **gRPC reflection service** — generic reflection for the gRPC surface | ✅ Done (shipped in G1) | grpc-gateway.md |
| G9 | **Evaluate quiche-python** (Rust QUIC) as a drop-in for aioquic | ✅ Done (evaluated) — no official/production-ready Python binding exists today; stay on aioquic, revisit when one matures | http-gateway.md |

**Status (2026-07-04): feature-complete.** G1–G6 and G8 shipped; G7 dropped (HTTP/2 Server Push is
dead in browsers — use G2/G3 instead); G9 evaluated (no viable Rust QUIC Python binding — stay on
aioquic). v0.6.0 Gateway Completion is ready to release.

**Non-goals for v0.6.0:** business logic in the gateway, load balancing, request queuing — the
gateway stays a thin translate-and-route edge ([`http-gateway.md`](design/http-gateway.md) Non-Goals).
A **bus-native streaming primitive** (agent-to-agent `stream()` across all transports) is also out of
scope for v0.6.0 — G2/G3 use gateway-mediated streaming; the first-class version shipped in **v0.7.1**
as R7 (see [`bus-native-streaming.md`](design/bus-native-streaming.md), [#22](https://github.com/civitas-io/python-civitas/pull/22)).

---

## v0.7.0 — Spawn Maturation & Gateway Auth (Released)

**Released 2026-07-05.** R1 (non-blocking spawn, #14), R2 (`spawn_into`, #16), R3 (JWT+mTLS auth + fail-open fix, #18), R4 (encrypted StateStore, #19), R5 (per-agent spawn quotas, #21), R6 (cross-process spawn, #20) all shipped. R7 (bus-native streaming, #15) deferred as a stretch item.

**Status: ✅ Released** — R1–R6 shipped in v0.7.0 (2026-07-05); R7 shipped in v0.7.1; R8 shipped in
v0.7.2; R9 shipped in v0.7.3; **v0.7.4** (2026-07-21) is a security patch: `click` ≥ 8.3.3
([PYSEC-2026-2132](https://osv.dev/vulnerability/PYSEC-2026-2132), transitive via typer, not
exploitable in civitas — `click.edit()` is never called) + restored the Semgrep SARIF pipeline in
the Security workflow (the action's `generateSarif` input was dropped upstream, silently
disabling SAST uploads — fail-open, now CLI-invoked).

Theme: *finish what v0.6.0 deferred.* The largest coherent cluster is **dynamic-spawn maturation**
(the #8 / #9 / #10 follow-ups + quotas + cross-process), plus completing **gateway auth** and one
**data-at-rest** security item. This also lays groundwork for a possible future **self-healing**
capability, which builds on supervision + dynamic spawn + telemetry (under investigation).

| # | Deliverable | Priority | Source |
|---|-------------|----------|--------|
| R1 | ✅ **Done (PR #14)** — **Non-blocking dynamic spawn**: `spawn(wait=False)` / `spawn_nowait()`; `on_start()` runs in-task; failures via `on_child_terminated`. Design: [`non-blocking-spawn.md`](design/non-blocking-spawn.md) (Oracle + Momus reviewed). | 🔴 High | GH #9 (from #8) |
| R2 | ✅ **Done (PR #16)** — **`spawn_into(supervisor_name, …)`** public cross-tree spawn helper | 🟡 Medium | GH #10 |
| R3 | ✅ **Done (PR #18)** — **First-party JWT auth** (opt-in `civitas[jwt]`) + **mTLS** client-cert auth + middleware fail-open fix | 🟡 Medium | v0.6.0 §G5 |
| R4 | ✅ **Done (PR #19)** — **Encrypted `StateStore` at rest** (`civitas[encryption]`) | 🟡 Medium | design/security-hardening.md |
| R5 | ✅ **Done (PR #21)** — **Per-agent spawn quotas** (beyond the global `max_children`) | 🟢 Low | design/dynamic-spawning.md Non-Goals |
| R6 | ✅ **Done (PR #20)** — **Cross-process dynamic spawning** (ZMQ / NATS) | 🟡 Medium | design/dynamic-spawning.md Non-Goals |
| R7 | **Bus-native streaming primitive** (`AgentProcess.stream()`, agent-to-agent, no transport change) | ✅ Done (v0.7.1) | [#22](https://github.com/civitas-io/python-civitas/pull/22) · [bus-native-streaming.md](design/bus-native-streaming.md) |
| R8 | **WS/gRPC gateway auth** — JWT (`Sec-WebSocket-Protocol` subprotocol) and gRPC (`ServerInterceptor` + mTLS transport wiring, Health/Reflection carve-out) auto-inherit from the existing HTTP JWT/mTLS config; fail-closed startup validations for the new insecure-config combinations. | ✅ Done (v0.7.2) | [#17](https://github.com/civitas-io/python-civitas/issues/17) · [gateway-ws-grpc-auth.md](design/gateway-ws-grpc-auth.md) |
| R9 | **HTTP mTLS via trusted reverse proxy** — `require_client_cert` was always non-functional against uvicorn (never populates the ASGI TLS extension); new opt-in `mtls_source="proxy_header"` trusts an RFC 9440 `Client-Cert` header from a trusted-CIDR proxy instead, feeding the unchanged DN-allowlist authorizer. `direct` mode (default) is unchanged, still non-functional. | ✅ Done (v0.7.3) | [#25](https://github.com/civitas-io/python-civitas/issues/25) · [gateway-http-mtls-proxy.md](design/gateway-http-mtls-proxy.md) |

**Suggested cut line:** R1–R2 (spawn follow-ups) are the headline; R3 (auth) + R4 (encrypted store)
are strong companions; R5–R9 are opportunistic and can slip to a later patch.

---

## Part 2 — Backlog

**Status: 🗂️ Tracked** — the active todo list: everything not yet done. New work lands here first
(a design doc if warranted), then moves into [Part 1 — Shipped](#part-1--shipped) once released.
Owner column: `core` = python-civitas, else the target repo.

### Now open — tracked issue (python-civitas)

| # | Issue | Severity | Area |
|---|-------|----------|------|
| [#28](https://github.com/civitas-io/python-civitas/issues/28) | Supervisor escalation is a no-op under ONE_FOR_ONE — escalated subtree never restarts | 🔴 P0 | supervision core ([design](design/supervision-hardening.md) A2) |
| [#29](https://github.com/civitas-io/python-civitas/issues/29) | Crash-restart re-registration drops capabilities/metadata (Supervisor + Worker) | 🔴 P0 | supervision core (A3) |
| [#30](https://github.com/civitas-io/python-civitas/issues/30) | Failed restarts are silent — crash-handler exceptions never retrieved; crash handling unserialized | 🔴 P0 | supervision core (B2) |
| [#31](https://github.com/civitas-io/python-civitas/issues/31) | Heartbeats at priority 0 — busy/suspended remote agents falsely declared crashed | 🔴 P1 | supervision core (A6) |
| [#32](https://github.com/civitas-io/python-civitas/issues/32) | `ErrorAction.RETRY` re-enqueues at back of mailbox — FIFO ordering broken | 🟡 P2 | process loop (C1) |
| [#33](https://github.com/civitas-io/python-civitas/issues/33) | `sender="_runtime"` is unroutable — replies via `message.sender` crash | 🟡 P2 | runtime API (C4) |
| [#34](https://github.com/civitas-io/python-civitas/issues/34) | `register_b64` pollutes routing table with phantom entries | 🟡 P2 | registry (C5) |
| [#35](https://github.com/civitas-io/python-civitas/issues/35) | README drift: provider extras / adapter imports contradict AGENTS.md + pyproject | 🟡 P2 | docs (C10) |
| [#27](https://github.com/civitas-io/python-civitas/issues/27) | `on_stop()` exceptions during normal shutdown aren't contained | — | process lifecycle |
| [#26](https://github.com/civitas-io/python-civitas/issues/26) | MCP client lacks Streamable HTTP transport | — | mcp / fabrica |

> #28–#31 are tracked in code by strict-xfail regression tests in
> `tests/unit/test_actor_model_gaps.py` — fixing one flips its test to XPASS and fails the suite
> until the marker is removed. #26–#35 (except #26) are scheduled for **v0.8.0** below.

### v0.8.0 — Supervision Core Hardening (Planned)

**Status: ⏳ Planned** — scoped from the 2026-07-21 actor-model architecture review.
Design: [`supervision-hardening.md`](design/supervision-hardening.md). Theme: *make the advertised
OTP guarantee true* — v0.1–v0.7 built the runtime outward (transports, spawning, gateway,
security); v0.8.0 turns inward and fixes the supervision core those layers stand on. Prerequisite
for the Medicus self-healing demo (restart-as-remediation needs trustworthy restarts).

| # | Deliverable | Priority | Source |
|---|-------------|----------|--------|
| H1 | ✅ **Done (dev/v0.8.0, `e758f72`)** — **Escalation restarts the escalated subtree** under ONE_FOR_ONE; budget window cleared on supervisor restart (fresh incarnation rule) | 🔴 P0 | [#28](https://github.com/civitas-io/python-civitas/issues/28) · design D2 |
| H2 | ✅ **Done (dev/v0.8.0, `e758f72`)** — **Serialized, observable crash handling** — per-supervisor crash queue (strictly sequential, stale-incarnation skip); restart failures logged at ERROR + escalated via the parent's queue; crash-drop window closed | 🔴 P0 | [#30](https://github.com/civitas-io/python-civitas/issues/30) · design D4 |
| H3 | ✅ **Done (dev/v0.8.0, `e758f72`)** — **Registration snapshot preserved across restart** — `reregister_preserving()` at 3 Supervisor paths + Worker | 🔴 P0 | [#29](https://github.com/civitas-io/python-civitas/issues/29) · design D3 |
| H4 | ✅ **Done (dev/v0.8.0)** — **Priority heartbeats (stopgap)** — `_agency.heartbeat` at `priority=1` (busy agents ack between messages; suspended agents ack while staying suspended); threshold breaches enqueue on the crash queue so restart backoff no longer stalls the monitor loop | 🔴 P1 | [#31](https://github.com/civitas-io/python-civitas/issues/31) · design D5 |
| H5 | ✅ **Done (dev/v0.8.0, staged (b) per §2.1)** — **Restart state reset**: `self.state` reset before checkpoint restore (only checkpointed state survives — suspend marker S7 intact); ctor-capture groundwork (`__new__` records the child spec) for v0.9 fresh-instance restart; AGENTS.md contract finalized | 🔴 P1 | design D1 (A1) · xfail tests |
| H6 | ✅ **Done (dev/v0.8.0)** — **Opt-in `handle_timeout` watchdog** — a hung async `handle()` becomes a visible crash through the normal `on_error` path; YAML per-agent; span attr for hung-vs-buggy triage | 🔴 P1 | design D5 (A7) |
| H7 | ✅ **Done (dev/v0.8.0)** — **`on_stop()` exception containment** during normal shutdown — contained + ERROR-logged, agent reaches STOPPED, shutdown completes | 🟡 P1 | [#27](https://github.com/civitas-io/python-civitas/issues/27) |
| H8 | ✅ **Done (dev/v0.8.0)** — `ErrorAction.RETRY` retries **in place** — FIFO preserved, fresh `handle_timeout` per attempt, STOP aborts mid-retry; `RETRY_AFTER` delay-lane idea recorded as deferred (design §5) | 🟡 P2 | [#32](https://github.com/civitas-io/python-civitas/issues/32) · design D8 |
| H9 | ✅ **Done (dev/v0.8.0)** — `_runtime` sink (bare subscription): WARNING + drop for send, fail-fast error reply for ask; glob patterns exclude `_`-prefixed system names (C6 slice of H13) | 🟡 P2 | [#33](https://github.com/civitas-io/python-civitas/issues/33) · design D8 |
| H10 | ✅ **Done (dev/v0.8.0)** — `LocalRegistry.register_b64` deleted (zero callers; keys live in `KeyRegistry` only) | 🟡 P2 | [#34](https://github.com/civitas-io/python-civitas/issues/34) · design D8 |
| H11 | ✅ **Done (dev/v0.8.0)** — Truth sweep: README + getting-started + index + plugins (extras/imports → civitas-contrib); 5 orphaned guide pages + security docs added to site nav | 🟡 P2 | [#35](https://github.com/civitas-io/python-civitas/issues/35) |
| H12 | ✅ **Done (dev/v0.8.0)** — "Delivery semantics & hazards" in messaging.md; restart-contract + escalation + handle_timeout + suspension sections in supervision.md; NEW `recipes.md` (when-to-use decision guide) + `agents-guide.md` + `llms.txt` (coding-agent-consumable docs) | 🟡 P2 | design D7 (A4/A5/A8/C8) |
| H13 | Hygiene batch — monotonic clocks for windows/TTL, broadcast glob excludes `_agency.*`, bus accessor methods (encapsulation) | 🟢 Stretch | design D8 (C2/C6/C7) |

**Exit criteria:** all six strict-xfail tests in `tests/unit/test_actor_model_gaps.py` converted to
plain passing tests (each Hn fix flips its test in the same PR); H2 adds a concurrent-crash stress
test (N children crashing in the same tick under ONE_FOR_ALL → exactly one restart cycle).

**Suggested cut line:** H1–H4 (the P0/P1 correctness cluster) are the headline and must ship
together; H5–H7 are strong companions; H8–H13 are opportunistic and can slip to v0.8.x patches.

**Explicitly deferred to v0.9+:** D6 (unify static `Supervisor` + `DynamicSupervisor` into one
actor-based engine — needs its own design/plan cycle), D5-structural (per-process out-of-band
liveness), C3 (in-process serialization fast path), B4 (DynSup `wait=True` head-of-line move).

### Active backlog (python-civitas, no issue yet)

| Item | Priority | Status | Where tracked |
|------|----------|--------|----------------|
| ~~Supervision hardening D1/D5-watchdog/D7/D8~~ → scheduled into [v0.8.0](#v080--supervision-core-hardening-planned) (H5, H6, H12, H8–H10, H13) | — | ⏳ scheduled | design/supervision-hardening.md |
| **Supervision hardening D5 (structural) — per-process out-of-band liveness** (beyond the v0.8.0 `handle_timeout` watchdog) | 🔴 High | ⏸️ deferred to v0.9+ | design/supervision-hardening.md D5 (A6/A7) |
| **Supervision hardening D6 — unify static Supervisor + DynamicSupervisor as one actor-based engine** | 🟡 Medium | 💡 needs own plan (v0.9+) | design/supervision-hardening.md D6 (B1) |
| DynamicSupervisor: move `wait=True` readiness wait off the message loop (head-of-line blocking) | 🟡 Medium | ⏸️ | design/dynamic-spawning.md addendum (B4) |
| Restart accounting: reconcile supervisor-wide budget window vs per-child lifetime backoff counts | 🟢 Low | ⏸️ | design/supervision-hardening.md B3 |
| R7 follow-up: credit-based stream backpressure (namespace `civitas.stream.credit` reserved) | 🟢 Low | ⏸️ opportunistic | design/bus-native-streaming.md §8 Q5 |
| R7 follow-up: immediate `StreamInterrupted` on producer loss (bidirectional producer→sink index; today bounded by `idle_timeout`) | 🟢 Low | ⏸️ opportunistic | design/bus-native-streaming.md D6 |
| Textual dashboard rebuild (on `TopologyServer` endpoints) | 🟡 Medium | ⏸️ follow-on | design/dynamic-spawning.md |
| [Visual Topology Editor](#m41-visual-topology-editor) (drag-drop UI) | 🟢 Low | ⏸️ low priority | §M4.1 |
| Fine-grained ACL DSL (overlaps M4.4 capabilities) | 🟡 Medium | ⏸️ | design/security-hardening.md |
| HSM / TPM-backed signing keys | 🟢 Low | ⏸️ post-v1.0 | design/security-hardening.md |
| PKI / CA integration (cert issuance) | 🟢 Low | ⏸️ deployment-layer | design/security-hardening.md |
| Durable suspension: fail-fast `ask()` into a suspended agent (times out today) | 🟡 Medium | ⏸️ | design/durable-suspension.md Non-Goals |
| Durable suspension: crash-while-suspended restart-budget exemption | 🟡 Medium | ⏸️ v1 | supervisor.py (S8 finding #5) |
| Fiddler eval exporter: two-way guardrail receive | 🟢 Low | ⏸️ | §M2.6 |
| Postgres: zero-downtime dual-write migration | 🟢 Low | ⏸️ | §Postgres StateStore |
| Postgres: PgBouncer deployment guide | 🟢 Low | ⏸️ docs pass | §Postgres StateStore |
| External security audit before v1.0 (fix all HIGH+; publish summary) | 🟡 Medium | ⏳ pre-v1.0 | §M4.3 |
| Continuous security posture (CVE watch, CVSS advisories) | 🟡 Medium | ⏳ ongoing | §M4.3 |

### Ideas (not yet specced)

| Item | Priority | Where tracked |
|------|----------|----------------|
| **Medicus self-healing hero demo** (P0+P1: detect → diagnose → verified PR) — flagship example; supersedes the Telegram personal assistant (which drops to a minor gateway+skills sample) | 🟡 Medium | design/medicus-demo.md |
| **Self-healing / autonomous remediation agent** — monitor (metrics/audit/OTEL/crash) → diagnose (LLM) → sandbox-verify → canary-deploy → auto-rollback, under staged autonomy + safety gates | 🟡 Medium | design/self-healing.md |
| Worker-level **restart-with-new-code** (blue-green drain) — the deploy primitive enabling self-healing & near-zero-downtime code updates (Python has no safe in-place reload) | 🟡 Medium | design/self-healing.md |

### Other repos (tracked for visibility only — per [`boundary.md`](https://github.com/civitas-io/context))

| Item | Owner | Status | Where tracked |
|------|-------|--------|---------------|
| `CivitasMCPServer` — expose an agent tree as an MCP server | fabrica | ⏸️ not started anywhere | §M3.4 |
| CrewAI adapter — full implementation (stub raises `NotImplementedError` today) | civitas-contrib | ⏳ stub | §Infrastructure & Release |
| MySQL StateStore | civitas-contrib | ⏸️ | §Postgres StateStore |
| [Prompt Library & Playground](#prompt-library--playground) | civitas-contrib | 💡 idea (🔴 high), spec unwritten | §Phase 5 |
| [Skills Gateway](#skills-gateway) | civitas-contrib | 💡 idea, spec unwritten | §Phase 5 |
| [Fabrica — Tools Gateway](#fabrica--tools-gateway) / `find_tools` (RFC 0001) | civitas-forge | 💡 idea (🔴 high), spec unwritten | §Phase 5, rfc/0001 |
| [LLM Gateway](#llm-gateway) (governed: rate limits, budgets, grant routing) | presidium | ⏸️ moved | §Phase 5 |
| Credential-propagation RFC (per-user OAuth for retrieved tools) | cross-repo | ⏸️ future RFC | rfc/0001 §out-of-scope |

**Recently shipped** (moved out of this backlog; see [Part 1](#part-1--shipped) for detail):
cross-process dynamic spawning (#20), per-agent spawn quotas (#21), encrypted `StateStore` at rest
(#19), first-party JWT + mTLS gateway auth for HTTP (#18), non-blocking dynamic spawn +
`spawn_into()` (#14, #16), bus-native streaming (#22), WS/gRPC gateway auth (#17), HTTP mTLS via
reverse proxy (#25).

---

### M4.1 — Visual Topology Editor

**Status: ⏸️ Deferred | Priority: 🟢 Low**

Web-based drag-and-drop editor for designing agent topologies visually.

| Deliverable | Status |
|-------------|--------|
| Drag-and-drop agent/supervisor canvas | ⏸️ |
| Visual message flow connections | ⏸️ |
| Supervision strategy configuration via UI | ⏸️ |
| Export to valid Civitas topology YAML | ⏸️ |
| Round-trip: imported YAML renders correctly | ⏸️ |

---

## Phase 5 — Agentic Platform

Civitas provides the runtime primitives. Governance lives in [Presidium](https://github.com/civitas-io/presidium) — an interface library that defines governance protocols (PolicyEngine, AgentRegistry, CredentialProvider, etc.) with lightweight defaults (CEL policy engine, in-memory registry) in the core package, and adapters for existing products (OPA, Vault, LiteLLM) plus reference implementations for novel components in presidium-contrib.

Presidium follows the same pattern as Civitas: protocols in core, implementations in contrib. Every component works as an in-process library (single-process deployments) or as a service (distributed deployments via Civitas GenServers or standalone HTTP). See [Civitas-Presidium Boundary](design/civitas-presidium-boundary.md) for the full architecture.

The items below are ideas across the wider Civitas product line that complement Presidium's governance layer. **Most of them are not python-civitas's job** — see the "Lives in" callout on each. Per [`boundary.md`](https://github.com/civitas-io/context) (2026-05-08), Prompt Library and Skills Gateway are civitas-contrib ("Dev tooling" / "skills routing layer"), same as Fabrica is its own repo. This file previously implied otherwise; corrected July 2026.

---

### Prompt Library & Playground

**Status: 💡 Idea — to be specced | Priority: 🔴 High | Lives in: civitas-contrib, not python-civitas**

> **Correction (July 2026):** Originally framed here as a "Civitas-side feature." `boundary.md`
> lists "Prompt library" under civitas-contrib ownership ("Dev tooling"). If built, `PromptStore`
> would be a `civitas_contrib`-namespaced `GenServer` subclass depending on this repo's `GenServer`
> base class — not a `civitas/` module itself. The `civitas playground` CLI reference below would
> need to become a civitas-contrib CLI plugin or a documented pattern, not a core subcommand.

Prompts as first-class versioned entities, stored and served by a supervised `PromptStore(GenServer)`. Agents load instructions by name rather than hardcoding strings — prompt changes never require a code deploy. The playground (CLI + dashboard tab) lets you test a prompt version against a live agent before promoting it.

This is one of the strongest SaaS upgrade stories: the OSS `PromptStore` runs in your deployment; a hosted version adds a web UI for non-engineers, team collaboration, cross-deployment promotion, and output analytics.

| Idea | Notes |
|------|-------|
| `PromptStore(GenServer)` — versioned prompt storage on the bus | Agents call `call("prompt_store", {"agent": "assistant", "slot": "system"})` |
| SQLite backend (runtime-mutable) + YAML dir backend (git-tracked) | User chooses per deployment |
| Named version aliases — `latest`, `stable`, `experimental` | Pinned per agent per environment in topology YAML |
| Per-agent, per-slot prompt mapping | Each agent can have multiple slots: `system`, `few_shot`, `tools` |
| Hot-swap support — reload prompt without restarting agent | Agent subscribes to prompt update events |
| `civitas playground` CLI — interactive session with a specified prompt version | Test against live runtime before promoting |
| Dashboard tab — side-by-side prompt diff, test messages, output comparison | Lightweight eval harness backed by EvalLoop (M2.5) |
| A/B traffic splitting between prompt versions | Random split; metrics tracked via OTEL spans |
| SaaS layer — web UI, team collaboration, cross-deployment promotion, analytics | `design/prompt-library.md — to be written` |
| Spec | design/prompt-library.md — to be written |

---

### LLM Gateway

**Status: ⏸️ Moved to Presidium**

Model routing *without* governance (multi-provider fallback for reliability) is a thin Civitas utility — `CompositeModelProvider`. It is not a full gateway.

The full governed LLM gateway — per-agent rate limits, cost tracking, budget enforcement, grant-based provider routing — belongs in Presidium. It is implemented via the `GovernedModelProvider` protocol in the `presidium` core package, with the `LiteLLMProxyAdapter` and `PortkeyAdapter` available in `presidium-contrib`. It wraps any Civitas `ModelProvider` via the plugin protocol and enforces governance policy before delegating to the underlying provider.

Civitas provides the `ModelProvider` protocol (integration point 2 for Presidium). Civitas does not provide rate limiting, budgets, or grant-based routing — those are governance concerns.

**Residual Civitas utility:** `CompositeModelProvider` — a simple ordered fallback chain (primary → fallback) for reliability. No governance, no per-agent tracking. Infrastructure, not governance.

See [Presidium](https://github.com/civitas-io/presidium) for the governed implementation (`GovernedModelProvider` in core, `LiteLLMProxyAdapter` in contrib).
See [docs/design/civitas-presidium-boundary.md](design/civitas-presidium-boundary.md) for the full boundary definition.

---

### Fabrica — Tools Gateway

**Status: 💡 Idea — to be specced | Priority: 🔴 High**

**Product:** Fabrica (`pip install fabrica`) — lives in `civitas-io/civitas-forge`, not in python-civitas.

Fabrica solves the tool schema token problem: passing all tool schemas to every LLM call is token-expensive and degrades selection accuracy beyond ~20–30 tools. Instead of N schemas, the LLM receives one `find_tools(query)` meta-tool and retrieves only the schema it needs.

Fabrica aggregates tool sources (local ToolStore, MCP servers, Composio, custom), serves a unified namespace, and exposes a retrieval interface. Civitas agents connect to it as a tool source — any other LLM framework can too.

**Dependency chain:** M3.4 (MCP plumbing) → M4.4 (ToolStore) → Fabrica (retrieval)

See RFC 0001 (`docs/rfc/0001-tool-retrieval.md`) for the formal problem statement and proposed interface standard.

| Idea | Notes |
|------|-------|
| `find_tools(query)` meta-tool — one schema sent to LLM, not N | Keyword backend (default) + embedding backend (`fabrica[search]`) |
| Tool source aggregation — local ToolStore, MCP servers, Composio, custom | Pluggable `ToolSource` protocol |
| Unified tool namespace across all sources | `gateway://source/tool_name` address scheme |
| Per-source credential isolation | Each source has its own auth config; agents never see other sources' secrets |
| Tool call sandboxing | Filesystem + network isolation for untrusted tool execution |
| Health monitoring + circuit breaker per source | Unhealthy sources removed from routing automatically |
| MCP-compatible interface | Fabrica itself exposes `list_tools` + `call_tool` — any MCP client can connect |
| Civitas integration — `ToolSource` plugin pointing at Fabrica | `civitas[fabrica]` extra |
| SaaS upgrade path — hosted Fabrica with team tool registry, analytics | Future |
| Spec | `civitas-forge/packages/fabrica/` — to be created |

---

### Skills Gateway

**Status: 💡 Idea — to be specced | Priority: 🟡 Medium | Lives in: civitas-contrib, not python-civitas**

> **Correction (July 2026):** Originally framed here as a "Civitas-side feature." `boundary.md`
> lists "Skills gateway" under civitas-contrib ownership ("skills routing layer"). It would
> *consume* this repo's Capability-Aware Registry (M4.4) and `MessageBus` as a dependency, the
> same way civitas-contrib's provider plugins and adapters consume civitas core today — it would
> not be implemented inside `civitas/`.

A supervised registry of composable agent workflows — "skills" — that can be discovered and invoked by name or capability. A skill is a named, versioned sequence of tool calls, LLM steps, or sub-agent invocations exposed as a single callable unit on the bus.

Extends the Capability-Aware Registry (M4.4): where M4.4 answers "which agent can do X?", the Skills Gateway answers "invoke skill X, wherever it runs."

| Idea | Notes |
|------|-------|
| `@skill` decorator — declare a reusable workflow on any agent | Versioned, named, queryable by capability tags |
| Skill discovery by capability / input type | `gateway.find_skill("summarise", input_type="text/html")` |
| Cross-agent skill composition | Skills can invoke other skills; gateway handles routing |
| Skill versioning with semver + forward compatibility | Old callers work when a skill is upgraded |
| Local + remote skill sources | Skills can live in the local registry or a remote Civitas deployment |
| Hosted skills marketplace | Future SaaS layer — shared skills across organisations |
| Spec | design/skills-gateway.md — to be written |
