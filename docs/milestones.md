# Milestones

Development progress across all phases of Civitas.

---

## Status legend

| Symbol | Status |
|--------|--------|
| ✅ | Completed |
| 🔄 | In Progress |
| ⏳ | Planned |
| ⏸️ | Deferred |
| 💡 | Idea — to be specced |

---

## Overview

| Phase | Milestone | Status | Completed |
|-------|-----------|--------|-----------|
| 1 | [Core Runtime](#phase-1-core-runtime) | ✅ Completed | Mar 2026 |
| 2 | [Ecosystem — Transports](#m21-zmq-multi-process-transport) | ✅ Completed | Mar 2026 |
| 2 | [Ecosystem — Observability](#m23-otel-observability) | ✅ Completed | Apr 2026 |
| 2 | [Ecosystem — EvalLoop (local)](#m25-evalloop) | ✅ Completed | Apr 2026 |
| 2 | [Ecosystem — Remote Eval Exporters](#m26-remote-eval-exporters) | ✅ Completed | Apr 2026 |
| 3 | [Developer Experience — CLI & Dashboard](#phase-3-developer-experience) | ✅ Completed | Mar 2026 |
| 3 | [Developer Experience — MCP Integration](#m34-mcp-integration) | ✅ Completed | Apr 2026 |
| 3 | [Developer Experience — GenServer](#m35-genserver) | ✅ Completed | Apr 2026 |
| — | [Infrastructure & Release](#infrastructure--release) | ✅ Completed | Apr 2026 |
| 4 | [Dynamic Agent Spawning](#m41b-dynamic-agent-spawning) | ✅ Completed | Apr 2026 |
| 4 | [Security Hardening](#m42-security-hardening) | ✅ Completed | May 2026 |
| 4 | [Codebase Security & Enterprise Posture](#m43-codebase-security--enterprise-posture) | ✅ Completed | Apr 2026 |
| 4 | [Capability-Aware Registry](#m44-capability-aware-registry) | ✅ Completed | May 2026 |
| 4 | [HTTP Gateway](#http-gateway) | ✅ Completed | Apr 2026 |
| 4 | [Gateway API Surface](#gateway-api-surface) | ✅ Completed | Apr 2026 |
| 4 | [Postgres StateStore + Migration](#postgres-statestore--migration) | ✅ Completed | May 2026 |
| 4 | [Visual Topology Editor](#m41-visual-topology-editor) | ⏸️ Deferred | — |
| 5 | [Prompt Library & Playground](#prompt-library--playground) | 💡 Idea | v0.5+ |
| 5 | [LLM Gateway](#llm-gateway) | ⏸️ Moved to Presidium | — |
| 5 | [Fabrica — Tools Gateway](#fabrica--tools-gateway) | 💡 Idea | v0.5+ |
| 5 | [Skills Gateway](#skills-gateway) | 💡 Idea | v0.5+ |

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
| M1.8 | Personal AI Assistant demo (Telegram gateway + skill agents) | 🟡 Medium | ⏸️ Deferred |

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

**Status: ✅ Completed — April 2026**

MCP protocol plumbing — the wire layer between Civitas agents and MCP tool servers. Agents call tools by direct address (`mcp://server/tool`); the runtime handles handshake, transport, schema negotiation, and tracing. Agents also expose themselves as MCP servers so external LLM clients can discover and call them.

**Scope:** protocol wire layer only. Connection pooling, circuit breakers, unified tool namespacing, and semantic retrieval are **not** in scope — they belong to Fabrica. See [design spec](design/mcp-integration.md).

**Dependency chain:** M3.4 → M4.4 (ToolStore) → Fabrica (pooling + retrieval)

| Deliverable | Status |
|-------------|--------|
| `civitas[mcp]` optional extra — `mcp>=1.0` dependency | ✅ |
| `MCPClient` — connect (stdio + SSE), `list_tools`, `call_tool`, persistent session via `AsyncExitStack` | ✅ |
| `MCPTool(ToolProvider)` — `mcp://server_name/tool_name` name scheme | ✅ |
| `AgentProcess.connect_mcp()` — connect + auto-register tools into `self.tools`; idempotent | ✅ |
| `self.tools.get("mcp://server/tool")` resolves to the registered `MCPTool` | ✅ |
| `MCPTool.execute()` emits `civitas.mcp.call` OTEL span | ✅ |
| `CivitasMCPServer(GenServer)` — deferred to Fabrica (scope boundary decision) | ⏸️ |
| Topology YAML `mcp.servers` block — auto-connect at agent startup | ✅ |
| 23 unit tests | ✅ |

**Explicitly out of scope for M3.4:**
- Connection pooling / persistent sessions — Fabrica (`MCPToolSource`)
- Circuit breakers per server — Fabrica
- Semantic or keyword tool retrieval (`find_tools`) — Fabrica
- Unified cross-agent tool namespace — M4.4 ToolStore
- Per-agent credential isolation — M4.2 Security Hardening

#### Implementation checklist

Ordered tasks — each step is independently mergeable.

1. **Package setup**
   - [ ] `civitas/mcp/__init__.py` — package stub
   - [ ] `civitas/mcp/types.py` — `MCPServerConfig` (name, transport, command/args/env/url), `MCPToolSchema`
   - [ ] `civitas[mcp]` extra in `pyproject.toml` — `mcp>=1.0`

2. **MCP client**
   - [ ] `civitas/mcp/client.py` — `MCPClient.__init__(config: MCPServerConfig)`
   - [ ] `MCPClient.list_tools()` — stdio transport: open subprocess session, call `list_tools`, close
   - [ ] `MCPClient.list_tools()` — SSE transport: open HTTP session, call `list_tools`, close
   - [ ] `MCPClient.call_tool(name, arguments)` — stdio transport
   - [ ] `MCPClient.call_tool(name, arguments)` — SSE transport

3. **MCPTool**
   - [ ] `civitas/mcp/tool.py` — `MCPTool(ToolProvider)` wrapping `MCPClient` + `MCPToolSchema`
   - [ ] `MCPTool.name` returns `mcp://server_name/tool_name`
   - [ ] `MCPTool.schema` returns the JSON Schema from the MCP tool definition
   - [ ] `MCPTool.execute(**kwargs)` calls `client.call_tool()` and returns result
   - [ ] `MCPTool.execute()` emits `civitas.mcp.call` OTEL span (attributes: server, tool, transport)

4. **AgentProcess integration**
   - [ ] `AgentProcess.connect_mcp(config)` — creates `MCPClient`, calls `list_tools`, registers each as `MCPTool` in `self.tools`
   - [ ] `connect_mcp()` is idempotent: deregisters existing tools for the same server before re-registering
   - [ ] `self.tools.get("mcp://github/create_issue")` resolves correctly via registered name

5. **MCP server exposure**
   - [ ] `civitas/mcp/server.py` — `CivitasMCPServer(GenServer)`
   - [ ] `CivitasMCPServer.init()` — starts MCP stdio server in background task via `mcp.Server`
   - [ ] `list_tools` handler — returns schemas from injected `ToolRegistry`
   - [ ] `call_tool` handler — calls the matching `MCPTool.execute()` or raises `ToolNotFoundError`

6. **Topology YAML support**
   - [ ] Runtime loader reads `mcp.servers` block, creates `MCPServerConfig` instances
   - [ ] Agents auto-connect configured servers during startup (before first message)
   - [ ] `mcp.expose.enabled: true` starts `CivitasMCPServer` as a supervised child
   - [ ] `civitas topology validate` accepts `mcp:` section without errors

7. **Tests (≥ 10 unit, ≥ 2 integration)**
   - [ ] `MCPServerConfig` validation (missing transport fields, unknown transport)
   - [ ] `MCPTool.name` follows `mcp://` scheme
   - [ ] `MCPTool.schema` returns correct JSON Schema
   - [ ] `MCPTool.execute()` calls `client.call_tool()` with correct args
   - [ ] `MCPTool.execute()` emits OTEL span
   - [ ] `connect_mcp()` registers tools in `self.tools`
   - [ ] `connect_mcp()` deregisters old tools on reconnect (idempotency)
   - [ ] `self.tools.get("mcp://server/tool")` returns correct tool
   - [ ] `CivitasMCPServer` `list_tools` returns all registered tools
   - [ ] `CivitasMCPServer` `call_tool` routes to correct tool
   - [ ] Integration: agent connects to real stdio MCP echo server, calls a tool
   - [ ] Integration: `CivitasMCPServer` handles `list_tools` request from real MCP client

8. **Release**
   - [ ] `CHANGELOG.md` entry under `## [0.3.0]`
   - [ ] Example: `examples/mcp_agent.py` — agent connecting to a stdio MCP server
   - [ ] `mkdocs.yml` nav updated with MCP integration design doc

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
| Framework adapters: LangGraph, OpenAI Agents SDK | ✅ | Mar 2026 |
| Framework adapters: CrewAI | ⏳ | — |

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
   - [x] Execution order: global → route-scoped → contract validation → bus dispatch
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
   - [x] Global middleware runs before route-scoped middleware
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

**Status: ✅ Completed — May 2026 | Priority: 🔴 High**

SQLite works for single-process deployments but breaks under concurrent cross-process writes (ZMQ Level 2+, NATS Level 3). `PostgresStateStore` extends the `StateStore` protocol — switching backends is a topology YAML change with no agent code changes.

| Deliverable | Status |
|-------------|--------|
| `StateStore` protocol extended with `list_agents()` and `close()` | ✅ |
| `InMemoryStateStore.list_agents()` / `close()` | ✅ |
| `PostgresStateStore` — `asyncpg` backend, connection pool, lazy init | ✅ |
| `civitas_agent_state` table — JSONB, upsert, `updated_at` timestamp | ✅ |
| Pool config via topology YAML — `min_size`, `max_size`, `timeout` | ✅ |
| `civitas[postgres]` optional extra — `asyncpg>=0.29` | ✅ |
| Plugin loader entry: `type: postgres` | ✅ |
| `@runtime_checkable StateStore` — `isinstance()` checks work | ✅ |
| `civitas state migrate <src> <dst>` — dry-run by default, `--execute` to apply | ✅ |
| `_parse_dsn()` — `sqlite:<path>`, `.db`/`.sqlite` extension, `postgresql://` URL | ✅ |
| Helpful `ImportError` with install hint if `asyncpg` not installed | ✅ |
| 20 unit tests covering protocol, PostgresStateStore (mocked), and migrate CLI | ✅ |
| Zero-downtime dual-write migration | ⏸️ Deferred — maintenance-window copy is sufficient for v0.4 |
| PgBouncer deployment guide | ⏸️ Deferred to docs pass |
| MySQL StateStore (`aiomysql`/`asyncmy` backend) | ⏸️ Deferred — see below |

> **MySQL StateStore** — deferred because Postgres covers the multi-process persistence gap and asyncpg is a better async foundation. Add if users are already running MySQL and cannot introduce a second database. Implementation is a clean 100-line addition following the same plugin pattern (`civitas[mysql]` extra, `type: mysql` loader entry, `mysql://` DSN in `_parse_dsn`).

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

The items below are Civitas-side features that complement Presidium's governance layer.

---

### Prompt Library & Playground

**Status: 💡 Idea — to be specced | Priority: 🔴 High**

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

**Status: 💡 Idea — to be specced | Priority: 🟡 Medium**

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
