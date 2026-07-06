# Design: Civitas–Presidium Boundary

> The definitive reference for what Civitas provides to Presidium, what it explicitly does not do, and where the boundary between the two layers sits.

**Status:** Authoritative (revised 2026-05-05)
**Companion:** [Presidium RFC-001](https://github.com/civitas-io/presidium/blob/main/docs/rfcs/001-presidium-scope.md)

---

## The One-Line Separation

> **Civitas:** Run agents reliably.
> **Presidium:** Run agents accountably.

These are additive. A customer never chooses between a Civitas feature and a Presidium feature for the same job. Civitas is complete and useful without Presidium. Presidium is meaningless without Civitas.

---

## What Civitas Provides

### Runtime Primitives

- `AgentProcess` — base class for all agents; mailbox, lifecycle hooks, supervision integration
- `Supervisor` — fault-tolerant supervision trees; ONE_FOR_ONE, ONE_FOR_ALL, REST_FOR_ONE
- `MessageBus` — message routing, backpressure, ephemeral topics, request-reply
- `StateStore` — pluggable persistence protocol (InMemory, SQLite, Postgres)
- Transports — InProcess, ZMQ (multi-process), NATS (distributed), with mTLS between nodes
- `Runtime` — topology loading, plugin wiring, lifecycle orchestration

### Observability & Eval

- OTEL tracing — automatic spans for agent actions, tool calls, LLM calls
- `AuditSink` — structured audit event emission pipeline (Civitas emits; Presidium enriches and exports)
- `EvalLoop` — agent self-correction signals; internal quality feedback for agent reasoning
- `ExportBackend` protocol — pluggable telemetry export (Presidium implements Fiddler, Arize, Langfuse exporters)

### Plugin Interfaces

- `ModelProvider` — LLM calling interface; implementations: Anthropic, OpenAI, Gemini, Mistral, LiteLLM
- `ToolProvider` — tool calling interface; MCP client integration
- `MCPClient` — MCP server connectivity mechanics
- `StateStore` — agent state persistence

### Routing Registry

- **Operational routing registry** — maps agent names to process references and capability routing tags
- **Capability routing tags** — operational tags on `AgentProcess` that drive `send_capable()` routing; what an agent *can handle technically*, not what it is *authorized to do*

### Extension Hooks for Presidium

These are the exact surfaces Presidium uses. Civitas does not know about Presidium — it simply exposes these hooks:

| Hook | Type | What Civitas Provides |
|------|------|-----------------------|
| `RegistryListener` | Callback protocol | Fires on every agent register/deregister; carries agent name + capability tags |
| `ModelProvider` protocol | Plugin interface | `chat(messages, agent_name, **kwargs) → ModelResponse` |
| `ToolProvider` protocol | Plugin interface | Tool call interface over MCP client |
| `AuditSink` | Event pipeline | Structured audit events: agent name, action, message, tool, result |
| `ExportBackend` | Plugin interface | Telemetry export target |
| `EvalLoop` hooks | Attachment point | Presidium can attach governance metrics alongside self-correction signals |
| Credential context injection | `credentials` dict | Passed to each agent at startup; Presidium populates it |
| Durable suspension | `AgentProcess` signal | Agent can suspend awaiting an external resume signal (HITL) |

### Advanced Runtime Features

- `GenServer` — OTP-style stateful service process (supervised long-running services)
- HTTP Gateway — infrastructure edge; maps HTTP requests to agent messages
- EvalLoop — agent self-correction signal infrastructure
- Fabrica (v0.5) — tool namespace, agent-as-tool composition
- Skills Gateway (v0.5) — named composable workflows
- Prompt Library (v0.5) — `PromptStore` GenServer; versioned prompt management

---

## What Civitas Does NOT Provide

These are Presidium concerns. Do not add them to Civitas:

| Not a Civitas concern | Why | Who owns it |
|----------------------|-----|-------------|
| Persistent agent identity | Civitas routing registry is ephemeral (process references); no owner, version, or trust score | `presidium-registry` |
| Agent grants (authorization entitlements) | Capability tags are routing-only; they are not authorization concepts | `presidium-registry` |
| Policy enforcement (ALLOW/DENY/REQUIRE_APPROVAL) | Runtime should run agents; governance decides what they can do | `presidium-policy` |
| Per-agent resource governance | Rate limits, budgets, and cost tracking are governance concerns | `presidium-llm-gateway` |
| Credential vault | OAuth tokens, API keys scoped per `(agent_id, user_id)` — governance, not runtime | `presidium-registry` |
| Token exchange (OBO, XAA) | Requires enterprise IdP integration; governance boundary | `presidium-registry` |
| Enterprise IdP integration | Civitas handles mTLS (transport); token-based auth is Presidium's job | `presidium-registry` |
| HITL approval routing | Approval policy and approver authentication are governance concerns | `presidium-mcp-gateway` |
| Trust scores | Computed from compliance signals; governance concept | `presidium-registry` |
| Compliance reporting | External accountability artifact; not runtime concern | `presidium-audit` |
| Tool ACLs | Access control based on agent grants; governance decision | `presidium-mcp-gateway` |
| Tool poisoning detection | Schema snapshot and change alert — governance concern | `presidium-mcp-gateway` |
| MCP OAuth 2.1 token acquisition | Token acquisition per MCP server endpoint is governance | `presidium-mcp-gateway` |
| Full LLM governance gateway | Rate limits, budgets, grant-based routing are governance | `presidium-llm-gateway` |

---

## Capability Tags vs. Grants — Critical Distinction

**Do not conflate these. They are different concepts at different layers.**

| Concept | Layer | Meaning | Used For |
|---------|-------|---------|----------|
| **Capability tag** | Civitas | What an agent *can handle technically* | Routing: `send_capable("text.summarize")` dispatches to any capable agent |
| **Grant** | Presidium | What an agent is *authorized to access* | Authorization: `tool:database:read` allows the DB tool call |

In code:
- Civitas: `AgentProcess.capabilities: list[str]` — operational routing strings
- Presidium: `AgentRecord.grants: list[str]` — authorization entitlements (e.g. `"tool:database:read"`, `"llm:claude-sonnet"`, `"data:customer_pii:read"`)

An agent may declare `capabilities = ["text.summarize"]` and hold zero grants. These are orthogonal.

---

## The Eight Integration Points

How Presidium attaches to Civitas — the only surfaces that cross the boundary:

| # | Hook | Civitas Provides | Presidium Consumes |
|---|------|-----------------|-------------------|
| 1 | `RegistryListener` | Callback on agent register/deregister | Populates `AgentRecord` in persistent registry |
| 2 | `ModelProvider` protocol | `chat()` interface | `GovernedModelProvider` wraps with rate limits, budgets, grant checks |
| 3 | `ToolProvider` protocol | Tool call interface | `GovernedToolProvider` wraps with ACLs, OAuth, poisoning detection |
| 4 | `AuditSink` | Structured audit event pipeline | Enriches with governance context; exports to external platforms |
| 5 | `ExportBackend` | Telemetry export interface | Presidium implements Fiddler, Arize, Langfuse exporters |
| 6 | `EvalLoop` hooks | Self-correction signal infrastructure | Attaches governance metrics as parallel stream (not replacement) |
| 7 | Credential context injection | `credentials` dict at agent startup | Presidium populates: agent token, vault endpoint, grants |
| 8 | Durable suspension | `AgentProcess` awaits external signal | Presidium HITL service sends the resume signal after approval |

---

## Transport Security vs. Application-Level Auth

**Civitas** handles transport-level security: mTLS between nodes (implemented in M4.2b). Civitas does not validate application-level tokens — it does not know what OAuth is.

**Presidium** handles application-level authentication: token issuance, OBO exchange, credential vault, IdP integration. These operate above the transport layer.

The two are complementary:
- mTLS (Civitas): proves the *process* connecting is who it claims to be at the network level
- OAuth 2.1 Bearer tokens (Presidium): proves the *agent identity* is authorized to perform this *specific action*

---

## CompositeModelProvider — The Residual Civitas Utility

Model routing *without* governance (multi-provider fallback for reliability) is a thin Civitas utility. It does not belong in Presidium because it has no governance semantics.

```python
class CompositeModelProvider:
    """Simple ordered fallback chain. Primary → fallback on failure.
    No per-agent tracking, no rate limits, no budgets. Infrastructure only."""
```

The full governed gateway — per-agent rate limits, cost tracking, budget enforcement, grant-based routing — lives in `presidium-llm-gateway` and wraps any Civitas `ModelProvider` via integration point 2.

---

## Design Principle

Civitas is designed so that governance can be added without modifying it. The extension hooks (`RegistryListener`, plugin protocols, `AuditSink`, credential context, durable suspension) are stable surfaces — Presidium attaches to them without Civitas needing to know Presidium exists.

This is the correct architecture: Civitas provides primitives; Presidium provides policy. Neither layer bleeds into the other's domain.

---

## Presidium Architecture: Interface-First with Dual Deployment Modes

> Decided: June 2026

![Full Stack Layers](../assets/full-stack-layers.svg)

### Interface-First Design

Presidium follows the same pattern as Civitas: **protocols in core, implementations in contrib**.

- `presidium` (pip install presidium) — Protocol definitions only, plus lightweight defaults (CEL policy engine, in-memory registry, env-var credentials). Zero external dependencies beyond Civitas.
- `presidium-contrib` (pip install presidium-contrib[opa,vault,...]) — Adapters for existing products and reference implementations for novel components.

This replaces the earlier 6-package design (presidium-registry, presidium-policy, etc.) with a simpler 2-package structure that mirrors civitas + civitas-contrib.

### CEL as Default Policy Language

The default policy engine uses [CEL (Common Expression Language)](https://github.com/google/cel-spec) — the same expression language used by Kubernetes admission policies and Google Cloud IAM.

CEL is chosen over OPA/Rego because:
- **Embeddable**: Evaluates in-process in microseconds. No sidecar, no HTTP call.
- **Industry direction**: Kubernetes is replacing OPA/Gatekeeper with CEL for admission policies.
- **Simpler**: Single expressions, not a full language with packages and rule heads.
- **Python support**: `cel-python` (Google's reference implementation).

OPA remains available as a `presidium-contrib[opa]` adapter for organisations that already run OPA infrastructure.

### Library Mode vs Service Mode

Every Presidium component works in two deployment modes:

| Mode | How It Works | When to Use |
|------|-------------|-------------|
| **Library** | In-process Python. No network calls. Evaluates directly. | Single-process, dev, small deployments |
| **Service** | Civitas GenServer on the message bus, or standalone HTTP endpoint | Multi-deployment, shared state, central management |

Library mode is the complete, correct implementation — not a degraded subset. Service mode adds shared state and centralised management when the user outgrows in-process.

Example topology configuration:

```yaml
# Library mode — everything in-process
presidium:
  policy:
    type: cel
    rules_path: ./policies/
  registry:
    type: sqlite
    db_path: ./presidium.db

# Service mode — components point to services
presidium:
  policy:
    type: opa
    url: http://policy-service:8181
  registry:
    type: remote
    url: http://registry-service:8080
  credentials:
    type: vault
    url: https://vault.internal:8200
```

### Mapping to Existing Products

Where mature products exist, Presidium wraps them via adapters rather than reimplementing:

| Presidium Interface | Existing Product Adapters | Novel (Reference Impl) |
|---|---|---|
| PolicyEngine | OPA, Cedar | CEL (default, built-in) |
| CredentialProvider | HashiCorp Vault, AWS Secrets Manager | Env/File (default, built-in) |
| GovernedModelProvider | LiteLLM Proxy, Portkey | In-process grant checks (default) |
| ApprovalService | Slack, Temporal, PagerDuty | Callback (default, built-in) |
| AgentRegistry | — | Postgres-backed registry (novel) |
| GovernedToolProvider | — | MCP governance (novel) |
| TrustScorer | — | Rule-based + learning scorer (novel) |
| AuditEnricher | Datadog, Splunk, ELK (via Civitas AuditSink) | In-process enrichment (default) |

### Autonomy Progression

![Autonomy Progression](../assets/autonomy-progression.svg)

Presidium supports a graduated autonomy model:

1. **Full HITL** — All significant actions require human approval. Uses `ApprovalService` protocol.
2. **Heuristic recommendations** — `EvalAgent` subclasses monitor behaviour and send corrections. CEL policies define guardrails.
3. **Learned partial autonomy** — Decision journal records approvals/rejections. Confidence-gated routing auto-approves high-confidence actions.
4. **Full autonomy** — Trust scores, policy compliance history, and learned models enable autonomous operation within governance bounds.

Levels 1-2 work today with Civitas + Presidium interfaces. Level 3 requires the decision journal and confidence routing (M4). Level 4 requires mature trust scoring and policy learning.
