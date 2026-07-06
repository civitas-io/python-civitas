# Design: Runtime Security Hardening (M4.2)

**Status:** Approved — v0.4
**Author:** Jeryn Mathew Varghese
**Last updated:** 2026-05

---

## Motivation

M4.3 produced a STRIDE threat model identifying **10 HIGH** and **8 MEDIUM** risks in the Civitas runtime. Most of those findings explicitly defer their mitigation to M4.2. Today the runtime ships with:

- **No transport encryption.** ZMQ messages traverse loopback as raw msgpack ([civitas/transport/zmq.py:1-284](../../civitas/transport/zmq.py)). NATS connects without client TLS. Cross-host deployments are not safe.
- **No message authentication.** `Message.sender` is a string field set by the sender and never verified ([civitas/messages.py](../../civitas/messages.py)). Any agent can spoof any other agent (Spoofing, HIGH).
- **Plaintext secrets in YAML.** `from_config()` does not substitute env vars; API keys must be hardcoded into topology files or wrapped at the deployment layer ([civitas/runtime.py:100-317](../../civitas/runtime.py)).
- **No per-agent credential scope.** Plugins (LLM providers, tools) read env vars or take config dicts at instantiation; every agent shares the same credentials.
- **No tool sandboxing.** MCP tools are executed as subprocesses with the runtime's full privileges ([civitas/mcp/tool.py:44-67](../../civitas/mcp/tool.py)).
- **No structured audit log.** OTel spans cover observability but are not designed as a tamper-evident audit trail.

M4.2 closes these gaps. It is the implementation arm of the threat model.

---

## Scope

**In scope:** mTLS for ZMQ + NATS, message signing with stable agent identity, env-var secret substitution + per-agent credential scoping, sandboxed tool execution, structured audit log.

**Out of scope (deferred):**

- HSM / TPM-backed keys (post-v1.0)
- Fine-grained ACL DSL (M4.4 Capability-Aware Registry overlaps; revisit once that lands)
- Encrypted `StateStore` at rest (separate effort — can move to v0.5)
- PKI/CA integration (deployment-layer concern; we ship key generation but not certificate issuance)
- HTTPGateway authentication middleware (already a documented integration point; users plug in their own JWT/OIDC)

---

## Design Decisions

### D1 — Security is opt-in per topology, not a global flag

A topology declares a `security:` block. When omitted, the runtime behaves exactly as today (in-process, no signing, no TLS) so existing demos and tests keep working. When present, every applicable layer is hardened.

```yaml
security:
  identity:
    mode: auto                  # auto-generate Ed25519 keypair per agent at startup
    # mode: provisioned         # load from key_dir
    # key_dir: /etc/civitas/keys
  signing:
    enabled: true
    algorithm: ed25519
    require_verification: true  # reject unsigned messages
    allow_unsigned: false       # set true only during rolling upgrades
  transport:
    tls_cert: /etc/civitas/server.crt
    tls_key:  /etc/civitas/server.key
    tls_ca:   /etc/civitas/ca.crt
  audit:
    enabled: true
    sink: file
    path: /var/log/civitas/audit.jsonl
```

Rationale: backwards compatibility is non-negotiable for v0.4. Forcing global migration would break every existing topology. Per-topology opt-in lets production deployments harden without churning the entire ecosystem.

### D2 — Ed25519 keypairs per agent (not HMAC)

Each agent holds an Ed25519 signing keypair. Messages carry `(signer_id, signature)`. Verifiers need only the public key registry, not a shared secret. This matches the agent identity model (each agent is a distinct principal) and avoids the key distribution problem of HMAC (every verifier needs every secret).

| Choice | Why |
|---|---|
| **Ed25519** over HMAC-SHA256 | Public-key model fits the multi-agent topology; verifiers don't hold sender secrets |
| **Ed25519** over RSA/ECDSA | Faster signing/verification, smaller signatures (64 bytes), no parameter pitfalls |
| **PyNaCl** library | Audited, single-purpose, already widely used in distributed systems |

The runtime auto-generates keypairs on first start (`mode: auto`) and stashes them in a configurable key directory. Production deployments use `mode: provisioned` and bring their own keys.

#### Key storage format

Keys use OpenSSH-style filenames (`id_ed25519`, `id_ed25519.pub`) for familiarity, with base64-encoded raw key material:

```
{key_dir}/{agent_name}/
├── id_ed25519       # base64-encoded 32-byte Ed25519 seed (mode 0600)
└── id_ed25519.pub   # base64-encoded 32-byte verify key (mode 0644)
```

The choice of OpenSSH naming (rather than a custom JSON manifest) reflects that:
- Every developer already knows this layout from `ssh-keygen`
- Rotation date, allowed roles, and other metadata are deployment-layer concerns — they belong in a sidecar `.meta.json` or your secrets manager, not in the key file itself
- A custom format would require custom tooling with no ecosystem benefit

#### Key distribution across deployment shapes

Private keys never leave their host. Only public keys (32 bytes each, not sensitive) need to be distributed. The distribution mechanism varies by deployment topology:

| Shape | Distribution mechanism |
|---|---|
| **Single-node (any process count)** | Shared `key_dir` path on the local filesystem. All processes on the same host read and write the same directory. `mode: auto` generates keys on first start; subsequent processes find and use them. |
| **Multi-node, static topology** | Public keys declared inline in `topology.yaml` (see below). The topology is already distributed to every node by the deployment layer (kubectl apply, Ansible, Terraform). Piggyback on that — no additional infrastructure required. |
| **Multi-node, dynamic agents** | The `civitas.dynamic.spawn` message carries the new agent's public key. The receiver verifies the spawn message against the spawning supervisor's already-registered public key, then trusts the enclosed key transitively. This is a certificate chain of depth 1, rooted in the supervisor's known identity. |
| **Multi-node, no mTLS** | Warn at startup: message signing provides no multi-node guarantee without transport-layer authentication. Signing defends against rogue agents *within* the cluster; it does not defend against network adversaries. If you don't have transport mTLS, you have a bigger problem than message signing. |

For multi-node static topologies, public keys go directly in the topology file alongside the agent declaration:

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - agent:
        name: research_agent
        type: myapp.agents.ResearchAgent
        public_key: "base64-encoded-verify-key"   # not sensitive; safe in source
    - agent:
        name: writer_agent
        type: myapp.agents.WriterAgent
        public_key: "base64-encoded-verify-key"
```

For `mode: provisioned`, the private key is loaded from `{key_dir}/{agent_name}/id_ed25519`. The public key is derived from the private key; the `public_key` topology field is only used when connecting to agents hosted on **other nodes** (where you don't have the key file locally).

### D3 — Sign the envelope, not the payload

Signing happens at the serializer layer, **after** `Message.to_dict()` but **before** msgpack encoding. The wire format wraps the message:

```python
{
    "v": 2,
    "msg": <serialized message dict>,
    "sig": {"signer": "agent_name", "alg": "ed25519", "value": <bytes>, "nonce": <bytes>}
}
```

The signed bytes are `msgpack.packb({"v": 2, "msg": msg_dict, "signer": signer_id, "nonce": nonce})` — a deterministic encoding of the full envelope minus the signature value. This protects routing fields (`sender`, `recipient`, `correlation_id`) from tampering, not just the payload.

Verification at deserialize-time recovers the message dict, verifies the signature against the registered public key for `signer`, and rejects on mismatch (raises `SignatureError`, a new `CivitasError` subclass). The schema bumps from v1 to v2; v1 messages remain readable by deserializers when `require_verification: false`.

### D4 — ZMQ CURVE for ZMQ; TLS + nkeys for NATS

| Transport | Mechanism | Why |
|---|---|---|
| **ZMQ** | CURVE (libsodium, built into pyzmq) | Native ZMQ support, no proxy changes, mature and battle-tested |
| **NATS** | TLS + nkeys (Ed25519-based subject auth) | NATS-native, integrates with their permissions model |
| **InProcess** | No-op | Single-process; no wire to protect |

The ZMQ proxy needs CURVE server keys; each Worker gets a CURVE client keypair. NATS clients receive an `nkey` seed and TLS material. Both are read from the `security.transport` block.

The `security` block can also be partially configured: `signing.enabled: true` works without TLS (e.g., on trusted networks where the threat is rogue agents, not network adversaries).

### D5 — Env-var substitution in YAML; secrets never in source

`Runtime.from_config()` walks the parsed YAML and resolves `${VAR_NAME}` patterns against `os.environ`. Unset variables raise a clear error. This is the **only** substitution syntax — no default values, no shell expansion, no nested expressions. The substitution layer is independent of any specific secrets manager: deployment owners use whatever surfaces env vars (Kubernetes secrets, Docker secrets, Vault sidecar, AWS Secrets Manager via their CLI).

```yaml
plugins:
  models:
    - type: anthropic
      config:
        api_key: ${ANTHROPIC_API_KEY}
```

A separate optional `civitas.secrets.SecretsProvider` protocol (with file/env/Vault implementations) lets advanced users plug in dynamic resolution. The default implementation is the env-var substituter described above.

### D6 — Per-agent credential scope via plugin handles

Plugins are instantiated once per process today. To scope credentials per agent without refactoring every plugin, introduce **plugin handles**: an agent calls `self.llm("anthropic")` instead of receiving an injected `ModelProvider`. The handle resolves the right credential set at call time based on `topology.yaml`:

```yaml
agents:
  - name: research_agent
    credentials:
      anthropic: ${RESEARCH_AGENT_ANTHROPIC_KEY}
  - name: writer_agent
    credentials:
      anthropic: ${WRITER_AGENT_ANTHROPIC_KEY}
```

When `agent.credentials.<plugin>` is unset, the handle falls back to the global plugin instance (current behaviour). This is opt-in and additive — existing agents keep working unchanged.

### D7 — Tool execution sandbox: bubblewrap on Linux, refuse-to-start elsewhere

MCP servers spawn as subprocesses today. M4.2 wraps that subprocess with [`bubblewrap`](https://github.com/containers/bubblewrap) on Linux: read-only root filesystem, no network unless declared, scratch tmpfs for `/tmp`. The sandbox profile is per-MCP-server in YAML:

```yaml
plugins:
  mcp:
    - name: shell_tool
      command: /usr/local/bin/shell_mcp
      sandbox:
        enabled: true
        network: deny           # deny | allow | <allowlist>
        filesystem:
          - /workspace:rw
          - /etc/ssl/certs:ro
```

When `sandbox.enabled: true` and `bwrap` is not available (macOS, Windows, or Linux without bubblewrap installed), the runtime **refuses to start** with a clear error message and install instructions. The right escape hatch is `sandbox.enabled: false` on development topologies — not silent degradation of a declared security control. We do **not** ship a Python-level sandbox (no real isolation in-process). Stronger isolation (gVisor, Firecracker) is out of scope.

### D8 — Audit log is separate from OTel

OTel is for debugging and observability — its spans drop, sample, and export to user-controlled backends. An audit log has different requirements: append-only, never sampled, locally durable, structured for compliance review.

M4.2 ships a `civitas.audit` module that emits structured events:

```python
class AuditEvent(TypedDict):
    timestamp: str        # ISO8601 with UTC offset
    event_type: str       # auth.deny | sign.verify_fail | sandbox.violation | secret.access | ...
    actor: str            # agent name
    target: str | None    # message recipient, tool name, secret name
    result: str           # allow | deny | error
    metadata: dict        # event-specific
```

Default sink is `JsonlFileSink` — newline-delimited JSON, batched fsync (every 100ms or 100 events). Log rotation is handled via SIGHUP: on `SIGHUP`, the sink closes and re-opens the file descriptor, making it compatible with `logrotate` without requiring built-in rotation logic. A `sync_writes: true` option exists for compliance-strict deployments. A `NullSink` exists for tests. Users can plug `SyslogSink`, `OtlpSink`, or custom sinks via `civitas.audit.AuditSink` protocol.

Audit events are emitted at well-defined chokepoints:
- `MessageBus.route()` — on signature failure or denied message
- `MCPTool.execute()` — on every tool invocation (allow + result code)
- Sandbox wrapper — on policy violation
- Secret access — when a plugin handle resolves a credential

### D9 — Performance discipline

Security primitives have measurable cost. Three rules keep that cost predictable:

1. **InProcess signing is a no-op.** When both sender and receiver are in the same OS process, the runtime does not create a `SigningSerializer` — the regular serializer is used and no Ed25519 operations occur. Cross-process and cross-host transports always sign and verify.

2. **Audit log batches by default.** `JsonlFileSink` buffers events in memory and fsyncs every 100ms or every 100 events, whichever comes first. A `sync_writes: true` option exists for compliance-strict deployments but is **off by default**. Per-event fsync at high throughput drops the bus to <1k msg/sec — the one configuration that would make security feel painful.

3. **CI benchmark gate.** A `pytest-benchmark` test measures `MessageBus.route()` throughput on InProcess transport with signing+audit enabled vs. baseline. CI fails if the regression exceeds **30%**. This makes performance regressions visible at PR time rather than after release.

Concrete numbers (modern x86, single core):

| Operation | Cost | Notes |
|---|---|---|
| Ed25519 sign | ~30–50µs | Sender pays |
| Ed25519 verify | ~80–120µs | Receiver pays — verify is slower than sign |
| ChaCha20-Poly1305 (TLS/CURVE) | ~5µs / KB | Negligible at message sizes |
| Audit emit (batched) | ~10µs | Default mode |
| Audit emit (sync fsync) | 1–10ms | Only when `sync_writes: true` |
| Plugin handle resolution | ~100ns | Dict lookup |

For LLM-orchestration workloads, total per-message overhead is **~300µs cross-process, 0µs in-process** — invisible against a 500ms–5s LLM round-trip. For high-throughput pipelines (>10k msg/sec/core sustained), signing on cross-process transports becomes the bottleneck; users can disable signing on trusted transports via `signing.enabled: false`.

---

## Phasing

M4.2 is too big for one PR. It splits into five sub-milestones, each independently shippable and testable:

| Sub-milestone | Scope | Depends on |
|---|---|---|
| **M4.2a — Identity & Signing** | Ed25519 keypairs, signed envelopes, `SignatureError`, public key registry, nonce cache, `security:` YAML block, multi-node key distribution | — |
| **M4.2b — Transport mTLS** | ZMQ CURVE, NATS TLS + nkeys, transport config plumbing | M4.2a (shares identity store) |
| **M4.2c — Credential Isolation** | `${VAR}` env substitution, `SecretsProvider` protocol, per-agent credential scope, plugin handles | — (independent) |
| **M4.2d — Tool Sandbox** | Bubblewrap wrapper for MCP subprocesses, sandbox YAML schema, refuse-to-start on unsupported platforms | — (independent) |
| **M4.2e — Audit Log** | `civitas.audit` module, `JsonlFileSink` (batched, SIGHUP rotation), integration at chokepoints | M4.2a (auth events use signer_id) |

Recommended order: **a → c → d → e → b**. Identity and signing unlock the audit log's notion of an authenticated actor; credentials and sandbox harden the local-trust scenarios most users hit first; transport mTLS lands last because it requires the most operational change (cert provisioning).

---

## Test Plan

Each sub-milestone ships with:

- **Unit tests** for primitives (sign/verify roundtrip, env substitution edge cases, sandbox config parsing)
- **Integration tests** at the MessageBus level (signed message accepted, unsigned rejected when required, replay rejected)
- **Negative tests** — every threat model HIGH item gets a regression test that proves the mitigation works (e.g., spoofed sender → rejected; tampered payload → rejected; unauthorized tool path → sandbox blocks)
- **Coverage target** ≥90% on new code; no drop below current 92% suite-wide

A new test directory: `tests/security/` for cross-cutting tests that combine multiple primitives.

---

## Resolved Design Questions

**Q1 — Key directory format**
Resolved: OpenSSH-style filenames (`id_ed25519`, `id_ed25519.pub`) with simple base64 key material. See D2 for full rationale and the multi-node key distribution breakdown by deployment shape.

**Q2 — Replay protection scope**
Resolved: Ship a bounded nonce cache. Each signed envelope carries a 16-byte random nonce. The receiver maintains an LRU set capped at 10,000 entries (~320KB). Duplicate nonces raise `SignatureError`. Rationale: signing may be the only layer (mTLS is not always deployed), the cost is trivial, and replay is a common-class threat.

**Q3 — Audit log rotation**
Resolved: SIGHUP. On `SIGHUP`, `JsonlFileSink` closes and re-opens the file descriptor. This integrates cleanly with `logrotate` and keeps the sink implementation simple. Built-in rotation would duplicate a solved problem.

**Q4 — Sandbox on macOS/Windows**
Resolved: Refuse to start when `sandbox.enabled: true` and `bwrap` is unavailable. The operator declared a security requirement; the runtime honours it by failing loudly rather than silently degrading. Development topologies should use `sandbox.enabled: false`. See updated D7.

**Q5 — Backwards-compatible signing**
Resolved: Strict rejection by default. When `signing.enabled: true` and an unsigned message arrives, the runtime raises `SignatureError`. A `signing.allow_unsigned: true` escape hatch is documented as transitional-only (rolling upgrades). The operator explicitly acknowledges the degraded state; the runtime does not make that choice silently.

---

## Risks

- **Operational complexity.** mTLS + key rotation + sandbox profiles add significant deployment burden. Mitigation: ship sane defaults, a `civitas security init` CLI command to scaffold keys/configs, and recipes in `docs/security/recipes.md`.
- **Performance.** Cross-process round-trip adds ~300µs (Ed25519 sign + verify on both sides; verify is the dominant ~80–120µs cost). For LLM-orchestration workloads this is invisible against the LLM call itself (500ms–5s). For high-throughput pipelines sustaining >10k msg/sec/core, signing becomes the bottleneck. The audit log is the bigger hidden risk: synchronous fsync per event would drop the bus to <1k msg/sec. Mitigations are codified in **D9 — Performance discipline**: InProcess transport short-circuits signing, audit log batches by default (100ms or 100 events), and a CI benchmark gate fails any change that regresses InProcess throughput by >30%.
- **Bubblewrap availability.** Not on macOS, optional on most Linux distros. Mitigation: clear error messages with install instructions per distro, refuse-to-start when `sandbox.enabled: true` and `bwrap` is absent.
- **Scope creep.** "Security" is unbounded; M4.2 explicitly lists what's out of scope above. Resist additions during implementation — capture them as v0.5+ candidates.

---

## Next Steps

1. Land **M4.2a — Identity & Signing**: `civitas/security/` package, `SigningSerializer`, runtime wiring, tests.
2. Update `docs/milestones.md` M4.2 deliverables to the five sub-milestone breakdown.
3. Follow recommended order: a → c → d → e → b.
