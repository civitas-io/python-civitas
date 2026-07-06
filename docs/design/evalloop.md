# Design: EvalLoop (M2.5 + M2.6)

**Status:** M2.5 Implemented — v0.3 | M2.6 Planned — v0.4
**Author:** Jeryn Mathew Varghese
**Last updated:** 2026-04

---

## Motivation

LLM agents can go off-rails — they hallucinate, ignore constraints, loop, or produce unsafe outputs. Detecting this from inside the agent's own `handle()` is possible but conflates application logic with correctness monitoring. You also cannot catch patterns that only emerge across multiple messages.

EvalLoop introduces a dedicated, supervised `EvalAgent` process that sits alongside regular agents in the supervision tree. Agents emit observable events; the EvalAgent scores them and injects correction signals back. If needed, it halts the offending agent entirely.

There are two tiers:

| Tier | What | When |
|------|------|------|
| **M2.5 — Local EvalLoop** | In-process `EvalAgent` with real-time correction signals | v0.3 |
| **M2.6 — Remote Eval Exporters** | Plugin adapters for Arize Phoenix, Fiddler, Langfuse, Braintrust, LangSmith | v0.4 |

Both tiers share the same `EvalEvent` schema. The local agent consumes it in-process; remote exporters translate it to OTEL GenAI spans and forward to external eval engines.

---

## Architecture

```
AgentProcess
    │
    ├── await self.emit_eval("llm_output", {"content": response, ...})
    │           │
    │           ├──▶ EvalAgent (local, in-process)
    │           │       │
    │           │       ├── on_eval_event(event) → CorrectionSignal | None
    │           │       ├── rate limit check
    │           │       └── send civitas.eval.correction / civitas.eval.halt
    │           │
    │           └──▶ EvalExporter (remote, M2.6)
    │                   ├── Arize Phoenix (OTEL GenAI spans)
    │                   ├── Fiddler (production guardrails)
    │                   ├── Langfuse (open-source)
    │                   ├── Braintrust (eval science)
    │                   └── LangSmith (LangChain ecosystem)
    │
    └── on_correction(message)   ◀── civitas.eval.correction (nudge / redirect)
        [auto-halt]              ◀── civitas.eval.halt
```

**Why a separate process:** the evaluator is independently supervised, independently restartable, stateful (rate limit counters, violation history), and swappable without touching agent code. This follows the OTP design principle: concerns that can fail independently should be separate processes.

---

## Scope boundary

| Concern | M2.5 (Local) | M2.6 (Remote) |
|---------|-------------|---------------|
| `EvalAgent`, `EvalEvent`, `CorrectionSignal` | ✅ | — |
| `emit_eval()` on `AgentProcess` | ✅ | — |
| `on_correction()` hook | ✅ | — |
| Rate limiting per target agent | ✅ | — |
| Topology YAML (`type: eval_agent`) | ✅ | — |
| `EvalExporter` protocol | ✅ (interface only) | — |
| Arize Phoenix plugin | — | ✅ |
| Fiddler plugin (two-way guardrails) | — | ✅ |
| Langfuse plugin | — | ✅ |
| Braintrust plugin | — | ✅ |
| LangSmith plugin | — | ✅ |

---

## Core types

### EvalEvent

Emitted by agents via `await self.emit_eval(event_type, payload)`. Schema is aligned with [OTEL GenAI Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/) so remote exporters can forward as standard spans.

```python
@dataclass
class EvalEvent:
    agent_name: str          # who emitted it
    event_type: str          # e.g. "llm_output", "tool_call", "decision", "custom"
    payload: dict[str, Any]  # event data (model output, tool result, etc.)
    trace_id: str = ""
    message_id: str = ""
    timestamp: float = field(default_factory=time.time)
```

**Event type conventions:**

| event_type | When to use |
|-----------|-------------|
| `llm_output` | After receiving an LLM response |
| `tool_call` | Before or after a tool invocation |
| `decision` | When the agent makes a routing or branching decision |
| `message_sent` | When the agent sends a message to another agent |
| `custom` | Any application-specific checkpoint |

### CorrectionSignal

Returned by `EvalAgent.on_eval_event()`. Three severity levels:

| Severity | Meaning | Agent behaviour |
|----------|---------|-----------------|
| `nudge` | Soft guidance — minor issue detected | Agent receives correction in `on_correction()`, continues running |
| `redirect` | Significant concern — approach needs to change | Agent receives correction in `on_correction()`, should alter course |
| `halt` | Critical violation | Agent's message loop is stopped via `civitas.eval.halt` |

```python
@dataclass
class CorrectionSignal:
    severity: Literal["nudge", "redirect", "halt"]
    reason: str
    payload: dict[str, Any] = field(default_factory=dict)
```

---

## EvalAgent

```python
class EvalAgent(AgentProcess):
    def __init__(
        self,
        name: str,
        max_corrections_per_window: int = 10,
        window_seconds: float = 60.0,
        **kwargs,
    ): ...

    async def on_eval_event(self, event: EvalEvent) -> CorrectionSignal | None:
        """Override to implement eval logic. Return None to take no action."""
        return None
```

`EvalAgent.handle()` receives `civitas.eval.event` messages, calls `on_eval_event()`, checks the rate limiter, then sends the correction. For `halt`, it sends a `civitas.eval.halt` message which breaks the target agent's message loop cleanly (same path as graceful shutdown — `on_stop()` still runs).

**Rate limiting** uses a sliding window keyed by target agent name. Once an agent has received `max_corrections_per_window` corrections in the last `window_seconds`, further corrections are dropped (and logged). This prevents correction storms against a broken agent.

---

## AgentProcess integration

Two additions to `AgentProcess`:

```python
async def emit_eval(
    self,
    event_type: str,
    payload: dict[str, Any],
    eval_agent: str = "eval_agent",
) -> None:
    """Send an EvalEvent to the named EvalAgent. No-op if bus not wired."""

async def on_correction(self, message: Message) -> None:
    """Called when this agent receives a civitas.eval.correction message.
    Override to react to nudge/redirect signals. Default: no-op."""
```

`civitas.eval.halt` is handled in `_message_loop()` — it breaks the loop the same way `_agency.shutdown` does, ensuring `on_stop()` always runs.

---

## Topology YAML

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - type: civitas.evalloop.EvalAgent
      name: eval_agent

    - type: myapp.agents.ResearchAgent
      name: researcher
```

`type: eval_agent` shorthand is also supported in `Runtime.from_config()`.

---

## EvalExporter protocol (M2.6)

Defined in M2.5 as an interface; implemented in M2.6. Remote eval engines receive the same `EvalEvent` objects, translated to their expected format.

```python
class EvalExporter(Protocol):
    async def export(self, event: EvalEvent) -> None:
        """Forward an EvalEvent to a remote eval engine."""
        ...
```

`emit_eval()` will forward to all registered exporters in addition to the local EvalAgent. Each exporter is responsible for translating `EvalEvent` to the target platform's format:

| Platform | Integration model | Notes |
|----------|-----------------|-------|
| **Arize Phoenix** | OTEL GenAI spans via OTLP | Strongest OTEL support; instrument once |
| **Fiddler** | Fiddler SDK + `fiddler-client` | Two-way: sends events, receives guardrail decisions |
| **Langfuse** | Langfuse Python SDK | Open-source; self-hostable |
| **Braintrust** | Braintrust Python SDK | Strong eval science focus |
| **LangSmith** | LangSmith SDK | LangChain ecosystem |

Fiddler is the most interesting integration: Fiddler can return a guardrail decision (block/allow) synchronously. The `FiddlerExporter` would surface this as a `CorrectionSignal`, making Fiddler a remote eval engine that drives local halt behaviour.

---

## OTEL alignment

`EvalEvent` fields map to OTEL GenAI Semantic Conventions:

| EvalEvent field | OTEL GenAI attribute |
|----------------|---------------------|
| `agent_name` | `gen_ai.agent.name` |
| `event_type` | `gen_ai.operation.name` |
| `payload["model"]` | `gen_ai.request.model` |
| `payload["input_tokens"]` | `gen_ai.usage.input_tokens` |
| `payload["output_tokens"]` | `gen_ai.usage.output_tokens` |
| `payload["content"]` | `gen_ai.output.text` |
| `trace_id` | OTEL trace context |

This alignment means a single `emit_eval()` call produces data consumable by any OTEL-native platform without transformation.

---

## Open questions

1. **Two-way Fiddler guardrails** — should `FiddlerExporter.export()` be async and block the agent until Fiddler responds? Fiddler claims sub-100ms latency; this is viable but adds per-eval latency to the hot path. Alternative: fire-and-forget export, Fiddler sends halt back asynchronously.

2. **EvalExporter registration** — should exporters be registered on the `EvalAgent` instance or globally on the `Runtime`? Per-agent is more flexible but adds configuration surface.

3. **Sampling** — high-throughput agents may emit thousands of eval events per second. Should `emit_eval()` support a sampling rate, or should that be the exporter's concern?

4. **Correction acknowledgement** — should agents be required to acknowledge corrections? Currently `on_correction()` is a best-effort hook with no reply.

---

## Acceptance criteria (M2.5)

- [ ] `EvalAgent` can receive eval events from any agent in the supervision tree
- [ ] `on_eval_event()` is the single override point — no other methods required
- [ ] `nudge` and `redirect` signals delivered via `on_correction()` hook
- [ ] `halt` stops the target agent cleanly (`on_stop()` still runs)
- [ ] Rate limiter drops excess corrections silently (logged at WARNING)
- [ ] `emit_eval()` is a no-op when no bus is wired (safe to call in tests)
- [ ] `type: eval_agent` supported in topology YAML
- [ ] ≥ 12 unit tests; ≥ 1 integration test with a real supervision tree
- [ ] `EvalExporter` protocol defined and documented, not yet implemented
