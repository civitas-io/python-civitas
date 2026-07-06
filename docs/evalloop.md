# EvalLoop

EvalLoop is Civitas's corrective observability layer. Rather than passively logging what agents do, it lets you deploy an `EvalAgent` into the supervision tree that actively monitors agent behavior and sends correction signals back in real time. When an agent emits an `EvalEvent`, the eval agent scores it and can respond with a nudge, a redirect, or a halt — all within the same message bus, with no external round-trips required.

This is distinct from tracing (which records what happened) and metrics (which aggregate over time). EvalLoop is for behavioral guardrails: prompt injection detection, off-topic response filtering, safety classification, quality checks, and any other logic where you want an automated reviewer that can act on what it sees.

---

## How it works

```
Agent                    Bus                   EvalAgent
  │                       │                       │
  │  await self.emit_eval(event)                  │
  │──────────────────────>│                       │
  │                       │  civitas.eval.event   │
  │                       │──────────────────────>│
  │                       │                       │  on_eval_event(event)
  │                       │                       │  → CorrectionSignal | None
  │                       │  civitas.eval.correction (or .halt)
  │<──────────────────────────────────────────────│
  │  on_correction(signal)│                       │
```

The eval agent runs as a supervised sibling of the agents it monitors. Agents opt in by calling `await self.emit_eval(event)` — events are sent as standard bus messages and never block the calling agent's message loop.

---

## Defining an EvalAgent

Subclass `EvalAgent` and override `on_eval_event()`:

```python
from civitas import EvalAgent, EvalEvent, CorrectionSignal

class SafetyEval(EvalAgent):

    async def on_eval_event(self, event: EvalEvent) -> CorrectionSignal | None:
        content = event.payload.get("response", "")

        # Prompt injection detection
        injection_markers = ["IGNORE ALL", "disregard your instructions", "jailbreak"]
        if any(m.lower() in content.lower() for m in injection_markers):
            return CorrectionSignal(
                severity="halt",
                reason="Potential prompt injection in response",
            )

        # Quality guard — responses under 10 characters are probably wrong
        if len(content.strip()) < 10:
            return CorrectionSignal(
                severity="nudge",
                reason="Response suspiciously short",
                payload={"hint": "provide a more complete answer"},
            )

        return None  # no action
```

Return `None` to take no action. Return a `CorrectionSignal` to intervene. `EvalAgent` handles delivery, rate limiting, and halt execution — you only need to implement the scoring logic.

---

## Emitting eval events from an agent

Agents emit eval events by calling `await self.emit_eval(event)` inside `handle()`:

```python
from civitas import AgentProcess, EvalEvent
from civitas.messages import Message

class ResearchAgent(AgentProcess):

    async def handle(self, message: Message) -> Message | None:
        response = await self.llm.chat(
            model="claude-haiku-4-5",
            messages=[{"role": "user", "content": message.payload["question"]}],
        )

        # Emit for evaluation before returning
        await self.emit_eval(EvalEvent(
            agent_name=self.name,
            event_type="llm_response",
            payload={
                "question": message.payload["question"],
                "response": response.content,
                "model": "claude-haiku-4-5",
            },
        ))

        return self.reply({"answer": response.content})
```

`emit_eval` is a fire-and-forget send — it returns immediately and does not block waiting for an eval result.

---

## Handling correction signals in an agent

When `EvalAgent` sends a `nudge` or `redirect`, the target agent's `on_correction()` method is called. Override it to act on the signal:

```python
from civitas import AgentProcess, CorrectionSignal
from civitas.messages import Message

class ResearchAgent(AgentProcess):

    async def on_correction(self, signal: CorrectionSignal) -> None:
        if signal.severity == "nudge":
            # Log and continue — soft guidance
            self.log.warning("Eval nudge: %s", signal.reason)
        elif signal.severity == "redirect":
            # Store the hint for use in the next LLM call
            self._eval_hint = signal.payload.get("hint", "")
            self.log.warning("Eval redirect: %s", signal.reason)

    async def handle(self, message: Message) -> Message | None:
        hint = getattr(self, "_eval_hint", None)
        self._eval_hint = None

        system = "You are a helpful research assistant."
        if hint:
            system += f" Note: {hint}"

        response = await self.llm.chat(
            model="claude-haiku-4-5",
            messages=[{"role": "user", "content": message.payload["question"]}],
            system=system,
        )
        return self.reply({"answer": response.content})
```

A `halt` signal stops the agent's message loop cleanly via the supervisor — `on_correction()` is not called for halts.

---

## Wiring into the supervision tree

```python
from civitas import Runtime, Supervisor, AgentProcess
from civitas.messages import Message

runtime = Runtime(
    supervisor=Supervisor("root", children=[
        SafetyEval("eval"),
        ResearchAgent("researcher"),
    ])
)
await runtime.start()
```

The eval agent is a regular supervised child. If it crashes, the supervisor restarts it according to the configured strategy — monitored agents continue operating (without eval coverage) until it comes back.

---

## Rate limiting

`EvalAgent` has built-in correction rate limiting to prevent a misconfigured eval from flooding agents:

```python
class SafetyEval(EvalAgent):
    def __init__(self, name: str, **kwargs):
        super().__init__(
            name,
            max_corrections_per_window=5,  # max 5 corrections per agent per window
            window_seconds=60.0,            # rolling 60-second window
            **kwargs,
        )
```

If the rate limit is exceeded for a given target agent, the correction is dropped and a warning is logged. The defaults are `max_corrections_per_window=10` and `window_seconds=60.0`.

---

## Forwarding to external eval platforms

`EvalExporter` is a protocol for sending `EvalEvent` objects to remote evaluation platforms (Fiddler, Arize Phoenix, Langfuse, Braintrust, etc.). Implementations ship as optional extras:

```bash
pip install 'civitas[fiddler]'
pip install 'civitas[arize]'
```

To attach an exporter, override `on_eval_event()` and call it directly:

```python
from civitas import EvalAgent, EvalEvent, CorrectionSignal
from civitas_fiddler import FiddlerExporter  # civitas[fiddler]

class ObservabilityEval(EvalAgent):

    def __init__(self, name: str, **kwargs):
        super().__init__(name, **kwargs)
        self._exporter = FiddlerExporter(
            project="my-project",
            model="researcher",
            api_key=os.environ["FIDDLER_API_KEY"],
        )

    async def on_eval_event(self, event: EvalEvent) -> CorrectionSignal | None:
        # Forward to Fiddler in the background
        await self._exporter.export(event)
        # Still apply local guardrails
        if "error" in event.payload.get("response", "").lower():
            return CorrectionSignal(severity="nudge", reason="Response contains error text")
        return None
```

Custom exporters implement the `EvalExporter` protocol — two methods, `export(event)` and `shutdown()`.

---

## Type reference

### EvalEvent

| Field | Type | Description |
|---|---|---|
| `agent_name` | `str` | Name of the agent that emitted the event |
| `event_type` | `str` | Caller-defined event category, e.g. `"llm_response"` |
| `payload` | `dict` | Arbitrary data: request, response, model name, etc. |
| `trace_id` | `str` | Trace ID from the originating message (set automatically by `emit_eval`) |
| `message_id` | `str` | Message ID of the originating message |
| `timestamp` | `float` | Unix timestamp at emit time |

### CorrectionSignal

| Field | Type | Description |
|---|---|---|
| `severity` | `"nudge"` \| `"redirect"` \| `"halt"` | How forcefully to intervene |
| `reason` | `str` | Human-readable explanation, shown in logs and traces |
| `payload` | `dict` | Optional data forwarded to the agent's `on_correction()` |

### Severity levels

| Level | Effect |
|---|---|
| `nudge` | Soft guidance. Agent continues. `on_correction()` is called. |
| `redirect` | Significant concern. Agent continues. `on_correction()` is called with a course-change hint. |
| `halt` | Critical violation. Agent's message loop is stopped cleanly by the supervisor. |

---

## What EvalLoop does not do

**Not a firewall.** Eval events are emitted after the agent has already handled the message. EvalLoop is reactive, not a pre-filter. For pre-execution validation, use input contracts on the HTTP gateway or a middleware layer.

**Not tracing.** Tracing records what happened at the infrastructure level. EvalLoop records what your agent produced and lets you reason about quality, safety, and behavior. Use both.

**Not synchronous.** `emit_eval` is fire-and-forget. The calling agent does not wait for the eval result before replying to its caller. Corrections arrive asynchronously on a subsequent message.

---

## See also

- [observability.md](observability.md) — OTEL tracing and span export
- [supervision.md](supervision.md) — how EvalAgent fits into the supervision tree
- [gateway.md](gateway.md) — input validation via `@contract` decorator
