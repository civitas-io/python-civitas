# Design: GenServer (M3.5)

**Status:** Implemented — v0.3
**Author:** Jeryn Mathew Varghese
**Last updated:** 2026-04

---

## Motivation

`AgentProcess` is designed for AI agent workloads — it carries LLM plugin injection, tool providers, OTEL tracing tuned for LLM spans, and a lifecycle oriented around message-driven AI reasoning.

Not every process in a supervision tree is an AI agent. A real system also needs:

- **Rate limiters** — stateful counters shared across agents
- **Aggregators** — collect and batch events before forwarding
- **API gateways** — translate external HTTP/gRPC requests into bus messages
- **Coordinators** — orchestrate workflows without LLM involvement
- **Caches / registries** — shared lookup tables supervised alongside agents

These are pure service processes. They don't need an LLM, they don't need tool providers, and their failure semantics are different — a dead rate limiter should restart fast with zero state loss concerns, not trigger an AI-aware backoff chain.

In OTP, this is exactly what `GenServer` is for. Civitas should have the same separation.

---

## OTP Analogy

| OTP | Civitas |
|-----|---------|
| `GenServer.call/3` | `await server.call(name, payload, timeout)` |
| `GenServer.cast/2` | `await server.cast(name, payload)` |
| `handle_call/3` | `async def handle_call(self, payload, from_) -> dict` |
| `handle_cast/2` | `async def handle_cast(self, payload) -> None` |
| `handle_info/2` | `async def handle_info(self, message) -> None` |
| `send_after/3` | `self.send_after(delay_ms, payload)` |
| `init/1` | `async def init(self) -> None` |

The key distinction from `AgentProcess`:

- **`handle_call`** — synchronous, the caller blocks until a reply is returned. Maps to the existing `ask()` / request-reply path on the bus.
- **`handle_cast`** — asynchronous, fire-and-forget. The caller does not wait. Maps to the existing `send()` path.
- **`handle_info`** — handles any other message on the mailbox: internal ticks, timer-fired events, out-of-band signals.

---

## Proposed API

### Base class

```python
from civitas.genserver import GenServer

class RateLimiter(GenServer):

    async def init(self) -> None:
        """Called once when the process starts. Set up initial state here."""
        self.state["tokens"] = 100
        self.state["window_start"] = time.monotonic()
        # Schedule a refill tick every second
        self.send_after(1000, {"type": "refill"})

    async def handle_call(self, payload: dict, from_: str) -> dict:
        """Synchronous request. Must return a reply dict."""
        if payload.get("type") == "acquire":
            if self.state["tokens"] > 0:
                self.state["tokens"] -= 1
                return {"ok": True, "remaining": self.state["tokens"]}
            return {"ok": False, "remaining": 0}
        return {"error": "unknown call"}

    async def handle_cast(self, payload: dict) -> None:
        """Async fire-and-forget. No reply."""
        if payload.get("type") == "reset":
            self.state["tokens"] = 100

    async def handle_info(self, payload: dict) -> None:
        """Internal messages — timers, ticks, out-of-band signals."""
        if payload.get("type") == "refill":
            self.state["tokens"] = min(100, self.state["tokens"] + 10)
            self.send_after(1000, {"type": "refill"})  # reschedule
```

### Calling a GenServer from an agent

```python
class MyAgent(AgentProcess):
    async def handle(self, message: Message) -> Message | None:
        # Synchronous call — blocks until RateLimiter replies
        result = await self.call("rate_limiter", {"type": "acquire"}, timeout=1.0)
        if not result["ok"]:
            return self.reply({"error": "rate limited"})

        # Fire-and-forget cast — no reply expected
        await self.cast("metrics_collector", {"event": "request", "agent": self.name})

        # ... do work ...
        return self.reply({"status": "ok"})
```

### Calling from the Runtime

```python
runtime = Runtime(
    supervisor=Supervisor("root", children=[
        RateLimiter("rate_limiter"),
        MyAgent("worker"),
    ])
)

# Synchronous call
result = await runtime.call("rate_limiter", {"type": "acquire"}, timeout=1.0)

# Cast
await runtime.cast("rate_limiter", {"type": "reset"})
```

---

## Implementation Plan

### 1. `civitas/genserver.py` — new module

`GenServer` subclasses `AgentProcess` and overrides `handle()` to dispatch based on whether a reply is expected:

```python
class GenServer(AgentProcess):
    """OTP-style generic server. Override handle_call, handle_cast, handle_info."""

    async def handle(self, message: Message) -> Message | None:
        if message.reply_to:
            # Synchronous call — caller is waiting
            result = await self.handle_call(message.payload, message.sender)
            return self.reply(result)
        elif message.payload.get("__cast__"):
            # Explicit cast — no reply
            await self.handle_cast(message.payload)
            return None
        else:
            # Out-of-band / timer message
            await self.handle_info(message.payload)
            return None

    async def handle_call(self, payload: dict, from_: str) -> dict:
        raise NotImplementedError

    async def handle_cast(self, payload: dict) -> None:
        pass

    async def handle_info(self, payload: dict) -> None:
        pass

    def send_after(self, delay_ms: int, payload: dict) -> None:
        """Schedule a handle_info message to self after delay_ms milliseconds."""
        async def _fire():
            await asyncio.sleep(delay_ms / 1000)
            if self._bus:
                msg = Message(type="info", sender=self.name,
                              recipient=self.name, payload=payload)
                await self._bus.route(msg)
        asyncio.create_task(_fire())
```

### 2. `call()` / `cast()` on `AgentProcess` and `Runtime`

`call()` is already covered by `ask()` — it uses the existing request-reply path (reply_to topic). We add named aliases:

```python
# On AgentProcess
async def call(self, name: str, payload: dict, timeout: float = 5.0) -> dict:
    """Alias for ask() with explicit cast=False semantics."""
    ...

async def cast(self, name: str, payload: dict) -> None:
    """Fire-and-forget send with __cast__ marker."""
    await self.send(name, {**payload, "__cast__": True})
```

### 3. Topology YAML support

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - name: rate_limiter
      type: gen_server
      module: myapp.services
      class: RateLimiter
    - name: worker
      type: agent
      module: myapp.agents
      class: MyAgent
```

### 4. No LLM injection

`GenServer.__init__` does not call `PluginLoader` for LLM or tool providers. State is available, `StateStore` is injected if configured. The supervisor treats it identically to `AgentProcess`.

---

## What GenServer does NOT do

- **No LLM plugin** — `self.llm` is not available. If you need LLM + service semantics, use `AgentProcess`.
- **No tool provider** — `self.tools` is not available.
- **No streaming** — `handle_call` must return a complete dict. Streaming responses go through `AgentProcess`.
- **No `handle()` override** — dispatch is fixed. Override `handle_call` / `handle_cast` / `handle_info` instead.

---

## Supervision behaviour

`GenServer` is a full `AgentProcess` from the supervisor's perspective. All existing supervision strategies, backoff policies, restart windows, and heartbeat monitoring apply unchanged. The supervisor does not need to know whether a child is a `GenServer` or `AgentProcess`.

---

## Open questions

| # | Question | Notes |
|---|----------|-------|
| Q1 | Should `call()` / `cast()` replace `ask()` / `send()` or live alongside them? | Lean towards aliases — no breaking change |
| Q2 | Should `send_after` tasks be cancelled on `GenServer` stop? | Yes — track tasks and cancel in `stop()` |
| Q3 | Should `handle_call` be allowed to return `None` to indicate no reply? | No — callers would hang. Enforce return type. |
| Q4 | Should GenServer appear as a separate node type in `civitas topology show`? | Yes — distinct icon/label for clarity |
| Q5 | OTEL tracing — emit `civitas.genserver.call` and `civitas.genserver.cast` spans instead of `civitas.agent.handle`? | Yes — cleaner trace differentiation |

---

## Acceptance criteria

- [ ] `GenServer` subclass dispatches correctly to `handle_call`, `handle_cast`, `handle_info`
- [ ] `call()` is synchronous — caller blocks until reply is returned or timeout fires
- [ ] `cast()` returns immediately — no reply
- [ ] `send_after()` fires `handle_info` after the specified delay
- [ ] `send_after` tasks cancelled cleanly on process stop
- [ ] `GenServer` can be a child of any `Supervisor` with any strategy
- [ ] Topology YAML supports `type: gen_server`
- [ ] `civitas topology show` displays GenServer nodes distinctly
- [ ] No LLM or tool plugin injected into GenServer
- [ ] ≥ 15 unit tests covering all dispatch paths, timeout, and timer behaviour
- [ ] Documented with at least one end-to-end example (rate limiter)
