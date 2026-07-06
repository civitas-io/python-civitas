# GenServer

GenServer is a stateful service process for non-AI workloads. Where `AgentProcess` is designed around LLM calls and tool invocations, `GenServer` is designed around shared state — rate limiters, caches, coordinators, aggregators, and any other pure service process that needs to live alongside agents in a supervision tree. It exposes OTP-style `handle_call` / `handle_cast` / `handle_info` dispatch instead of a single `handle()` method, making the synchronous vs. fire-and-forget distinction explicit in the type system.

No LLM or tool provider is injected. `self.llm` and `self.tools` are `None`.

---

## Three dispatch methods

| Incoming message | Handler called | Reply required? |
|---|---|---|
| `runtime.ask()` or `self.call()` — caller is waiting | `handle_call(payload, from_)` | Yes — must return a `dict` |
| `runtime.send()` or `self.cast()` with cast semantics | `handle_cast(payload)` | No |
| Timer (`send_after`) or internal bus message | `handle_info(payload)` | No |

The dispatch is determined by the message itself, not by how you call the method. You never override `handle()` — GenServer's internal `handle()` routes to the right method automatically.

---

## Basic example — rate limiter

```python
import time
from civitas import GenServer

class RateLimiter(GenServer):

    async def init(self) -> None:
        self.state["tokens"] = 100
        self.state["window_start"] = time.monotonic()
        # Schedule the first refill tick
        self.send_after(1000, {"type": "refill"})

    async def handle_call(self, payload: dict, from_: str) -> dict:
        if payload.get("type") == "acquire":
            if self.state["tokens"] > 0:
                self.state["tokens"] -= 1
                return {"ok": True, "remaining": self.state["tokens"]}
            return {"ok": False, "remaining": 0}
        return {"error": "unknown call"}

    async def handle_cast(self, payload: dict) -> None:
        if payload.get("type") == "reset":
            self.state["tokens"] = 100

    async def handle_info(self, payload: dict) -> None:
        if payload.get("type") == "refill":
            self.state["tokens"] = min(100, self.state["tokens"] + 10)
            self.send_after(1000, {"type": "refill"})  # reschedule
```

---

## Lifecycle — use `init()`, not `on_start()`

GenServer calls `init()` once when the process starts. Initialize `self.state` here. Do not override `on_start()` — GenServer uses it internally to call `init()`.

```python
class SessionRegistry(GenServer):

    async def init(self) -> None:
        self.state["sessions"] = {}
        self.state["count"] = 0
```

`self.state` is a plain `dict` that persists across every call, cast, and info message for the lifetime of the process. If the supervisor restarts the process, `init()` is called again and state resets to whatever you set there.

---

## Calling a GenServer from an agent

Agents call GenServers using `self.call()` (synchronous, returns the reply dict) and `self.cast()` (fire-and-forget):

```python
from civitas import AgentProcess
from civitas.messages import Message

class WorkerAgent(AgentProcess):

    async def handle(self, message: Message) -> Message | None:
        # Synchronous — blocks until RateLimiter.handle_call returns
        result = await self.call("rate_limiter", {"type": "acquire"}, timeout=1.0)
        if not result["ok"]:
            return self.reply({"error": "rate limited", "remaining": result["remaining"]})

        # Fire-and-forget — no reply expected
        await self.cast("metrics", {"event": "request", "agent": self.name})

        # ... do work ...
        return self.reply({"status": "ok"})
```

`self.call()` maps to `handle_call`. `self.cast()` maps to `handle_cast`. The calling agent does not see the distinction between `GenServer` and `AgentProcess` — it uses the same `call()` / `cast()` API for both.

---

## Calling a GenServer from the runtime

```python
from civitas import Runtime, Supervisor

runtime = Runtime(
    supervisor=Supervisor("root", children=[
        RateLimiter("rate_limiter"),
        WorkerAgent("worker"),
    ])
)
await runtime.start()

# Synchronous call — waits for handle_call to return
result = await runtime.call("rate_limiter", {"type": "acquire"}, timeout=1.0)
print(result)  # {"ok": True, "remaining": 99}

# Cast — returns immediately
await runtime.cast("rate_limiter", {"type": "reset"})
```

---

## Timers — `send_after`

`self.send_after(delay_ms, payload)` schedules a `handle_info` call to self after `delay_ms` milliseconds. It is non-blocking — a background task is created and the current handler returns immediately.

To reschedule a recurring tick, call `send_after` again inside `handle_info`:

```python
async def handle_info(self, payload: dict) -> None:
    if payload.get("type") == "heartbeat":
        await self._flush_buffer()
        self.send_after(5000, {"type": "heartbeat"})  # next tick in 5s
```

All pending `send_after` tasks are cancelled cleanly when the process stops. You do not need to manage them manually.

---

## Topology YAML

GenServer nodes use the same `type` field as `AgentProcess` — the dotted import path of your class:

```yaml
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - name: rate_limiter
      type: myapp.services.RateLimiter
    - name: worker
      type: myapp.agents.WorkerAgent
```

The supervisor treats `GenServer` identically to `AgentProcess` — all supervision strategies, restart windows, backoff policies, and heartbeat monitoring apply unchanged.

---

## Example — shared cache with TTL eviction

```python
import time
from civitas import GenServer

class TTLCache(GenServer):

    async def init(self) -> None:
        self.state["cache"] = {}
        self.send_after(10_000, {"type": "evict"})

    async def handle_call(self, payload: dict, from_: str) -> dict:
        op = payload.get("op")
        key = payload.get("key")

        if op == "get":
            entry = self.state["cache"].get(key)
            if entry is None:
                return {"hit": False}
            value, expires_at = entry
            if time.monotonic() > expires_at:
                del self.state["cache"][key]
                return {"hit": False}
            return {"hit": True, "value": value}

        if op == "set":
            ttl = payload.get("ttl", 60)
            self.state["cache"][key] = (payload["value"], time.monotonic() + ttl)
            return {"ok": True}

        return {"error": "unknown op"}

    async def handle_info(self, payload: dict) -> None:
        if payload.get("type") == "evict":
            now = time.monotonic()
            self.state["cache"] = {
                k: v for k, v in self.state["cache"].items() if v[1] > now
            }
            self.send_after(10_000, {"type": "evict"})
```

---

## API reference

| Method | Signature | Description |
|---|---|---|
| `init` | `async def init(self) -> None` | Called once on start. Initialize `self.state` here. |
| `handle_call` | `async def handle_call(self, payload: dict, from_: str) -> dict` | Synchronous request. Must return a dict. |
| `handle_cast` | `async def handle_cast(self, payload: dict) -> None` | Fire-and-forget message. No reply. |
| `handle_info` | `async def handle_info(self, payload: dict) -> None` | Timer or internal signal. No reply. |
| `send_after` | `def send_after(self, delay_ms: int, payload: dict) -> None` | Schedule a `handle_info` to self after `delay_ms` ms. |
| `self.state` | `dict` | Mutable state dict, persisted between calls for process lifetime. |
| `self.call()` | `await self.call(name, payload, timeout=5.0)` | Synchronous call to another GenServer or agent. |
| `self.cast()` | `await self.cast(name, payload)` | Fire-and-forget send with cast semantics. |

---

## Observability

GenServer emits distinct OTEL spans that differentiate it from agent spans:

| Span name | When |
|---|---|
| `civitas.genserver.call` | Each `handle_call` invocation |
| `civitas.genserver.cast` | Each `handle_cast` invocation |
| `civitas.genserver.info` | Each `handle_info` invocation |

These appear in your trace backend nested under the calling agent's span, just like any other agent interaction.

---

## What GenServer does not do

**No LLM.** `self.llm` is `None`. If you need LLM capability alongside service semantics, use `AgentProcess`.

**No tool provider.** `self.tools` is `None`.

**No streaming.** `handle_call` must return a complete dict. Streaming responses are an `AgentProcess` concern.

**No `handle()` override.** The dispatch is fixed. Override `handle_call`, `handle_cast`, and `handle_info` instead.

**No persistent state across restarts.** `self.state` is in-memory. If the supervisor restarts the process, `init()` runs again. For durable state, plug in a `StateStore` via topology config.

---

## See also

- [supervision.md](supervision.md) — how GenServer fits into the supervision tree
- [messaging.md](messaging.md) — message routing, `ask()` / `send()`, call vs. cast semantics
- [observability.md](observability.md) — OTEL tracing for GenServer spans
