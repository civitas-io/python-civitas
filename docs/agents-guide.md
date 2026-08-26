# Civitas for Coding Agents

A dense, copy-paste-oriented reference for AI coding agents (and humans in a hurry) **building
applications on Civitas**. Everything here is normative — deviating from these rules produces the
bugs listed at the bottom. For prose explanations, each section links the human guide.

> Machine-readable index: [`/llms.txt`](llms.txt). Contributing to civitas itself? That's
> [`AGENTS.md` in the repo](https://github.com/civitas-io/python-civitas/blob/main/AGENTS.md), not this page.

## Install & imports

```bash
pip install civitas                       # core runtime (this package)
pip install civitas[zmq|nats|http|otel|jwt|encryption|security]   # core extras
pip install civitas-contrib[anthropic|openai|gemini|mistral|litellm]  # model providers
pip install civitas-contrib[postgres]     # state stores beyond in-memory/sqlite
```

```python
# Core (always available)
from civitas import AgentProcess, GenServer, Supervisor, DynamicSupervisor, Runtime, Worker
from civitas import HTTPGateway, GatewayConfig
from civitas.messages import Message
from civitas.errors import CivitasError, ErrorAction
from civitas.plugins.tools import ToolRegistry
from civitas.plugins.state import InMemoryStateStore

# Providers/adapters (separate package: civitas-contrib)
from civitas_contrib.plugins.anthropic import AnthropicProvider
from civitas_contrib.adapters.langgraph import LangGraphAgent
```

**There are no `civitas[anthropic]` extras and no `civitas.adapters` module** — providers and
adapters live in `civitas-contrib`.

## The minimal correct agent

```python
class MyAgent(AgentProcess):
    capabilities = ["text.summarize"]                 # optional, class-level

    async def on_start(self) -> None:
        self.state.setdefault("count", 0)             # init state; NO send/ask here

    async def handle(self, message: Message) -> Message | None:
        self.state["count"] += 1
        await self.checkpoint()                       # persist after a unit of work
        return self.reply({"count": self.state["count"]})   # REQUIRED if caller used ask()

    async def on_error(self, error: Exception, message: Message) -> ErrorAction:
        if isinstance(error, TransientError):
            await asyncio.sleep(1.0)                  # backoff lives HERE
            return ErrorAction.RETRY                  # in-place, FIFO-preserving
        return ErrorAction.ESCALATE                   # default: crash → supervisor restarts

    async def on_stop(self) -> None: ...              # cleanup; raising is contained
```

```python
runtime = Runtime(supervisor=Supervisor("root", children=[MyAgent("worker")],
                                        max_restarts=5, backoff="EXPONENTIAL"))
await runtime.start()
result = await runtime.ask("worker", {"task": "x"}, timeout=30.0)
await runtime.stop()
```

## Hard rules (violations = production bugs)

1. **`ask()` caller ⇒ handler must `return self.reply(...)`.** Missing reply = caller hangs to timeout.
2. **Payloads are JSON primitives only** (`str/int/float/bool/list/dict/None`). `.model_dump()` Pydantic models first.
3. **No `send`/`ask` from `on_start()`** — the bus isn't ready. First outbound message goes from `handle()`.
4. **Only checkpointed state survives restart** (v0.9.0, final). `self.state` is reset on every (re)start, then restored from the last `checkpoint()`. Instance variables: undefined across restarts — never rely on them.
5. **Never block the event loop.** All I/O async; wrap unavoidable blocking calls in `asyncio.to_thread()`. One blocking `handle()` stalls every agent in the process.
6. **Route by name, never by object reference.** `await self.ask("other", ...)` — direct method calls bypass supervision, tracing, and transport.
7. **Reserved prefixes:** application `message_type` must not start with `_agency.` or `civitas.stream.` (raises `MessageValidationError`).
8. **`message.sender` on externally-injected messages (`"_runtime"`) is not a routable agent.** Return data via `reply()`; a `send(message.sender)` to `_runtime` is logged and dropped.
9. **No cycles in your `ask()` graph.** A asks B asks A = both stall until timeout-crash. Back-channels use `send()`.
10. **`broadcast("*")` never reaches system names** (`_runtime`, `_agency.*`). Explicit `"_agency.*"` patterns do.

## Decision quick-table

| Question | Answer | Details |
|---|---|---|
| Which restart strategy? | Independent children → `ONE_FOR_ONE` · shared state → `ONE_FOR_ALL` · pipeline → `REST_FOR_ONE` | [recipes](recipes.md#restart-strategy-what-should-die-together) |
| Which transport? | One process → `in_process` · multi-process/one host → `zmq` · multi-machine → `nats` | [recipes](recipes.md#transport-how-far-apart-are-your-agents) |
| Can this agent hang? | Set `handle_timeout=N` (async hangs only; flows through `on_error`) | [recipes](recipes.md#handle_timeout-can-this-agent-hang) |
| Transient vs. poison failure? | `RETRY` (with sleep in `on_error`) vs. `SKIP` · unknown → `ESCALATE` | [recipes](recipes.md#error-handling-what-does-this-failure-mean) |
| Fixed vs. elastic population? | Static YAML children vs. `DynamicSupervisor` + `spawn()` (cap it) | [recipes](recipes.md#static-children-vs-dynamic-spawning-is-the-population-fixed) |
| Human approval gate? | `runtime.suspend(name, reason)` / `resume(name, approver)` — approver is mandatory | [recipes](recipes.md#suspension-when-do-humans-gate-the-loop) |
| Key-value service, not an agent? | Subclass `GenServer` (`handle_call/cast/info`), not `AgentProcess` | [genserver](genserver.md) |
| State must survive deploys? | contrib `sqlite`/`postgres` store; encrypt with `civitas[encryption]` | [recipes](recipes.md#state-persistence-what-must-survive) |

## Copy-paste patterns

**Fan-out with structured concurrency:**
```python
async def handle(self, message: Message) -> Message | None:
    async with asyncio.TaskGroup() as tg:
        a = tg.create_task(self.ask("worker_a", {"chunk": message.payload["a"]}))
        b = tg.create_task(self.ask("worker_b", {"chunk": message.payload["b"]}))
    return self.reply({"merged": [a.result().payload, b.result().payload]})
```

**LLM + tools loop** (provider injected as `self.llm`, tools as `self.tools`):
```python
response = await self.llm.chat(model="claude-sonnet-4-6", messages=msgs,
                               tools=[t.schema for t in self.tools.list_tools()])
while response.tool_calls:
    results = [{"tool_use_id": tc.id,
                "content": str(await self.tools.get(tc.name).execute(**tc.input))}
               for tc in response.tool_calls]
    msgs += [{"role": "assistant", "content": response.content},
             {"role": "user", "content": results}]
    response = await self.llm.chat(model="claude-sonnet-4-6", messages=msgs, tools=...)
```

**Streaming reply** (terminator sent automatically, even on error):
```python
async def handle(self, message: Message) -> None:
    async with self.stream_reply(max_duration=600) as stream:
        async for token in produce():
            await stream.send({"token": token})
```

**Dynamic fan-out (one child per job):**
```python
await self.spawn_nowait(JobAgent, f"job-{job_id}", config={"job": job_id})
async def on_child_terminated(self, name: str, reason: str) -> None:
    if reason == "restarts_exhausted": await self.send("alerts", {"failed": name})
```

**Topology YAML skeleton:**
```yaml
transport: { type: in_process }          # in_process | zmq | nats
plugins:
  models: [{ type: anthropic, config: { default_model: claude-sonnet-4-6 } }]
  state: { type: sqlite, config: { db_path: ./state.db } }
supervision:
  name: root
  strategy: ONE_FOR_ONE
  children:
    - agent: { name: worker, type: myapp.MyAgent, handle_timeout: 120 }
```
Run: `civitas run --topology topology.yaml` · validate: `civitas topology validate topology.yaml`.

## Semantics you must design around

- **At-most-once delivery.** In-flight message lost on crash; queued messages survive restart; no DLQ. Idempotency and re-send logic are yours. [Full contract →](messaging.md#delivery-semantics-hazards)
- **RETRY is immediate and blocking** (ordered). Non-blocking deferral = `SKIP` + re-send to self.
- **Backpressure blocks senders** (bounded mailboxes, default 1000). Mutually-full cycles deadlock.
- **Suspension buffers business messages** (senders eventually block) but acks heartbeats.
- **`ask()` into a suspended agent times out** — poll with `send` for long approvals.

## Debugging signals

| Symptom | Likely cause |
|---|---|
| Caller hangs 30 s then `TimeoutError` | Handler didn't `reply()` (rule 1), ask-cycle (rule 9), or target suspended |
| `MessageRoutingError: No agent registered` | Name typo, agent crashed permanently (budget exhausted), or routing by stale name after despawn |
| Agent restarts in a tight loop, then dies | Poison message: `on_error` should `SKIP` it — restarts can't fix bad input |
| Everything freezes at once | Blocking call in some `handle()` (rule 5) — the whole event loop is stalled |
| `civitas.handle.timeout=true` spans | Hung await in `handle()` — check external calls without client timeouts |
| Works in-process, breaks on ZMQ/NATS | Non-JSON payload (rule 2) that only serialization catches, or name not announced cluster-wide |
